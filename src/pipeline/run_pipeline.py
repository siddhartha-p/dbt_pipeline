import logging
import sys
from pathlib import Path
from datetime import datetime
from src.pipeline.db import connect_db, execute_sql_file
from src.pipeline.extract import extract_employees, extract_timesheets
from src.pipeline.load import load_to_bronze

# Setup logging with file output
log_dir = Path(__file__).resolve().parents[2] / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"elt_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def init_schema(schema_dir: Path) -> bool:
    logger.info("=== Initializing Database Schema ===")
    conn = connect_db()
    if not conn:
        logger.error("Database connection failed")
        return False
    
    try:
        sql_files = sorted(schema_dir.glob("*.sql"))
        if not sql_files:
            logger.warning(f"No schema files found in {schema_dir}")
            return True  # Not an error if no files
        
        for sql_file in sql_files:
            logger.info(f"Executing: {sql_file.name}")
            execute_sql_file(conn, str(sql_file))

        conn.commit()
        logger.info("Schema initialized")
        return True
        
    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def truncate_all_layers() -> bool:
    
    logger.info("=== Truncating All Layers ===")
    conn = connect_db()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                TRUNCATE TABLE 
                    gold.fact_attendance,
                    gold.dim_employees,
                    gold.dim_date,
                    silver.stg_employees,
                    silver.stg_timesheets,
                    bronze.employees,
                    bronze.timesheets,
                    bronze.processed_files
                CASCADE;
            """)
            conn.commit()
        
        logger.info("All layers truncated successfully")
        return True
        
    except Exception as e:
        logger.error(f"Truncate failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def load_bronze_layer(data_dir: Path) -> bool:

    logger.info("=== Loading Bronze Layer ===")
    all_success = True
    
    #these files are being extracted even if the the source file has already been processed
    emp_files = sorted(data_dir.glob("employee_*.csv"))
    if not emp_files:
        logger.warning("No employee CSV files found")

    ## def find_and_load(file_names: list[str], table_name: str):
        # for emp_file in file_names:
        #     try:
        #         logger.info(f"Processing {emp_file.name}")
        #         headers, rows = extract_employees(str(emp_file))
                
        #         if not load_to_bronze(table_name, headers, rows, emp_file.name):
        #             all_success = False
        #         else:
        #             logger.info(f"✓ Loaded {len(rows)} employee records")
                    
        #     except Exception as e:
        #         logger.error(f"Employee load failed for {emp_file.name}: {e}")
        #         all_success = False   
        #
        #
        # find_and_load()  
        # find_and_load()  
        


    for emp_file in emp_files:
        try:
            logger.info(f"Processing {emp_file.name}")
            headers, rows = extract_employees(str(emp_file))
            
            if not load_to_bronze("bronze.employees", headers, rows, emp_file.name):
                all_success = False
            else:
                logger.info(f"Loaded {len(rows)} employee records")
                
        except Exception as e:
            logger.error(f"Employee load failed for {emp_file.name}: {e}")
            all_success = False
    
    # Process all timesheet files
    ts_files = sorted(data_dir.glob("timesheet_*.csv"))
    if not ts_files:
        logger.warning("No timesheet CSV files found")
    
    for ts_file in ts_files:
        try:
            logger.info(f"Processing {ts_file.name}")
            headers, rows = extract_timesheets(str(ts_file))
            
            if not load_to_bronze("bronze.timesheets", headers, rows, ts_file.name):
                all_success = False
            else:
                logger.info(f"Loaded {len(rows)} timesheet records")
                
        except Exception as e:
            logger.error(f"Timesheet load failed for {ts_file.name}: {e}")
            all_success = False
    
    return all_success


def main():

    base_dir = Path(__file__).resolve().parents[2]
    schema_dir = base_dir / "sql" / "create_table_queries"
    data_dir = base_dir / "data"

    if not init_schema(schema_dir):
        logger.error("Pipeline aborted: Schema initialization failed")
        return False
    

    if not load_bronze_layer(data_dir):
        logger.error("Pipeline failed: Bronze load incomplete")
        return False

    sys.exit(0)


if __name__ == "__main__":
    main()