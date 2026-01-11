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
                    silver.employees,
                    silver.timesheets,
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


def transform_bronze_to_silver(sql_dir: Path) -> bool:
    """Execute Silver layer SQL transformations."""
    logger.info("=== Transforming Bronze → Silver ===")
    conn = connect_db()
    if not conn:
        return False
    
    try:
        # Ordered list of transformations
        sql_files = [
            (sql_dir / "10_transform_employees_bronze_to_silver.sql", "Transform Employees"),
            (sql_dir / "20_transform_timesheets_bronze_to_silver.sql", "Transform Timesheets"),
        ]
        

        for sql_file, description in sql_files:
            if not sql_file.exists():
                logger.error(f"SQL file not found: {sql_file}")
                return False
            
            logger.info(f"Executing: {description}")
            execute_sql_file(conn, str(sql_file))
            logger.info(f"{description} complete")
        
        conn.commit()
        logger.info(" All Silver transformations committed successfully")     
        return True
        
    except Exception as e:
        logger.error(f"Silver transformation failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def transform_silver_to_gold(sql_dir: Path) -> bool:
    """Execute Gold layer SQL transformations."""
    logger.info("=== Transforming Silver → Gold ===")
    conn = connect_db()
    if not conn:
        return False
    
    try:
        gold_dir = sql_dir / "gold" / "transformations"
        
     
        sql_files = [ (sql_dir / "30_transform_dim_date.sql", "Transformation Dim Dates Table"),
            (sql_dir / "40_transform_dim_employees.sql", "Transformation Dim Employees Table"),
            (sql_dir / "50_transform_fact_attendance.sql", "Transformation Fact Attendance Table"),
            ]
        
        if not sql_files:
            logger.info("No Gold transformations defined yet")
            return True
        
        for sql_file, description in sql_files:
            if not sql_file.exists():
                logger.error(f"SQL file not found: {sql_file}")
                return False
            
            logger.info(f"Executing: {description}")
            execute_sql_file(conn, str(sql_file))
            logger.info(f"{description} complete")

        conn.commit()
        logger.info(f"All Gold transformations committed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Gold transformation failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def run_full_pipeline(truncate: bool = False) -> bool:

    logger.info("=" * 60)
    logger.info(f"Starting Full Pipeline (truncate={truncate})")
    logger.info("=" * 60)
    
    base_dir = Path(__file__).resolve().parents[2]
    schema_dir = base_dir / "sql" / "create_table_queries"
    data_dir = base_dir / "data"
    transform_dir = base_dir / "sql" /"transform_queries"
    

    if not init_schema(schema_dir):
        logger.error("Pipeline aborted: Schema initialization failed")
        return False
    

    if truncate:
        if not truncate_all_layers():
            logger.error("Pipeline aborted: Truncate failed")
            return False
    

    if not load_bronze_layer(data_dir):
        logger.error("Pipeline failed: Bronze load incomplete")
        return False
    
  
    if not transform_bronze_to_silver(transform_dir):
        logger.error("Pipeline failed: Silver transformation incomplete")
        return False
    
 
    if not transform_silver_to_gold(transform_dir):
        logger.error("Pipeline failed: Gold transformation incomplete")
        return False
    
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully")
    logger.info("=" * 60)
    return True


# def run_incremental_load() -> bool:
#     """Run incremental Bronze load + transformations (no truncate)."""
#     logger.info("=" * 60)
#     logger.info("Starting Incremental Load")
#     logger.info("=" * 60)
    
#     base_dir = Path(__file__).resolve().parents[2]
#     data_dir = base_dir / "data"
#     sql_dir = base_dir / "sql"
    
#     # Load Bronze
#     if not load_bronze_layer(data_dir):
#         logger.error("Incremental load failed")
#         return False
    
#     # Run transformations (SQL watermarks handle incremental logic)
#     if not transform_bronze_to_silver(sql_dir):
#         logger.error("Silver transformation failed")
#         return False
    
#     if not transform_silver_to_gold(sql_dir):
#         logger.error("Gold transformation failed")
#         return False
    
#     logger.info("Incremental load completed")
#     return True


def main():
    # """Interactive CLI for running the pipeline."""
    # print("\n" + "=" * 60)
    # print("ELT Pipeline - Medallion Architecture")
    # print("=" * 60)
    # print("\nSelect run mode:")
    # print("  1. Full Pipeline (Truncate Bronze)")
    # print("  2. Full Pipeline (Append to Bronze)")
    # print("  3. Incremental Load")
    # print("  0. Exit")
    # print("=" * 60)
    
    # choice = input("\nEnter choice (0-3): ").strip()
    
    # if choice == "1":
    #     logger.info("User selected: Full Pipeline (Truncate)")
    #     success = run_full_pipeline(truncate=True)
    # elif choice == "2":
    #     logger.info("User selected: Full Pipeline (Append)")
    #     success = run_full_pipeline(truncate=False)
    # elif choice == "3":
    #     logger.info("User selected: Incremental Load")
    #     success = run_incremental_load()
    # elif choice == "0":
    #     logger.info("Pipeline cancelled by user")
    #     print("Exiting...")
    #     return
    # else:
    #     print("Invalid choice. Exiting.")
    #     logger.warning(f"Invalid user input: {choice}")
    #     return
    
    # # Exit with proper code for CI/CD
    # sys.exit(0 if success else 1)


############### Test Script ####################

    # logger.info("=" * 60)
    # logger.info("Starting Bronze Layer Test Load")
    # logger.info("=" * 60)
    
    # base_dir = Path(__file__).resolve().parents[2]
    # schema_dir = base_dir / "sql" / "create_table_queries"
    # transform_dir=base_dir / "sql" /"transform_queries"
    # data_dir = base_dir / "data"
    
    # logger.info("Step 1: Initializing database schema...")
    # if not init_schema(schema_dir):
    #     logger.error("Schema initialization failed. Aborting.")
    #     sys.exit(1)
    
    # Load Bronze layer
    # logger.info("Step 2: Loading CSV data into Bronze tables...")
    # if not load_bronze_layer(data_dir):
    #     logger.error("Bronze layer load failed.")
    #     sys.exit(1)
    
    # logger.info("Bronze layer load completed successfully!")

    # logger.info("Step 3: Transforming bronze to silver")
    # if not transform_bronze_to_silver(transform_dir):
    #     logger.error("Transformtaion for bronze to silver failed")
    #     sys.exit(1)

    # logger.info("Step 4: Transforming silver to gold")
    # if not transform_silver_to_gold(transform_dir):
    #     logger.error("Transformtaion for gold to silver failed")
    #     sys.exit(1)
    
    run_full_pipeline(truncate=True)

    sys.exit(0)


if __name__ == "__main__":
    main()