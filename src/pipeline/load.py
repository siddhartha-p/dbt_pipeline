import logging
from datetime import datetime
import psycopg2
import psycopg2.extras
from src.pipeline.db import connect_db

logger = logging.getLogger(__name__)

def load_to_bronze(table_name: str, headers: list, rows: list, source_file: str) -> bool:
    conn = None
    try:
        conn = connect_db()
        if not conn:
            logger.error("Database connection failed")
            return False

        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM bronze.processed_files WHERE file_name = %s",
                (source_file,)
            )
            if cur.fetchone():
                logger.info(f"Skipping {source_file} - already processed")
                return True  # Return success but skip loading       


        load_timestamp = datetime.now()
        
        extended_headers = headers + ['loaded_at', 'source_file']
        
        extended_rows = []
        for row in rows:
            extended_row = list(row) + [load_timestamp, source_file]
            extended_rows.append(extended_row)

        columns_str = ', '.join(extended_headers)
        placeholders = ', '.join(['%s'] * len(extended_headers))
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, extended_rows) #not multi row insert but multiple single row inserts sent together... look into .COPY as well in the future as it is faster
            
            cur.execute("""
                INSERT INTO bronze.processed_files (file_name, table_name, record_count)
                VALUES (%s, %s, %s)
            """, (source_file, table_name, len(rows)))

            conn.commit()                                            # look into .COPY as well in the future as it is faster
            
        logger.info(f"Successfully loaded {len(extended_rows)} rows into {table_name} from {source_file}")
        return True

    except Exception as e:
        logger.error(f"Failed to load to {table_name}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
