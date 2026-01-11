import psycopg2
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

def connect_db():
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        logger.info(f"Database connected successfully")
        return connection
        
    except psycopg2.Error as e:
        logger.info(f"Database connection failed: {e}")
        return None


def execute_sql_file(connection, sql_file_path: str):
    try:
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
        
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        
        logger.info(f"Executed SQL file: {sql_file_path}")
        
    except Exception as e:
        logger.error(f"Failed to execute {sql_file_path}: {e}")
        raise


if __name__ == "__main__":
    conn = connect_db()
    if conn:
        conn.close()
        print("✓ Connection closed")