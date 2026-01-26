from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator  


import sys

# Add your project directory to Python path
sys.path.insert(0, '/Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'elt_pipeline',
    default_args=default_args,
    description='ELT Pipeline: Load CSV to Bronze DB and Transform with dbt',
    schedule='@daily',  # Run daily, adjust as needed
    start_date=datetime(2026, 1, 25),
    catchup=False,
    tags=['elt', 'dbt', 'employee'],
) as dag:

    # Task 1: Run the Python script to load CSV data into bronze database
    load_csv_to_bronze = BashOperator(
        task_id='load_csv_to_bronze',
        bash_command='source /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/venv311/bin/activate && cd /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline && python -m src.pipeline.run_pipeline',
    )

    # Task 2: Run dbt to transform data
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='source /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/venv311/bin/activate && cd /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/dbt_employee_pipeline && dbt run',
    )

    # Task 3: Run dbt tests to validate transformations
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='source /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/venv311/bin/activate && cd /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/dbt_employee_pipeline && dbt test',
    )

    # Optional Task 4: Generate dbt documentation
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='source /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/venv311/bin/activate && cd /Users/siddharthapradhan/Programming/dbt_pipeline/dbt_pipeline/dbt_employee_pipeline && dbt docs generate',
    )

    # Define task dependencies: CSV Load -> dbt Run -> dbt Test -> dbt Docs
    load_csv_to_bronze >> dbt_run >> dbt_test >> dbt_docs_generate