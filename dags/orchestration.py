from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

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
    schedule='@daily',
    start_date=datetime(2024, 1, 25),
    catchup=False,
    tags=['elt', 'dbt', 'employee'],
) as dag:

    # Task 1: Load CSV to Bronze
    load_csv_to_bronze = BashOperator(
        task_id='load_csv_to_bronze',
        bash_command='cd /opt/airflow/project && python -m src.pipeline.run_pipeline',
    )

    # Task 2: Run dbt
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/project/dbt_employee_pipeline && dbt run',
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/dbt_employee_pipeline && dbt test',
    )

    # Task 4: Generate dbt docs
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/project/dbt_employee_pipeline && dbt docs generate',
    )

    load_csv_to_bronze >> dbt_run >> dbt_test >> dbt_docs_generate