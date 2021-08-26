from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from Transfer_Job import run_job


def run():
    success = run_job()
    return success


default_args = {
    'owner': 'Mingwei',
    'depends_on_past': False,
    'email': ['mingweizhou@hotmail.co.uk'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(2),
    'execution_timeout': 3000
}
dag = DAG(
    'Property_Transactions_DAG',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    tags=['interview'],
    catchup=False,
    max_active_runs=1
)
task = PythonOperator(
    task_id='Property_Transactions_DAG',
    python_callable=run(),
    dag=dag
)
