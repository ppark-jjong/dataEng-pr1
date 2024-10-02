from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG('my_first_dag', default_args=default_args, schedule_interval='@daily') as dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')

    # Define task dependencies
    start_task >> end_task
