from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}


with DAG(
    dag_id='my_first_dag',
    description='My first DAG',
    default_args=default_args,
    start_date=datetime(2023, 10, 1),
    schedule_interval=timedelta(days=1),

) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World, this is my first DAG!"',
    )
