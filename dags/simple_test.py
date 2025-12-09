from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'test_files',
    start_date=datetime(2019, 1, 22),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    check_files = BashOperator(
        task_id='list_landing_zone',
        bash_command='ls -lh /opt/airflow/data/landing_zone/'
    )
