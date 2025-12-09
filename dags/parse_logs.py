from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def parse_log_file(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import regexp_extract, col
    
    ds = context['ds']
    
    spark = SparkSession.builder \
        .appName("LogParser") \
        .master("local[2]") \
        .getOrCreate()
    
    log_file = f"/opt/airflow/data/landing_zone/access_log_{ds}.log"
    
    raw_logs = spark.read.text(log_file)
    
    log_pattern = r'^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "([^"]*)" "([^"]*)"'
    
    parsed_logs = raw_logs.select(
        regexp_extract('value', log_pattern, 1).alias('ip'),
        regexp_extract('value', log_pattern, 2).alias('timestamp'),
        regexp_extract('value', log_pattern, 3).alias('method'),
        regexp_extract('value', log_pattern, 4).alias('url'),
        regexp_extract('value', log_pattern, 6).alias('status_code'),
        regexp_extract('value', log_pattern, 7).alias('size'),
        regexp_extract('value', log_pattern, 9).alias('user_agent')
    ).filter(col('ip') != '')
    
    total = parsed_logs.count()
    print(f"Parsed {total} records for {ds}")
    
    output = f"/opt/airflow/data/processed/logs_{ds}.parquet"
    parsed_logs.write.mode("overwrite").parquet(output)
    
    spark.stop()

with DAG(
    'auto_parse_logs',
    start_date=datetime(2019, 1, 22),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['auto', 'pyspark'],
) as dag:
    
    check_file = BashOperator(
        task_id='check_file_exists',
        bash_command='test -f /opt/airflow/data/landing_zone/access_log_{{ ds }}.log',
        retries=10,
        retry_delay=30
    )
    
    parse = PythonOperator(
        task_id='parse_log',
        python_callable=parse_log_file
    )
    
    check_file >> parse
