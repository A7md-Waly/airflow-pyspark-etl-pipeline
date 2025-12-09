from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import glob
import os

def process_new_files():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import regexp_extract, col
    
    landing = "/opt/airflow/data/landing_zone/"
    processed_dir = "/opt/airflow/data/processed/"
    
    # Get all log files
    all_files = glob.glob(f"{landing}access_log_*.log")
    
    # Check which ones are NOT processed yet
    for log_file in all_files:
        filename = os.path.basename(log_file)
        date = filename.replace("access_log_", "").replace(".log", "")
        output = f"{processed_dir}logs_{date}.parquet"
        
        # Skip if already processed
        if os.path.exists(output):
            print(f"Already processed: {filename}")
            continue
        
        print(f"Processing: {filename}")
        
        spark = SparkSession.builder \
            .appName("LogParser") \
            .master("local[2]") \
            .getOrCreate()
        
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
        print(f"Parsed {total} records from {filename}")
        
        parsed_logs.write.mode("overwrite").parquet(output)
        spark.stop()
        print(f"Saved: {output}")

with DAG(
    'auto_process_any_log',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['auto', 'any_file'],
) as dag:
    
    scan_and_process = PythonOperator(
        task_id='scan_and_process_new_files',
        python_callable=process_new_files
    )
