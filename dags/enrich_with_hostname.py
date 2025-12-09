from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import glob
import os

def enrich_with_hostname():
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("EnrichHostname") \
        .master("local[2]") \
        .getOrCreate()
    
    processed_dir = "/opt/airflow/data/processed/"
    enriched_dir = "/opt/airflow/data/enriched/"
    os.makedirs(enriched_dir, exist_ok=True)
    
    # Read hostname lookup
    hostname_df = spark.read.csv(
        "/opt/airflow/data/lookup/client_hostname.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Loaded {hostname_df.count()} hostnames")
    
    # Process each parquet file
    parquet_files = glob.glob(f"{processed_dir}logs_*.parquet")
    
    for parquet_file in parquet_files:
        filename = os.path.basename(parquet_file)
        output_file = f"{enriched_dir}{filename}"
        
        # Skip if already enriched
        if os.path.exists(output_file):
            print(f"Already enriched: {filename}")
            continue
        
        print(f"Enriching: {filename}")
        
        # Read parsed logs
        logs_df = spark.read.parquet(parquet_file)
        
        # Join with hostname (left join to keep all records)
        enriched_df = logs_df.join(
            hostname_df.select("client", "hostname"),
            logs_df.ip == hostname_df.client,
            "left"
        ).drop("client")
        
        record_count = enriched_df.count()
        print(f"Enriched {record_count} records")
        
        # Save
        enriched_df.write.mode("overwrite").parquet(output_file)
        print(f"Saved: {output_file}")
    
    spark.stop()

with DAG(
    'enrich_logs_with_hostname',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['enrich', 'join'],
) as dag:
    
    enrich_task = PythonOperator(
        task_id='join_hostname',
        python_callable=enrich_with_hostname
    )
