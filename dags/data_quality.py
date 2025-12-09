from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import glob
import os

def clean_data():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, regexp_extract, when
    
    spark = SparkSession.builder \
        .appName("DataQuality") \
        .master("local[2]") \
        .getOrCreate()
    
    enriched_dir = "/opt/airflow/data/enriched/"
    cleaned_dir = "/opt/airflow/data/cleaned/"
    
    parquet_files = glob.glob(f"{enriched_dir}logs_*.parquet")
    
    for parquet_file in parquet_files:
        filename = os.path.basename(parquet_file)
        output_file = f"{cleaned_dir}{filename}"
        
        if os.path.exists(output_file):
            print(f"Already cleaned: {filename}")
            continue
        
        print(f"Cleaning: {filename}")
        
        df = spark.read.parquet(parquet_file)
        
        initial_count = df.count()
        print(f"Initial records: {initial_count}")
        
        # 1. Remove nulls in critical fields
        df_clean = df.filter(
            (col('ip').isNotNull()) & 
            (col('status_code').isNotNull()) &
            (col('ip') != '') &
            (col('status_code') != '')
        )
        
        # 2. Validate IP format (basic: has dots)
        df_clean = df_clean.filter(col('ip').rlike(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'))
        
        # 3. Validate status codes (200-599)
        df_clean = df_clean.filter(
            (col('status_code') >= '200') & 
            (col('status_code') < '600')
        )
        
        # 4. Replace null hostname with "Unknown"
        df_clean = df_clean.withColumn(
            'hostname',
            when(col('hostname').isNull(), 'Unknown').otherwise(col('hostname'))
        )
        
        # 5. Remove duplicates
        df_clean = df_clean.dropDuplicates(['ip', 'timestamp', 'url'])
        
        final_count = df_clean.count()
        removed = initial_count - final_count
        print(f"Final records: {final_count}")
        print(f"Removed: {removed} ({removed*100//initial_count}%)")
        
        df_clean.write.mode("overwrite").parquet(output_file)
        print(f"Saved: {output_file}")
    
    spark.stop()

with DAG(
    'data_quality_cleaning',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['quality', 'cleaning'],
) as dag:
    
    clean_task = PythonOperator(
        task_id='clean_and_validate',
        python_callable=clean_data
    )
