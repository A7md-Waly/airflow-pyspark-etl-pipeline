from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import glob
import os
import json

def generate_analytics():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, hour, desc
    
    spark = SparkSession.builder \
        .appName("Analytics") \
        .master("local[2]") \
        .getOrCreate()
    
    cleaned_dir = "/opt/airflow/data/cleaned/"
    analytics_dir = "/opt/airflow/data/analytics/"
    
    parquet_files = glob.glob(f"{cleaned_dir}logs_*.parquet")
    
    for parquet_file in parquet_files:
        filename = os.path.basename(parquet_file)
        date = filename.replace("logs_", "").replace(".parquet", "")
        output_json = f"{analytics_dir}daily_summary_{date}.json"
        
        if os.path.exists(output_json):
            print(f"Already analyzed: {filename}")
            continue
        
        print(f"\n{'='*50}")
        print(f"Analytics for: {date}")
        print('='*50)
        
        df = spark.read.parquet(parquet_file)
        
        total_records = df.count()
        print(f"\nTotal Records: {total_records:,}")
        
        # Top 10 IPs
        print("\n--- Top 10 IPs ---")
        top_ips = df.groupBy("ip") \
            .agg(count("*").alias("visits")) \
            .orderBy(desc("visits")) \
            .limit(10)
        top_ips.show(truncate=False)
        
        # Top 10 URLs
        print("\n--- Top 10 URLs ---")
        top_urls = df.groupBy("url") \
            .agg(count("*").alias("hits")) \
            .orderBy(desc("hits")) \
            .limit(10)
        top_urls.show(truncate=False)
        
        # Top 10 Hostnames
        print("\n--- Top 10 Hostnames ---")
        top_hosts = df.groupBy("hostname") \
            .agg(count("*").alias("visits")) \
            .orderBy(desc("visits")) \
            .limit(10)
        top_hosts.show(truncate=False)
        
        # Status Codes Distribution
        print("\n--- Status Codes Distribution ---")
        status_dist = df.groupBy("status_code") \
            .agg(count("*").alias("count")) \
            .orderBy("status_code")
        status_dist.show(truncate=False)
        
        # Calculate percentages
        status_with_pct = status_dist.withColumn(
            "percentage", 
            (col("count") / total_records * 100)
        )
        
        # Save JSON summary
        summary = {
            "date": date,
            "total_records": total_records,
            "top_ips": [
                {"ip": row.ip, "visits": row.visits} 
                for row in top_ips.collect()
            ],
            "top_urls": [
                {"url": row.url, "hits": row.hits} 
                for row in top_urls.collect()
            ],
            "status_codes": [
                {"code": row.status_code, "count": row['count'], 
                 "percentage": round(row.percentage, 2)} 
                for row in status_with_pct.collect()
            ]
        }
        
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Saved: {output_json}")
    
    spark.stop()

with DAG(
    'analytics_reports',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['analytics', 'reports'],
) as dag:
    
    analyze = PythonOperator(
        task_id='generate_daily_analytics',
        python_callable=generate_analytics
    )
