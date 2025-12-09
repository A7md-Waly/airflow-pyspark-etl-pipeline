# Big Data Pipeline - E-commerce Web Server Access Logs Analysis

A complete data engineering pipeline built with Apache Airflow and PySpark to process and analyze 10M+ web server access logs from an Iranian e-commerce website.

## 📊 Project Overview

This project demonstrates a production-grade ETL pipeline that:
- Processes 3.5GB of raw Apache access logs (10M+ records)
- Splits logs by date for parallel processing
- Parses and structures unstructured log data
- Enriches data with hostname information
- Performs data quality validation and cleaning
- Generates comprehensive analytics reports

**Data Source:** [Web Server Access Logs - Kaggle](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs)

## 🏗️ Pipeline Architecture

```
Raw Logs (3.5GB)
    ↓ [Split by Date]
Landing Zone (5 daily files)
    ↓ [Parse with PySpark]
Parsed Parquet (235MB, -93% compression)
    ↓ [Enrich with Hostname]
Enriched Parquet (256MB)
    ↓ [Data Quality & Cleaning]
Cleaned Parquet (240MB)
    ↓ [Analytics Generation]
JSON Reports + Insights
```

## 🚀 Technologies Used

- **Apache Airflow 2.10.3** - Workflow orchestration
- **Apache Spark 3.5.0** - Distributed data processing
- **PostgreSQL 13** - Airflow metadata database
- **Docker & Docker Compose** - Containerization
- **Python 3.12** - Scripting and data processing
- **Parquet** - Columnar storage format

## 📁 Project Structure

```
project/
├── dags/                          # Airflow DAG definitions
│   ├── auto_parse_any_file.py    # Auto-detect and parse new log files
│   ├── enrich_with_hostname.py   # Join with hostname lookup
│   ├── data_quality.py           # Data cleaning and validation
│   └── analytics.py              # Generate analytics reports
├── scripts/
│   └── split_logs_by_date.py     # Split raw logs by date
├── landing_zone/                  # Raw log files (gitignored)
├── processed/                     # Parsed parquet files (gitignored)
├── enriched/                      # Enriched data (gitignored)
├── cleaned/                       # Cleaned data (gitignored)
├── analytics/                     # JSON analytics reports (gitignored)
├── lookup/
│   └── client_hostname.csv       # IP to hostname mapping (258k records)
├── docker-compose.yaml           # Docker services configuration
├── Dockerfile                    # Custom Airflow image with PySpark
└── README.md
```

## 🛠️ Setup Instructions

### Prerequisites
- Docker & Docker Compose installed
- 8GB+ RAM available
- 10GB+ disk space

### Installation

1. **Clone the repository:**
```bash
git clone <your-repo-url>
cd project
```

2. **Download the dataset:**
   - Get data from [Kaggle](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs)
   - Place `access.log` in a temporary directory

3. **Split logs by date:**
```bash
python3 scripts/split_logs_by_date.py /path/to/access.log landing_zone/
```

4. **Start Airflow:**
```bash
docker compose up -d
```

5. **Access Airflow UI:**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

6. **Enable and run DAGs in order:**
   - `auto_process_any_log` - Parse raw logs
   - `enrich_logs_with_hostname` - Add hostname data
   - `data_quality_cleaning` - Clean and validate
   - `analytics_reports` - Generate insights

## 📈 Pipeline Results

### Data Compression
- **Raw logs:** 3.5GB
- **Parsed Parquet:** 235MB (93% reduction)
- **Final cleaned:** 240MB

### Processing Performance
- **Total records:** 10,364,866
- **Records after cleaning:** 10,241,992 (1% removed)
- **Processing time:** ~5 minutes (all stages)

### Key Insights from Analytics
- **Top traffic source:** Googlebot (66.249.66.194) with 70k+ daily visits
- **Success rate:** 92-94% (HTTP 200)
- **Bot traffic:** ~60% of total traffic
- **Peak URLs:** Static assets and API endpoints

## 🔄 DAG Workflows

### 1. Auto Parse Logs (`auto_process_any_log`)
- **Schedule:** Every 5 minutes
- **Function:** Automatically detects new log files and parses them
- **Output:** Structured Parquet files with extracted fields (IP, timestamp, URL, status, user-agent)

### 2. Enrich with Hostname (`enrich_logs_with_hostname`)
- **Schedule:** Every 10 minutes
- **Function:** Joins parsed logs with hostname lookup table
- **Output:** Enriched data with hostname information

### 3. Data Quality (`data_quality_cleaning`)
- **Schedule:** Manual trigger
- **Function:** 
  - Removes duplicates
  - Validates IP format
  - Filters invalid status codes
  - Handles null values
- **Output:** Cleaned, production-ready data

### 4. Analytics (`analytics_reports`)
- **Schedule:** Manual trigger
- **Function:** Generates comprehensive analytics:
  - Top 10 IPs by traffic
  - Top 10 URLs by hits
  - Top 10 hostnames
  - Status code distribution
- **Output:** JSON reports per day

## 🎓 Learning Outcomes

This project demonstrates:
- ✅ Building scalable ETL pipelines with Airflow
- ✅ Processing large datasets with PySpark
- ✅ Data quality best practices
- ✅ Containerization with Docker
- ✅ Workflow orchestration and scheduling
- ✅ Parquet optimization for analytics
- ✅ Real-world log parsing and analysis

## 📝 Future Enhancements

- [ ] Bot detection and classification
- [ ] Real-time streaming with Kafka
- [ ] Visualization dashboard (Grafana/Superset)
- [ ] URL decoding for Persian text
- [ ] Geolocation enrichment
- [ ] Anomaly detection

## 📄 License

This project is open source and available under the MIT License.

## 🙏 Acknowledgments

- Dataset provided by [Elias Dabbas on Kaggle](https://www.kaggle.com/eliasdabbas)
- Apache Airflow and Spark communities
