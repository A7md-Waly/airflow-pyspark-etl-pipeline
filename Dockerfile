# Start from the Airflow image you already have
FROM apache/airflow:2.10.3

# Switch to root user to install Java
USER root

# Update system and install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean

# Switch back to airflow user (for security)
USER airflow

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.0
