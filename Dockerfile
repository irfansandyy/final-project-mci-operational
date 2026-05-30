FROM apache/airflow:2.9.2

# Switch to root to install system dependencies (Java for PySpark)
USER root
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install Python dependencies
USER airflow
COPY dags/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
