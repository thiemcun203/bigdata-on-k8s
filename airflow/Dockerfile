FROM apache/airflow:2.9.3-python3.10

# Switch to root user to install system packages and Spark
USER root

# Install Spark
RUN mkdir -p /opt/spark && \
    curl -L https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    tar -xz -f /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    ln -s /opt/spark/bin/spark-submit /usr/local/bin/spark-submit && \
    rm -f /tmp/spark.tgz

# Install necessary system packages, including bash
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    bash \
    gcc \
    python3-dev \
    openjdk-17-jdk \
    procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Ensure bash is the default shell
RUN ln -sf /bin/bash /bin/sh

# Switch to airflow user for installing Python dependencies
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to root to download the PostgreSQL JAR
USER root

# Download PostgreSQL JAR
RUN mkdir -p /opt/spark/jars && \
    curl -o /opt/spark/jars/postgresql-42.2.6.jar https://jdbc.postgresql.org/download/postgresql-42.2.6.jar

# Switch back to airflow user to copy DAGs
USER airflow

# Copy DAGs from the repository to AIRFLOW_HOME/dags
COPY dags $AIRFLOW_HOME/dags

# Set ownership of the DAGs directory to the airflow user
# RUN chown -R airflow:airflow $AIRFLOW_HOME/dags

# Set the default command to airflow
CMD ["airflow"]