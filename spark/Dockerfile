# Use the Bitnami Spark base image
FROM docker.io/bitnami/spark:3.5.3-debian-12-r1

# Switch to root to set permissions and install dependencies
USER root

# Install redis and requests libraries for Python
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y python3-pip curl && \
    pip3 install redis requests && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the SPARK_HOME environment variable
ENV SPARK_HOME=/opt/bitnami/spark

# Ensure proper permissions for all directories upfront
RUN chmod -R 755 $SPARK_HOME

# Download PostgreSQL JAR
RUN curl -o $SPARK_HOME/jars/postgresql-42.2.6.jar https://jdbc.postgresql.org/download/postgresql-42.2.6.jar

# Switch back to the default user
USER 1001

# Set the working directory to SPARK_HOME
WORKDIR $SPARK_HOME

# Copy custom job files last
COPY jobs/ $SPARK_HOME/examples/src/main/python

# Retain the default entrypoint
ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]

# CMD now starts the Spark master and then runs the worker
CMD ["sh", "-c", "/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master & ./sbin/start-worker.sh spark://spark-master-0:7077"]