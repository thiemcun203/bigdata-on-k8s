# Big Data on Kubernetes Platform

This repository contains deployment configurations and instructions for setting up a comprehensive big data platform on Kubernetes, featuring HDFS, PostgreSQL, Kafka, Spark, Airflow, Redis, Superset, and Prometheus.

## Prerequisites

- Minikube v1.34.0 or later
- Docker 27.1.1 or later
- Ubuntu 23.10
- Helm v3.9.0+g7ceeda6
- Kubectl v1.24.0
- Sufficient system resources:
  - 10 CPUs
  - 16GB RAM
  - 30GB disk space

## Initial Setup

### Starting Minikube

Start the Minikube cluster with the required resources:

```bash
minikube start \
  --cpus=10 \
  --memory=16384 \
  --disk-size=30g
```

Access the Minikube dashboard for cluster monitoring:
```bash
minikube dashboard
```

## Component Installation and Configuration

### 1. HDFS Setup

Deploy HDFS using the provided scripts:

```bash
# Deploy HDFS
bash hdfs/deploy.sh

# Stop and remove HDFS deployment
bash hdfs/undeploy.sh
```

Access the HDFS UI:
```bash
kubectl port-forward my-hdfs-namenode-0 50070:9870
```

Set up user permissions:
```bash
# Create shared group and users
groupadd sharedgroup
useradd -m -s /bin/bash airflow
useradd -m -s /bin/bash spark

# Add users to shared group
usermod -a -G sharedgroup airflow
usermod -a -G sharedgroup spark

# Set HDFS permissions
hdfs dfs -chown -R spark:sharedgroup /user
hdfs dfs -chmod -R 775 /user/data/banking_data/
```

To delete HDFS files:
```bash
kubectl exec -it my-hdfs-namenode-0 -- hdfs dfs -rm -r -f /user/data/banking_data
```

### 2. PostgreSQL Setup

Configure PostgreSQL credentials in `postgresql/postgres-configmap.yaml`:
```yaml
POSTGRES_DB: ps_db
POSTGRES_USER: ps_user
POSTGRES_PASSWORD: thiemcun@169
```

Deploy PostgreSQL:
```bash
bash postgresql/deploy.sh

# To stop PostgreSQL
bash postgresql/undeploy.sh
```

### 3. Kafka Setup

Install Kafka using Helm:
```bash
helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka \
    --values kafka/kafka_values.yaml -n default
```

Retrieve Kafka client password:
```bash
password=$(kubectl get secret kafkadev-user-passwords --namespace default -o jsonpath='{.data.client-passwords}' | base64 --decode)
password=$(echo "$password" | awk -F ',' '{print $1}')
echo "$password"
```

Deploy and run Kafka producer:
```bash
kubectl apply -f kafka/python-kafka-client.yaml

kubectl cp kafka/producer/producer.py default/python-kafka-client:/scripts/producer.py

kubectl exec -it python-kafka-client -- python /scripts/producer.py
```

To uninstall Kafka:
```bash
helm uninstall kafka
```

### 4. Spark Setup

#### Option A: Custom Docker Image (Optional)
If you need to modify the Spark environment:

```bash
# Docker login
echo <token> | docker login https://index.docker.io/v1/ -u <username> --password-stdin

# Create Kubernetes secret for Docker registry
kubectl create secret docker-registry myregistrykey \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=<username> \
  --docker-password=<password> \
  --docker-email=<email> \
  --namespace default

# Build and push custom image
docker build -t <username>/spark:latest .
docker push <username>/spark:latest
```

#### Option B: Standard Installation
Install Spark using Helm:
```bash
helm install spark oci://registry-1.docker.io/bitnamicharts/spark \
    --values spark/values.yaml
```

Access Spark UI:
```bash
kubectl port-forward spark-master-0 8080:8081
```
Visit `http://localhost:8081`

Run Spark streaming jobs:
```bash
# Consumer job
kubectl exec -it spark-master-0 -- /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master-0:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    /opt/bitnami/spark/examples/src/main/python/consumer.py

# Fraud detection job
kubectl exec -it spark-master-0 -- /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master-0:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    /opt/bitnami/spark/examples/src/main/python/fraud.py
```

### 5. Airflow Setup

Build and push custom Airflow image (if needed):
```bash
docker build -t <username>/airflow-with-java:latest .
docker push <username>/airflow-with-java:latest
```

Deploy Airflow:
```bash
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace default -f airflow/airflow-values.yaml
```

Access Airflow UI:
```bash
kubectl port-forward airflow-webserver-<id> 8080:8080
```

Configure Spark connection in Airflow:
1. Navigate to Admin > Connections
2. Add new connection:
   - Connection Id: spark_default
   - Host: spark://spark-master-0
   - Port: 7077
   - Extra: {"kubernetes_namespace": "default"}

### 6. Redis Setup

Install Redis:
```bash
helm install redis oci://registry-1.docker.io/bitnamicharts/redis
```

Delete all Redis keys (replace password with the one provided in helm output):
```bash
kubectl exec -it redis-master-0 -- sh -c "redis-cli -a <password> --scan | xargs -L 100 redis-cli -a <password> DEL"
```

### 7. Superset Setup

Install Superset:
```bash
helm repo add superset https://apache.github.io/superset
helm upgrade --install --values superset superset superset/superset
```

Access Superset UI:
```bash
kubectl port-forward superset-<id> 8088:8088
```

### 8. Prometheus & Grafana Setup

Install Prometheus stack:
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack
```

Get Grafana admin password:
```bash
kubectl get secret --namespace default prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 --decode; echo
# Default password: prom-operator
```

Access Grafana:
```bash
kubectl port-forward prometheus-grafana-<id> 3000:3000
```

## Additional Notes

- Always verify the versions of components match your requirements
- Ensure sufficient resources are available before deployment
- Back up any important data before performing uninstall operations
- Monitor resource usage through Grafana dashboards
- Check component-specific documentation for advanced configurations