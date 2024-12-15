from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import redis
import logging
import requests
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StreamProcessor")

# Telegram configuration
TOKEN = "8126351006:AAEo11VubErHf8vEHIg4JLpKzm-QFvLcvwA"
chat_id = "5024844038"

# Schema definition
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("sender_account_id", StringType(), True),
    StructField("receiver_account_id", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("transaction_fee", DecimalType(10, 2), True),
    StructField("subtype", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_amount", DecimalType(10, 2), True),
    StructField("transaction_message", StringType(), True),
])

class FraudDetector:
    def __init__(self, redis_host="redis-master", redis_port=6379, redis_password="xts7qvg3yU"):
        self.redis_client = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )
        self.NOTIFICATION_COOLDOWN = timedelta(hours=24)
        self.TRANSACTION_COUNT_THRESHOLD = 1
        self.TRANSACTION_AMOUNT_THRESHOLD = 10000.0

    def should_send_notification(self, account_id, transaction_date):
        notification_key = f"fraud_notification:{account_id}"
        last_notification = self.redis_client.get(notification_key)
        
        if last_notification:
            last_notification_time = datetime.fromisoformat(last_notification)
            if datetime.now() - last_notification_time < self.NOTIFICATION_COOLDOWN:
                return False
        return True

    def record_notification(self, account_id):
        notification_key = f"fraud_notification:{account_id}"
        self.redis_client.set(notification_key, datetime.now().isoformat())
        self.redis_client.expire(notification_key, 172800)

    def send_telegram_notification(self, message):
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            logger.info(f"Telegram notification sent successfully: {message}")
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")

    def process_transaction(self, account_id, transaction_date, daily_count, daily_amount):
        is_fraud = (
            daily_count > self.TRANSACTION_COUNT_THRESHOLD or
            daily_amount > self.TRANSACTION_AMOUNT_THRESHOLD
        )

        if is_fraud:
            status_key = f"fraud_status:{account_id}:{transaction_date}"
            if not self.redis_client.exists(status_key):
                self.redis_client.hset(status_key, mapping={
                    "daily_count": daily_count,
                    "daily_amount": daily_amount,
                    "detected_at": datetime.now().isoformat()
                })
                
                if self.should_send_notification(account_id, transaction_date):
                    message = (
                        f"🚨 Fraud Alert 🚨\n"
                        f"Account: {account_id}\n"
                        f"Date: {transaction_date}\n"
                        f"Daily Transactions: {daily_count}\n"
                        f"Daily Amount: ${daily_amount:,.2f}"
                    )
                    self.send_telegram_notification(message)
                    self.record_notification(account_id)
                    logger.warning(f"Fraud notification sent for account {account_id}")

        activity_key = f"user_activity:{account_id}:{transaction_date}"
        self.redis_client.hset(activity_key, mapping={
            "daily_transaction_count": daily_count,
            "daily_transaction_amount": daily_amount,
            "is_fraud": str(is_fraud)
        })

def detect_fraud_and_store(batch_df, batch_id):
    logger.info(f"Processing fraud detection batch {batch_id}")
    
    if batch_df.rdd.isEmpty():
        logger.info("Batch is empty. Skipping processing.")
        return
    
    try:
        detector = FraudDetector()
        for row in batch_df.collect():
            detector.process_transaction(
                row["sender_account_id"],
                row["transaction_date"],
                row["daily_transaction_count"],
                float(row["daily_transaction_amount"]) if row["daily_transaction_amount"] is not None else 0.0
            )
    except Exception as e:
        logger.error(f"Error processing fraud detection batch {batch_id}: {e}")

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaStreamProcessor") \
        .getOrCreate()

    # Kafka configuration
    kafka_options = {
        "kafka.bootstrap.servers": "kafka-controller-headless:9092",
        "kafka.security.protocol": "PLAINTEXT",
        "subscribe": "test",
        "startingOffsets": "latest",
        "maxOffsetsPerTrigger": "100",
        "failOnDataLoss": "false"
    }

    # Read from Kafka and parse JSON
    raw_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    messages = raw_stream.selectExpr("CAST(value AS STRING) as json_str")
    parsed_messages = messages.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
    parsed_messages = parsed_messages.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Filter valid messages
    filtered_messages = parsed_messages.filter(
        col("transaction_id").isNotNull() &
        col("sender_account_id").isNotNull() &
        col("amount").isNotNull() &
        col("timestamp").isNotNull()
    )

    # Add transaction date column
    filtered_messages = filtered_messages.withColumn("transaction_date", to_date(col("timestamp")))

    # Path configurations
    hdfs_base = "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data"
    hdfs_path = f"{hdfs_base}/transactions/"
    checkpoint_base = f"{hdfs_base}/checkpoints"

    # Write raw data to HDFS
    hdfs_query = filtered_messages.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", f"{checkpoint_base}/transactions/") \
        .start()

    # Aggregate for fraud detection
    aggregated_data = filtered_messages.groupBy(
        "sender_account_id", "transaction_date"
    ).agg(
        count("*").alias("daily_transaction_count"),
        sum("amount").alias("daily_transaction_amount")
    )

    # Process aggregated data for fraud detection
    fraud_query = aggregated_data.writeStream \
        .outputMode("update") \
        .foreachBatch(detect_fraud_and_store) \
        .option("checkpointLocation", f"{checkpoint_base}/fraud/") \
        .start()

    # Wait for both queries to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()