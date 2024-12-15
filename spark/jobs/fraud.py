from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import redis
import logging
import os
import requests
from datetime import datetime, timedelta

TOKEN = "8126351006:AAEo11VubErHf8vEHIg4JLpKzm-QFvLcvwA"
chat_id = "5024844038"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FraudDetection")

# Define the schema for the Kafka messages
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

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaFraudDetectionWithRedis") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .getOrCreate()

# Kafka configuration
kafka_options = {
    "kafka.bootstrap.servers": "kafka-controller-headless:9092",
    # "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.security.protocol": "PLAINTEXT",
    # "kafka.sasl.jaas.config": """org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="LKcBAMCslM";""",
    "subscribe": "test",
    "startingOffsets": "latest",#"earliest",
    "maxOffsetsPerTrigger": "100",
    "failOnDataLoss": "false"
}

class FraudDetector:
    def __init__(self, redis_host="redis-master", redis_port=6379, redis_password="xts7qvg3yU"):
        self.redis_client = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )
        self.NOTIFICATION_COOLDOWN = timedelta(hours=24)  # Cooldown period for notifications
        self.TRANSACTION_COUNT_THRESHOLD = 3
        self.TRANSACTION_AMOUNT_THRESHOLD = 10000.0

    def should_send_notification(self, account_id, transaction_date):
        """Check if we should send a notification based on cooldown period"""
        notification_key = f"fraud_notification:{account_id}"
        last_notification = self.redis_client.get(notification_key)
        
        if last_notification:
            last_notification_time = datetime.fromisoformat(last_notification)
            if datetime.now() - last_notification_time < self.NOTIFICATION_COOLDOWN:
                return False
        
        return True

    def record_notification(self, account_id):
        """Record the timestamp of the notification"""
        notification_key = f"fraud_notification:{account_id}"
        self.redis_client.set(notification_key, datetime.now().isoformat())
        # Set expiration to cleanup old records (48 hours)
        self.redis_client.expire(notification_key, 172800)  # 48 hours in seconds

    def send_telegram_notification(self, message):
        """Send notification to Telegram"""
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            logger.info(f"Telegram notification sent successfully: {message}")
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")

    def process_transaction(self, account_id, transaction_date, daily_count, daily_amount):
        """Process a transaction and handle fraud detection"""
        is_fraud = (
            daily_count > self.TRANSACTION_COUNT_THRESHOLD or
            daily_amount > self.TRANSACTION_AMOUNT_THRESHOLD
        )

        if is_fraud:
            # Store fraud status
            status_key = f"fraud_status:{account_id}:{transaction_date}"
            if not self.redis_client.exists(status_key):
                self.redis_client.hset(status_key, mapping={
                    "daily_count": daily_count,
                    "daily_amount": daily_amount,
                    "detected_at": datetime.now().isoformat()
                })
                
                # Check if we should send notification
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
                else:
                    logger.info(f"Skipping notification for account {account_id} (cooldown period)")

        # Store activity data
        activity_key = f"user_activity:{account_id}:{transaction_date}"
        self.redis_client.hset(activity_key, mapping={
            "daily_transaction_count": daily_count,
            "daily_transaction_amount": daily_amount,
            "is_fraud": str(is_fraud)
        })

def detect_fraud_and_store(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    
    if batch_df.rdd.isEmpty():
        logger.info("Batch is empty. Skipping processing.")
        return
    
    try:
        detector = FraudDetector()
        
        # Process each row in the batch
        for row in batch_df.collect():
            sender_account_id = row["sender_account_id"]
            transaction_date = row["transaction_date"]
            daily_transaction_count = row["daily_transaction_count"]
            daily_transaction_amount = float(row["daily_transaction_amount"]) if row["daily_transaction_amount"] is not None else 0.0
            
            detector.process_transaction(
                sender_account_id,
                transaction_date,
                daily_transaction_count,
                daily_transaction_amount
            )
            
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")

# Read and process the stream
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

messages = raw_stream.selectExpr("CAST(value AS STRING) as json_str")
parsed_messages = messages.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
parsed_messages = parsed_messages.withColumn("timestamp", col("timestamp").cast(TimestampType()))

filtered_messages = parsed_messages.filter(
    col("transaction_id").isNotNull() &
    col("sender_account_id").isNotNull() &
    col("amount").isNotNull() &
    col("timestamp").isNotNull()
)

filtered_messages = filtered_messages.withColumn("transaction_date", to_date(col("timestamp")))

aggregated_data = filtered_messages.groupBy(
    "sender_account_id", "transaction_date"
).agg(
    count("*").alias("daily_transaction_count"),
    sum("amount").alias("daily_transaction_amount")
)

# Define checkpoint directory
checkpoint_dir = "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/checkpoints/fraud/"

# Start the streaming query
query_redis = aggregated_data.writeStream \
    .outputMode("update") \
    .foreachBatch(detect_fraud_and_store) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Wait for the streaming to finish
query_redis.awaitTermination()