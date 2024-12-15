from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import socket

# Define the schema for the JSON messages
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
    .appName("KafkaToHDFS") \
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

# Read data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Convert binary value to string
messages = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

# Parse the JSON messages directly, now that they are valid
parsed_messages = messages.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Convert timestamp field to TimestampType
parsed_messages = parsed_messages.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Write to console to verify correct parsing
# query = parsed_messages.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()


# Filter data to ensure proper structure
filtered_messages = parsed_messages.filter(
    col("transaction_id").isNotNull() &
    col("timestamp").isNotNull() &
    col("transaction_type").isNotNull()
)

# Define HDFS path for saving data

hdfs_path = "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/transactions/"

# debug_query = filtered_messages.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
# debug_query.awaitTermination()

# Write the stream to HDFS in Parquet format
query = filtered_messages.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", hdfs_path) \
    .option("checkpointLocation", "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/checkpoints/transactions/") \
    .start()
    # 
    
# Wait for the streaming to finish
query.awaitTermination()
