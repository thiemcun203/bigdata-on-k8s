import time
import json
import psycopg2
from confluent_kafka import Producer

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'kafka-controller-headless:9092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': 'user1',
    'sasl.password': 'N4yMeyCkX0',
}

producer = Producer(kafka_config)
kafka_topic = 'test'

# PostgreSQL configuration
db_config = {
    "user": "ps_user",
    "password": "thiemcun@169",
    "host": "postgres",
    "port": "5432",
    "database": "banking_data",
}

# Connect to PostgreSQL database
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database.")

    # SQL query to fetch transaction data
    query = """
    SELECT transaction_id, timestamp, transaction_type, sender_account_id, receiver_account_id, 
           amount, transaction_fee, subtype, device_type, location, product_name, product_amount, transaction_message
    FROM public.transactions;
    """
    cursor.execute(query)
    while True:
        # Fetch 5 rows from the database
        rows = cursor.fetchmany(5)
        if not rows:
            print("No more rows to process.")
            break

        # Push rows to Kafka topic
        for row in rows:
            # Ensure all data is serialized properly
            message = {
                "transaction_id": row[0],
                "timestamp": row[1].isoformat() if row[1] else None,
                "transaction_type": row[2],
                "sender_account_id": row[3],
                "receiver_account_id": row[4],
                "amount": float(row[5]) if row[5] else None,  # Convert Decimal to float
                "transaction_fee": float(row[6]) if row[6] else None,  # Convert Decimal to float
                "subtype": row[7],
                "device_type": row[8],
                "location": row[9],
                "product_name": row[10],
                "product_amount": float(row[11]) if row[11] else None,  # Convert Decimal to float
                "transaction_message": row[12],
            }

            # Serialize to JSON string
            json_message = json.dumps(message)
            producer.produce(kafka_topic, value=json_message)
            print(f"Pushed message to Kafka: {json_message}")

        producer.flush()
        time.sleep(1)  # Wait for 1 second before fetching the next batch

except Exception as e:
    print(f"Error: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("PostgreSQL connection closed.")