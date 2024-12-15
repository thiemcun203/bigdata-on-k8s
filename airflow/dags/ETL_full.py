from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.01)
}

# DAG definition
dag = DAG(
    'banking_insights_etl',
    default_args=default_args,
    description='Load data from Raw PostgreSQL to HDFS then transform and load to PostgreSQL',
    schedule_interval='@daily',
)

# PostgreSQL JDBC JAR path
postgresql_jar_path = "/opt/spark/jars/postgresql-42.2.6.jar"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {postgresql_jar_path} pyspark-shell"

# PostgreSQL configurations
pg_config = {
    "url": "jdbc:postgresql://postgres:5432/banking_data",
    "user": "ps_user",
    "password": "thiemcun@169",
    "driver": "org.postgresql.Driver"
}

# HDFS paths
HDFS_BASE_PATH = "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data"
CUSTOMERS_PATH = f"{HDFS_BASE_PATH}/customers/"
ACCOUNTS_PATH = f"{HDFS_BASE_PATH}/accounts/"
TRANSACTIONS_PATH = f"{HDFS_BASE_PATH}/transactions/"

def create_spark_session(app_name="BankingInsights"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5")
            .getOrCreate())

def income_category(income):
    if income is None:
        return "Unknown"
    elif income < 20000:
        return "Low"
    elif income < 70000:
        return "Medium"
    else:
        return "High"

def save_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", pg_config["url"]) \
        .option("dbtable", table_name) \
        .option("user", pg_config["user"]) \
        .option("password", pg_config["password"]) \
        .option("driver", pg_config["driver"]) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS") \
        .mode("overwrite") \
        .save()

def load_customers(**context):
    spark = create_spark_session("Load Customers")
    try:
        customers_df = spark.read.parquet(CUSTOMERS_PATH)
        customers_df.cache()
        print(f"Successfully loaded customers data. Count: {customers_df.count()}")
    finally:
        spark.stop()

def load_accounts(**context):
    spark = create_spark_session("Load Accounts")
    try:
        accounts_df = spark.read.parquet(ACCOUNTS_PATH)
        accounts_df.cache()
        print(f"Successfully loaded accounts data. Count: {accounts_df.count()}")
    finally:
        spark.stop()

def process_top_customers(**context):
    spark = create_spark_session("Top Customers Analysis")
    try:
        # Load required data
        customers_df = spark.read.parquet(CUSTOMERS_PATH)
        accounts_df = spark.read.parquet(ACCOUNTS_PATH)
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)

        # Process top customers
        broadcast_customers_df = F.broadcast(customers_df)
        cust_accounts_df = (accounts_df
                           .join(broadcast_customers_df, accounts_df.customer_id == broadcast_customers_df.id, "left")
                           .select("account_id", "customer_id", "full_name"))

        top_customers_window = Window.orderBy(F.col("total_amount").desc())
        top_customers_df = (transactions_df
                           .join(cust_accounts_df, transactions_df.sender_account_id == cust_accounts_df.account_id, "left")
                           .groupBy("customer_id", "full_name")
                           .agg(F.sum("amount").alias("total_amount"))
                           .withColumn("rank", F.row_number().over(top_customers_window))
                           .filter(F.col("rank") <= 10)
                           .select("customer_id", "full_name", "total_amount", "rank"))

        save_to_postgres(top_customers_df, "insight_top_customers")
    finally:
        spark.stop()

def process_customer_segments(**context):
    spark = create_spark_session("Customer Segments Analysis")
    try:
        # Define UDF inside the function for proper serialization
        def income_category(income):
            if income is None:
                return "Unknown"
            elif income < 20000:
                return "Low"
            elif income < 70000:
                return "Medium"
            else:
                return "High"
                
        # Register UDF within the Spark session
        spark.udf.register("income_category", income_category, StringType())
        
        # Read data directly from HDFS
        customers_df = spark.read.parquet(CUSTOMERS_PATH)
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        accounts_df = spark.read.parquet(ACCOUNTS_PATH)
        
        # Join the dataframes
        enriched_df = (transactions_df
            .join(accounts_df, transactions_df.sender_account_id == accounts_df.account_id, "left")
            .join(customers_df, accounts_df.customer_id == customers_df.id, "left"))
        
        # Use registered UDF
        customer_segments_df = (enriched_df
            .withColumn("income_category", F.expr("income_category(income)"))
            .groupBy("customer_id", "full_name", "income_category", "customer_segment")
            .agg(
                F.count("transaction_id").alias("total_transactions"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_transaction_amount"),
                F.percentile_approx("amount", 0.5).alias("median_transaction_amount")
            )
            .withColumn("transaction_frequency", 
                F.when(F.col("total_transactions") > 100, "High")
                 .when(F.col("total_transactions") > 50, "Medium")
                 .otherwise("Low")))
        
        # Save results
        save_to_postgres(customer_segments_df, "customer_segments_analysis")
        print("Successfully processed customer segments")
    except Exception as e:
        print(f"Error in customer segments processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_customer_activity(**context):
    spark = create_spark_session("Customer Activity Analysis")
    try:
        customers_df = spark.read.parquet(CUSTOMERS_PATH)
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        accounts_df = spark.read.parquet(ACCOUNTS_PATH)
        
        enriched_df = (transactions_df
            .join(accounts_df, transactions_df.sender_account_id == accounts_df.account_id, "left")
            .join(customers_df, accounts_df.customer_id == customers_df.id, "left"))
        
        customer_activity_window = Window.partitionBy("customer_id").orderBy("timestamp")
        
        activity_df = (enriched_df
            .withColumn("hour_of_day", F.hour("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))
            .withColumn("time_since_last_transaction", 
                F.unix_timestamp("timestamp") - 
                F.lag(F.unix_timestamp("timestamp")).over(customer_activity_window))
            .groupBy("customer_id", "full_name")
            .agg(
                F.avg("hour_of_day").alias("avg_activity_hour"),
                F.avg("time_since_last_transaction").alias("avg_time_between_transactions"),
                F.min("timestamp").alias("first_transaction_time"),
                F.max("timestamp").alias("last_transaction_time"),
                F.count("transaction_id").alias("total_transactions"),
                F.sum("amount").alias("total_amount")
            ))
        
        save_to_postgres(activity_df, "customer_activity_patterns")
        print("Successfully processed customer activity")
    except Exception as e:
        print(f"Error in customer activity processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_account_balance(**context):
    spark = create_spark_session("Account Balance Analysis")
    try:
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        accounts_df = spark.read.parquet(ACCOUNTS_PATH)
        
        enriched_df = transactions_df.join(
            accounts_df,
            transactions_df.sender_account_id == accounts_df.account_id,
            "left"
        )
        
        balance_window = Window.partitionBy("account_id").orderBy("timestamp")
        
        balance_df = (enriched_df
            .withColumn("running_balance", 
                F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount"))
                      .otherwise(-F.col("amount")))
                .over(balance_window))
            .groupBy("account_id")
            .agg(
                F.first("running_balance").alias("current_balance"),
                F.max("running_balance").alias("highest_balance"),
                F.min("running_balance").alias("lowest_balance"),
                F.avg("running_balance").alias("avg_balance"),
                F.max("timestamp").alias("last_updated")
            ))
        
        save_to_postgres(balance_df, "account_balance_trends")
        print("Successfully processed account balance")
    except Exception as e:
        print(f"Error in account balance processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_account_monthly(**context):
    spark = create_spark_session("Account Monthly Analysis")
    try:
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        
        monthly_df = (transactions_df
            .withColumn("month", F.month("timestamp"))
            .withColumn("year", F.year("timestamp"))
            .groupBy("sender_account_id", "year", "month")
            .agg(
                F.count("transaction_id").alias("monthly_transactions"),
                F.sum("amount").alias("monthly_volume"),
                F.countDistinct("transaction_type").alias("transaction_type_diversity"),
                F.first("device_type").alias("primary_device"),
                F.count("device_type").alias("device_usage_count")
            ))
        
        save_to_postgres(monthly_df, "account_monthly_activity")
        print("Successfully processed monthly activity")
    except Exception as e:
        print(f"Error in monthly activity processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_device_usage(**context):
    spark = create_spark_session("Device Usage Analysis")
    try:
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        
        device_df = (transactions_df
            .groupBy("sender_account_id", "device_type")
            .agg(
                F.count("transaction_id").alias("transactions_count"),
                F.sum("amount").alias("total_amount"),
                F.max("timestamp").alias("last_used")
            ))
        
        save_to_postgres(device_df, "device_usage_analysis")
        print("Successfully processed device usage")
    except Exception as e:
        print(f"Error in device usage processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_product_location(**context):
    spark = create_spark_session("Product Location Analysis")
    try:
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        
        product_df = (transactions_df
            .filter(F.col("product_name").isNotNull())
            .groupBy("product_name", "location")
            .agg(
                F.count("transaction_id").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"),
                F.countDistinct("sender_account_id").alias("unique_customers"),
                F.max("timestamp").alias("last_transaction_time")
            ))
        
        save_to_postgres(product_df, "product_location_analysis")
        print("Successfully processed product location")
    except Exception as e:
        print(f"Error in product location processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_transaction_flow(**context):
    spark = create_spark_session("Transaction Flow Analysis")
    try:
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        
        flow_df = (transactions_df
            .withColumn("hour", F.hour("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))
            .groupBy("hour", "day_of_week", "transaction_type")
            .agg(
                F.count("transaction_id").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.max("timestamp").alias("last_analyzed")
            )
            .orderBy("day_of_week", "hour"))
        
        save_to_postgres(flow_df, "transaction_flow_patterns")
        print("Successfully processed transaction flow")
    except Exception as e:
        print(f"Error in transaction flow processing: {str(e)}")
        raise
    finally:
        spark.stop()

def process_time_based(**context):
    spark = create_spark_session("Time Based Analysis")
    try:
        transactions_df = spark.read.parquet(TRANSACTIONS_PATH)
        
        time_df = (transactions_df
            .withColumn("hour", F.hour("timestamp"))
            .groupBy("sender_account_id")
            .agg(
                F.avg(F.when(F.col("hour").between(9, 17), F.col("amount")).otherwise(0))
                .alias("avg_business_hour_amount"),
                F.avg(F.when(F.col("hour").between(18, 23), F.col("amount")).otherwise(0))
                .alias("avg_evening_amount"),
                F.avg(F.when(F.col("hour").between(0, 8), F.col("amount")).otherwise(0))
                .alias("avg_early_morning_amount"),
                F.max("timestamp").alias("last_transaction_time")
            ))
        
        save_to_postgres(time_df, "time_based_analysis")
        print("Successfully processed time based analysis")
    except Exception as e:
        print(f"Error in time based processing: {str(e)}")
        raise
    finally:
        spark.stop()
# Task definitions
load_customers_task = PythonOperator(
    task_id='load_customers_data',
    python_callable=load_customers,
    provide_context=True,
    dag=dag,
)

load_accounts_task = PythonOperator(
    task_id='load_accounts_data',
    python_callable=load_accounts,
    provide_context=True,
    dag=dag,
)

top_customers_task = PythonOperator(
    task_id='process_top_customers',
    python_callable=process_top_customers,
    provide_context=True,
    dag=dag,
)
# Task definitions for the remaining tasks
customer_segments_task = PythonOperator(
    task_id='process_customer_segments',
    python_callable=process_customer_segments,
    provide_context=True,
    dag=dag,
)

customer_activity_task = PythonOperator(
    task_id='process_customer_activity',
    python_callable=process_customer_activity,
    provide_context=True,
    dag=dag,
)

account_balance_task = PythonOperator(
    task_id='process_account_balance',
    python_callable=process_account_balance,
    provide_context=True,
    dag=dag,
)

account_monthly_task = PythonOperator(
    task_id='process_account_monthly',
    python_callable=process_account_monthly,
    provide_context=True,
    dag=dag,
)

device_usage_task = PythonOperator(
    task_id='process_device_usage',
    python_callable=process_device_usage,
    provide_context=True,
    dag=dag,
)

product_location_task = PythonOperator(
    task_id='process_product_location',
    python_callable=process_product_location,
    provide_context=True,
    dag=dag,
)

transaction_flow_task = PythonOperator(
    task_id='process_transaction_flow',
    python_callable=process_transaction_flow,
    provide_context=True,
    dag=dag,
)

time_based_task = PythonOperator(
    task_id='process_time_based',
    python_callable=process_time_based,
    provide_context=True,
    dag=dag,
)




# Set task dependencies
(
    load_customers_task 
    >> load_accounts_task 
    >> top_customers_task 
    >> customer_segments_task 
    >> customer_activity_task 
    >> account_balance_task 
    >> account_monthly_task 
    >> device_usage_task 
    >> product_location_task 
    >> transaction_flow_task 
    >> time_based_task
)