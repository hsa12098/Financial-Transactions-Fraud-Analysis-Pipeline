# spark/consumer_mysql.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit, current_timestamp
from pyspark.sql.types import *
import os

# Create a flexible schema that can handle both datasets
schema = StructType([
    # Common fields for both datasets
    StructField("Time", DoubleType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Class", IntegerType(), True),  # 1 for fraud
    
    # Fields from kaggle dataset
    StructField("V1", DoubleType(), True),
    StructField("V2", DoubleType(), True),
    StructField("V3", DoubleType(), True),
    StructField("V4", DoubleType(), True),
    StructField("V5", DoubleType(), True),
    StructField("V6", DoubleType(), True),
    StructField("V7", DoubleType(), True),
    StructField("V8", DoubleType(), True),
    StructField("V9", DoubleType(), True),
    StructField("V10", DoubleType(), True),
    StructField("V11", DoubleType(), True),
    StructField("V12", DoubleType(), True),
    StructField("V13", DoubleType(), True),
    StructField("V14", DoubleType(), True),
    StructField("V15", DoubleType(), True),
    StructField("V16", DoubleType(), True),
    StructField("V17", DoubleType(), True),
    StructField("V18", DoubleType(), True),
    StructField("V19", DoubleType(), True),
    StructField("V20", DoubleType(), True),
    StructField("V21", DoubleType(), True),
    StructField("V22", DoubleType(), True),
    StructField("V23", DoubleType(), True),
    StructField("V24", DoubleType(), True),
    StructField("V25", DoubleType(), True),
    StructField("V26", DoubleType(), True),
    StructField("V27", DoubleType(), True),
    StructField("V28", DoubleType(), True),
    
    # Fields from bank_sim dataset
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("bank_amount", DoubleType(), True),  # Renamed from 'amount' to avoid ambiguity
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    
    # Source identifier
    StructField("data_source", StringType(), True)
])

# Add MySQL JDBC driver
spark = SparkSession.builder \
    .appName("KafkaMySQLConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.28") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Standardize the schema for both datasets
standardized = parsed \
    .withColumn("transaction_amount", 
               when(col("Amount").isNotNull(), col("Amount"))
               .when(col("bank_amount").isNotNull(), col("bank_amount"))
               .otherwise(lit(0.0))) \
    .withColumn("is_fraud", 
               when(col("Class").isNotNull(), col("Class"))
               .when(col("isFraud").isNotNull(), col("isFraud"))
               .otherwise(lit(0))) \
    .withColumn("transaction_time", 
               when(col("Time").isNotNull(), col("Time"))
               .when(col("step").isNotNull(), col("step").cast("double"))
               .otherwise(lit(0.0))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .select("transaction_time", "transaction_amount", "is_fraud", "data_source", "processed_timestamp")

# Write to console for debugging
query1 = standardized.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write to MySQL
def write_to_mysql(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Create a summary of transactions
        summary = batch_df.groupBy("is_fraud", "data_source") \
            .agg({"transaction_amount": "sum", "transaction_time": "count"}) \
            .withColumnRenamed("sum(transaction_amount)", "total_amount") \
            .withColumnRenamed("count(transaction_time)", "transaction_count") \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("processed_timestamp", current_timestamp())
        
        # Write detailed transactions
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/finance_db") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "transactions") \
            .option("user", "admin") \
            .option("password", "xyz") \
            .mode("append") \
            .save()
        
        # Write summary
        summary.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/finance_db") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "transaction_summary") \
            .option("user", "admin") \
            .option("password", "xyz") \
            .mode("append") \
            .save()

# Start the stream with foreachBatch to write to MySQL
query2 = standardized.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
