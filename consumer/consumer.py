"""
Kafka to PostgreSQL Spark Structured Streaming Pipeline.

This script consumes stock market records from a Kafka topic, parses each Kafka
message as JSON, applies basic type conversion, and writes the resulting
micro-batches into a PostgreSQL table using JDBC.

Pipeline flow:
    Kafka topic: stock_analysis
        -> Spark Structured Streaming
        -> JSON parsing
        -> Basic transformation / casting
        -> PostgreSQL table: stocks

Notes:
    - Spark checkpointing is enabled for fault tolerance.
    - JDBC writes are handled with foreachBatch because Spark does not provide
      a native continuous JDBC streaming sink.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json, col
import os


# Directory where Spark stores checkpoint metadata.
# This allows the stream to recover from failures and avoid reprocessing data
# unnecessarily after restart.
checkpoint_dir = "/tmp/checkpoint/kafka_t0_postgres"

if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)


# PostgreSQL JDBC connection configuration.
# These options are passed directly to Spark's DataFrameWriter.
postgres_config = {
    "url": "jdbc:postgresql://postgres:5432/stock_data",
    "user": "admin",
    "password": "admin",
    "dbtable": "stocks",
    "driver": "org.postgresql.Driver"
}


# Schema expected from Kafka message values.
# Each Kafka value is expected to be a JSON object with these fields.
kafka_data_schema = StructType([
    StructField("date", StringType()),
    StructField("high", StringType()),
    StructField("low", StringType()),
    StructField("open", StringType()),
    StructField("close", StringType()),
    StructField("symbol", StringType())
])


# Initialize the Spark application.
spark = (
    SparkSession.builder
    .appName("KafkaSparkStreaming")
    .getOrCreate()
)


# Create a streaming DataFrame that reads records from the Kafka topic.
# The Kafka message key and value are binary by default, so they are converted
# to strings later before parsing.
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "stock_analysis")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# Parse Kafka message values from JSON into structured Spark columns.
parsed_df = (
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .select(from_json(col("value"), kafka_data_schema).alias("data"))
    .select("data.*")
)


# Apply basic transformations before writing to PostgreSQL.
# The date field is cast to TimestampType so it matches timestamp-compatible
# database columns. Other fields are currently kept as strings.
processed_df = parsed_df.select(
    col("date").cast(TimestampType()).alias("date"),
    col("high").alias("high"),
    col("low").alias("low"),
    col("open").alias("open"),
    col("close").alias("close"),
    col("symbol").alias("symbol")
)


def write_to_postgres(batch_df, batch_id):
    """
    Write one Spark Structured Streaming micro-batch to PostgreSQL.

    Spark calls this function automatically for every micro-batch generated
    from the Kafka stream.

    Args:
        batch_df (pyspark.sql.DataFrame):
            The micro-batch DataFrame containing processed stock records.
        batch_id (int):
            Unique identifier for the current micro-batch. This can be used
            for logging, auditing, or implementing idempotent writes.

    Returns:
        None
    """
    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .options(**postgres_config)
        .save()
    )


# Start the streaming query.
# foreachBatch is used because PostgreSQL/JDBC is a batch sink, not a native
# Spark streaming sink.
query = (
    processed_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", checkpoint_dir)
    .outputMode("append")
    .start()
)


# Keep the streaming application running until manually stopped or terminated.
query.awaitTermination()