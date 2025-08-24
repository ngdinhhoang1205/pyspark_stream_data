# Labraries
from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import StringType, StructField, StructType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

# 1. Create Spark session
spark = (
    SparkSession
    .builder
    .appName("OrdersToKafka")
    .config('spark.streaming.stopGracefullyOnShutdown', True)
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
    .config('spark.sql.shuffle.partitions', 4)
    .master('local[*]')
    .getOrCreate()
)

# Read streaming data from customer_event kafka:9092
customer_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'kafka:9092')
    .option('subscribe', 'customer_event')
    .option('startingOffsets', 'earliest')
    .load()
)
# Tranform data customer_event
customer_trans = customer_df.withColumn('value', expr('cast(value as string)'))
# Create a schema
customer_schema = (
    StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
)

# order data to table format
customer_stream_df = customer_trans.withColumn('value_json', from_json(col('value'), customer_schema)).selectExpr('value_json.*')

# Write the output to console sink to check the output --to remove later
(customer_stream_df
 .writeStream
 .format('console')
 .outputMode('append')
 .trigger(continuous='10 seconds')
 .option('checkpointLocation', 'checkpoint_dir_kafka_customer_event')
 .start()
 .awaitTermination()
)