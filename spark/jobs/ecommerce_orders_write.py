# Labraries
from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import StringType, StructField, StructType, LongType

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
# Read streaming data from order_data kafka:9092
order_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'kafka:9092')
    .option('subscribe', 'order_data')
    .option('startingOffsets', 'earliest')
    .load()
)

# Tranform data order_data
order_trans = order_df.withColumn('value', expr('cast(value as string)'))
# Create a schema
order_schema = (
    StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("price", StringType(), True),
        StructField("total_value", StringType(), True),
        StructField("order_date", StringType(), True)
    ])
)

# order data to table format
order_stream_df = order_trans.withColumn('value_json', from_json(col('value'), order_schema)).selectExpr('value_json.*')

# Write the output to console sink to check the output --to remove later
(order_stream_df
 .writeStream
 .format('console')
 .outputMode('append')
 .trigger(continuous='10 seconds')
 .option('checkpointLocation', 'checkpoint_dir_kafka_order_data')
 .start()
 .awaitTermination()
)