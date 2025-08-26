# Labraries
from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import StringType, StructField, StructType, LongType, TimestampType
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
        StructField("order_date", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
)

# order data to table format
order_stream_df = (
    order_trans
    .withColumn('value_json', from_json(col('value'), order_schema))
    .selectExpr('value_json.*'
                , "timestamp as kafka_timestamp"
               )
    .withWatermark("kafka_timestamp", "1 minute")  # watermark on Kafka ingestion time
)

def order_data_output(df, batch_id):
    print(f"Batch id: {batch_id}")
    # Write each batch to parquet in /data (mounted to host ./data)
    (df
        .write
        .format('parquet')
        .mode('append')
        .save('./data/output/orders/')
    )
    # Show a preview in notebook logs
    df.show(truncate=False)
         
# Streaming query
(order_stream_df
 .writeStream
 .foreachBatch(order_data_output)
 .trigger(processingTime="10 seconds")
 .option("checkpointLocation", "./data/checkpoints/orders")
 .start()
 .awaitTermination()
)