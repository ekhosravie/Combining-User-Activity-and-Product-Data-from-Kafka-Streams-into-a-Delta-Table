from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

# Define Data Structures (replace with your actual schema)
topic1_schema = StructType([
  StructField("user_id", LongType(), True),
  StructField("item_id", LongType(), True),
  StructField("timestamp", TimestampType(), True)
])

topic2_schema = StructType([
  StructField("item_id", LongType(), True),
  StructField("item_details", StructType([
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True)
  ]), True)
])

# Configure Spark Session
spark = SparkSession.builder \
  .appName("KafkaToDeltaLake") \
  .getOrCreate()

# Kafka consumer parameters (replace with your details)
bootstrapServers = "localhost:9092"
subscribeTopics = ["topic1", "topic2"]

# Create a DLQ topic
dlqTopic = "dlq-topic"

# Read Kafka topics as DataFrames with specified schema
def deserialize_topic1(message):
    try:
        return from_json(message, topic1_schema)
    except Exception as e:
        print(f"Failed to deserialize message: {message}")
        print(f"Sending message to DLQ topic: {dlqTopic}")
        spark.sparkContext.broadcast(message).value.write.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("topic", dlqTopic).save()
        return None

df1 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrapServers) \
  .option("subscribe", subscribeTopics[0]) \
  .load() \
  .selectExpr("CAST(value AS STRING) AS json") \
  .select(col("json").cast("string").as("value")) \
  .select(deserialize_topic1(col("value")).alias("data")) \
  .where(col("data").isNotNull())

def deserialize_topic2(message):
    try:
        return from_json(message, topic2_schema)
    except Exception as e:
        print(f"Failed to deserialize message: {message}")
        print(f"Sending message to DLQ topic: {dlqTopic}")
        spark.sparkContext.broadcast(message).value.write.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("topic", dlqTopic).save()
        return None

df2 = spark \
  .readStream \
.format("kafka") \
  .option("kafka.bootstrap.servers", bootstrapServers) \
  .option("subscribe", subscribeTopics[1]) \
  .load() \
  .selectExpr("CAST(value AS STRING) AS json") \
  .select(col("json").cast("string").as("value")) \.select(deserialize_topic2(col("value")).alias("data")) \
  .where(col("data").isNotNull())

# Join DataFrames based on your logic (replace with your join condition)
joinedDF = df1.join(df2, on=["item_id"], how="inner")  # Example join condition

# Write the joined stream to Delta table in micro-batch mode
def write_to_delta(df, batch_id):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/path/to/your/delta/table")

joinedDF.writeStream \
  .foreachBatch(write_to_delta) \
  .outputMode("update") \
  .option("checkpointLocation", "/tmp/checkpoint") \
  .start()

# Wait for streaming application to terminate
spark.streams.awaitTermination()

