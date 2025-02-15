
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, when, lit, current_timestamp, rand, count

# ------------------ Initialize Spark ------------------
spark = SparkSession.builder \
    .appName("ServiceNowAssetStream") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# ------------------ Generate Streaming Asset Data ------------------
asset_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load() \
    .withColumn("asset_id", expr("concat('AST', cast(value as string))")) \
    .withColumn("asset_name", expr("concat('Asset-', cast(value as string))")) \
    .withColumn("asset_type", when(col("value") % 3 == 0, lit("Server"))
                               .when(col("value") % 3 == 1, lit("Laptop"))
                               .otherwise(lit("Network Device"))) \
    .withColumn("status", when(col("value") % 4 == 0, lit("In Use"))
                          .when(col("value") % 4 == 1, lit("Retired"))
                          .when(col("value") % 4 == 2, lit("In Maintenance"))
                          .otherwise(lit("Broken"))) \
    .withColumn("location", when(col("value") % 3 == 0, lit("Austin, TX"))
                            .when(col("value") % 3 == 1, lit("New York, NY"))
                            .otherwise(lit("San Francisco, CA"))) \
    .withColumn("assigned_to", when(col("value") % 2 == 0, lit("John Doe"))
                               .otherwise(lit("Jane Smith"))) \
    .withColumn("purchase_date", expr("date_add(current_date(), - cast(value % 1000 as int))")) \
    .withColumn("cost", expr("cast(rand() * 5000 + 5000 as int)")) \
    .withColumn("last_updated", current_timestamp())

# ------------------ Count Incoming Stream Records ------------------
incoming_count = asset_stream.groupBy().agg(count("*").alias("streamed_records"))

# ------------------ Write Incoming Count to Memory ------------------
query_stream_count = incoming_count.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("stream_tracking") \
    .trigger(processingTime="3 seconds") \
    .start()

# ------------------ Write Stream to Console ------------------
query_console = asset_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="3 seconds") \
    .start()

# ------------------ Write Stream to In-Memory Table ------------------
query_memory = asset_stream.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("asset_data") \
    .trigger(processingTime="3 seconds") \
    .start()  # ✅ Ensure no trailing `\`

# ------------------ Run for 30 Seconds ------------------
time.sleep(30)  # ✅ Corrected placement

# Fetch counts from memory
total_streamed = spark.sql("SELECT * FROM stream_tracking").collect()[0][0]
total_written = spark.sql("SELECT COUNT(*) FROM asset_data").collect()[0][0]

# Stop the streams
query_console.stop()
query_memory.stop()
query_stream_count.stop()

# ✅ No need for `awaitTermination()`, since we stop manually
print(f"Total records streamed: {total_streamed}")
print(f"Total records written to memory: {total_written}")
