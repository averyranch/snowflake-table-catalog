import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, when, lit, current_timestamp, rand

# ------------------ Initialize Spark ------------------
spark = SparkSession.builder \
    .appName("ServiceNowAssetStream") \
    .getOrCreate()

# ------------------ Generate Streaming Asset Data ------------------
asset_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \  # 5 asset records per second
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
    .withColumn("purchase_date", expr("date_add(current_date(), - (value % 1000))")) \
    .withColumn("cost", (rand() * 5000 + 5000).cast("int")) \  # Random cost between $5000 - $10000
    .withColumn("last_updated", current_timestamp())

# ------------------ Print Streaming Data as JSON ------------------
query = asset_stream.writeStream \
    .outputMode("append") \
    .format("console") \  # Print JSON format to stdout
    .option("truncate", False) \
    .option("numRows", 10) \  # Print 10 rows at a time
    .trigger(processingTime="30 seconds") \  # Process every 30 seconds
    .start()

# ------------------ Run for X Batches, then Stop ------------------
X = 10  # Number of batches to run
time.sleep(X * 30)  # Wait for X batches (each batch runs for 30 seconds)
query.stop()
query.awaitTermination()
