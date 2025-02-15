import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, when, lit, current_timestamp, rand

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StreamAndMergeWithTableCreation") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://your-bucket/iceberg/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# Define Paths
s3_parquet_path = "s3a://your-bucket/iceberg/asset_data/"
checkpoint_path = "s3a://your-bucket/iceberg/checkpoints/asset_data/"
table_name = "iceberg_catalog.default.asset_data"

# Check if Table Exists and Create if Missing
tables = spark.sql(f"SHOW TABLES IN iceberg_catalog.default").collect()
table_exists = any(row.tableName == "asset_data" for row in tables)

if not table_exists:
    spark.sql(f"""
        CREATE TABLE {table_name} (
            asset_id STRING,
            asset_name STRING,
            asset_type STRING,
            status STRING,
            location STRING,
            assigned_to STRING,
            purchase_date DATE,
            cost INT,
            last_updated TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (purchase_date)
        LOCATION '{s3_parquet_path}'
    """)

# Generate Streaming Data
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

# Write Streaming Data to S3 in Parquet
query_parquet = asset_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", s3_parquet_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="3 seconds") \
    .start()

# Function to Merge Data into Iceberg
def merge_into_iceberg():
    merge_query = f"""
        MERGE INTO {table_name} target
        USING (
            SELECT * FROM parquet.`{s3_parquet_path}`
        ) source
        ON target.asset_id = source.asset_id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.status = source.status,
                target.last_updated = source.last_updated
        WHEN NOT MATCHED THEN 
            INSERT *
    """
    spark.sql(merge_query)

# Run Merge Every 5 Minutes While Streaming
try:
    while True:
        time.sleep(300)  # Wait for 5 minutes
        merge_into_iceberg()
except KeyboardInterrupt:
    print("Stopping streaming and merge process...")

# Stop Streaming
query_parquet.stop()
spark.stop()
