from pyspark.sql import SparkSession
import time
import random
import threading
import os

# Step 1: Simulate Streaming Data
def generate_streaming_data(output_path, interval=2):
    """
    Simulates real-time streaming data by creating JSON files in the specified directory.
    :param output_path: Directory where JSON files are created.
    :param interval: Time interval (in seconds) between file creations.
    """
    os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists
    while True:
        timestamp = int(time.time())  # Current timestamp
        # Generate random records
        records = [
            {"id": random.randint(1, 1000), 
             "name": random.choice(["Alice", "Bob", "Charlie", "David"]), 
             "age": random.randint(20, 50), 
             "event_time": timestamp}
            for _ in range(5)  # Generate 5 records per file
        ]
        # Save the records to a JSON file
        file_name = f"{output_path}/data_{timestamp}.json"
        with open(file_name, "w") as f:
            import json
            json.dump(records, f)
        print(f"Generated: {file_name}")
        time.sleep(interval)  # Wait for the next interval

# Step 2: Initialize Spark Session
spark = SparkSession.builder \
    .appName("StreamingToIceberg") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hive") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://your-bucket-name/warehouse/") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Step 3: Define Iceberg Catalog, Database, and Table
catalog = "my_catalog"
database = "my_database"
table = "my_table"

# Step 4: Create Iceberg Table (if not exists)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
    id INT,
    name STRING,
    age INT,
    event_time BIGINT
)
USING iceberg
PARTITIONED BY (age)  -- Partition the table by age
""")
print(f"Table {catalog}.{database}.{table} is ready.")

# Step 5: Simulate Streaming Data (in a separate thread)
input_dir = "/path/to/streaming/input"  # Local directory to simulate the stream
data_thread = threading.Thread(target=generate_streaming_data, args=(input_dir,))
data_thread.daemon = True  # Daemon thread to stop when the main program ends
data_thread.start()

# Step 6: Define Streaming Source (Read JSON files from input_dir)
streaming_df = spark.readStream \
    .format("json") \
    .schema("id INT, name STRING, age INT, event_time BIGINT") \
    .load(input_dir)  # Directory where simulated JSON files are stored

# Step 7: Write Stream to Iceberg Table
query = streaming_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \  # Append new records to the Iceberg table
    .option("checkpointLocation", f"/path/to/checkpoints") \  # Checkpoint for fault tolerance
    .toTable(f"{catalog}.{database}.{table}")  # Write to Iceberg table

# Step 8: Keep the Streaming Query Running
query.awaitTermination()  # Keeps the process alive to handle incoming data
