import logging
import random
import time
import threading
import os
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IcebergStreamingPipeline")


class StreamingDataSimulator:
    """
    Simulates real-time data streams by generating JSON files.
    """
    def __init__(self, output_path, interval=2):
        self.output_path = output_path
        self.interval = interval
        os.makedirs(self.output_path, exist_ok=True)  # Ensure the directory exists

    def generate_streaming_data(self):
        """
        Generates JSON files with random records at regular intervals.
        """
        while True:
            timestamp = int(time.time())
            records = [
                {
                    "id": random.randint(1, 1000),
                    "name": random.choice(["Alice", "Bob", "Charlie", "David"]),
                    "age": random.randint(20, 50),
                    "event_time": timestamp
                }
                for _ in range(5)
            ]
            file_name = f"{self.output_path}/data_{timestamp}.json"
            with open(file_name, "w") as f:
                import json
                json.dump(records, f)
            logger.info(f"Generated file: {file_name}")
            time.sleep(self.interval)


class IcebergStreamingPipeline:
    """
    Manages the PySpark pipeline for streaming data into an Iceberg table.
    """
    def __init__(self, config):
        self.config = config
        self.spark = self._initialize_spark()
        self.catalog = config["catalog"]
        self.database = config["database"]
        self.table = config["table"]
        self.input_dir = config["input_dir"]

    def _initialize_spark(self):
        """
        Initializes the SparkSession with Iceberg and S3 configurations.
        """
        logger.info("Initializing SparkSession...")
        return SparkSession.builder \
            .appName("StreamingToIceberg") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hive") \
            .config("spark.sql.catalog.my_catalog.warehouse", self.config["warehouse"]) \
            .config("spark.hadoop.fs.s3a.access.key", self.config["aws_access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config["aws_secret_key"]) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .getOrCreate()

    def create_table(self):
        """
        Creates the Iceberg table if it doesn't already exist.
        """
        logger.info(f"Creating table {self.catalog}.{self.database}.{self.table} (if not exists)...")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.{self.table} (
            id INT,
            name STRING,
            age INT,
            event_time BIGINT
        )
        USING iceberg
        PARTITIONED BY (age)
        """)
        logger.info(f"Table {self.catalog}.{self.database}.{self.table} is ready.")

    def start_streaming_query(self):
        """
        Reads streaming data from JSON files and writes it to the Iceberg table.
        """
        logger.info("Setting up streaming query...")

        # Define the streaming source
        streaming_df = self.spark.readStream \
            .format("json") \
            .schema("id INT, name STRING, age INT, event_time BIGINT") \
            .load(self.input_dir)

        # Define the streaming sink
        query = streaming_df.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("checkpointLocation", self.config["checkpoint_dir"]) \
            .toTable(f"{self.catalog}.{self.database}.{self.table}")

        # Keep the query running
        logger.info("Starting streaming query...")
        query.awaitTermination()


def main():
    # Configuration
    config = {
        "app_name": "IcebergStreamingPipeline",
        "catalog": "my_catalog",
        "database": "my_database",
        "table": "my_table",
        "warehouse": "s3a://your-bucket-name/warehouse/",
        "aws_access_key": "YOUR_AWS_ACCESS_KEY",
        "aws_secret_key": "YOUR_AWS_SECRET_KEY",
        "input_dir": "/path/to/streaming/input",
        "checkpoint_dir": "/path/to/checkpoints"
    }

    # Step 1: Start the data simulator
    simulator = StreamingDataSimulator(output_path=config["input_dir"], interval=2)
    simulator_thread = threading.Thread(target=simulator.generate_streaming_data)
    simulator_thread.daemon = True
    simulator_thread.start()

    # Step 2: Initialize the Iceberg streaming pipeline
    pipeline = IcebergStreamingPipeline(config)

    # Step 3: Create the Iceberg table
    pipeline.create_table()

    # Step 4: Start the streaming query
    pipeline.start_streaming_query()


if __name__ == "__main__":
    main()
