from pyspark.sql import SparkSession

def main():
    # Step 1: Initialize Spark Session
    spark = SparkSession.builder \
        .appName("InsertIntoParquetIcebergTable") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hive") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://your-bucket-name/warehouse/") \
        .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
        .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    catalog = "my_catalog"
    database = "my_database"
    table = "my_table"

    # Step 2: Create the table if it doesn't exist
    logger.info(f"Checking if table {catalog}.{database}.{table} exists...")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
        id INT,
        name STRING,
        age INT
    )
    USING iceberg
    PARTITIONED BY (age)
    """)
    logger.info(f"Table {catalog}.{database}.{table} is ready.")

    # Step 3: Insert data into the table
    logger.info(f"Inserting data into table {catalog}.{database}.{table}...")
    insert_sql = f"""
    INSERT INTO {catalog}.{database}.{table}
    VALUES (1, 'Alice', 25)
    """
    spark.sql(insert_sql)
    logger.info("Data inserted successfully.")

    # Step 4: Cache the table (optional)
    logger.info(f"Caching table {catalog}.{database}.{table}...")
    spark.sql(f"CACHE TABLE {catalog}.{database}.{table}")

    # Step 5: Verify the data
    logger.info(f"Verifying data in table {catalog}.{database}.{table}...")
    result = spark.sql(f"SELECT * FROM {catalog}.{database}.{table}")
    result.show()

    # Step 6: Uncache the table after verification (optional)
    logger.info(f"Uncaching table {catalog}.{database}.{table}...")
    spark.sql(f"UNCACHE TABLE {catalog}.{database}.{table}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    import logging
    # Configure Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("IcebergPipeline")

    main()
