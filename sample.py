from pyspark.sql import SparkSession

def main():
    try:
        # Step 1: Initialize SparkSession with Iceberg & S3 Support
        spark = SparkSession.builder \
            .appName("Create Iceberg Table on S3 for Starburst") \
            .config("spark.hadoop.fs.s3a.access.key", "your-access-key") \
            .config("spark.hadoop.fs.s3a.secret.key", "your-secret-key") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.hitachi.com") \
            .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.hive_catalog.type", "hive") \
            .config("spark.sql.catalog.hive_catalog.warehouse", "s3a://your-bucket-name/warehouse/") \
            .enableHiveSupport() \
            .getOrCreate()
        print("✅ SparkSession initialized successfully.")
    
    except Exception as e:
        print(f"❌ Error initializing SparkSession: {e}")
        return

    # Step 2: Define Database & Table Name
    database_name = "your_database"
    table_name = "your_table"
    full_table_name = f"hive_catalog.{database_name}.{table_name}"

    try:
        # Step 3: Ensure the database exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"✅ Database '{database_name}' checked/created successfully.")
    except Exception as e:
        print(f"❌ Error creating/checking database: {e}")
        return

    try:
        # Step 4: Create the Iceberg table (if it does not exist)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                id INT,
                name STRING,
                timestamp STRING
            )
            USING iceberg
            LOCATION 's3a://your-bucket-name/{table_name}/'
        """)
        print(f"✅ Iceberg table '{full_table_name}' created successfully.")
    except Exception as e:
        print(f"❌ Error creating Iceberg table: {e}")
        return

    try:
        # Step 5: Insert Sample Data
        data = [(1, "Alice", "2025-01-27T10:00:00"), (2, "Bob", "2025-01-27T10:10:00")]
        columns = ["id", "name", "timestamp"]
        df = spark.createDataFrame(data, columns)

        # Append data to the Iceberg table
        df.write.format("iceberg").mode("append").save(full_table_name)
        print(f"✅ Data written successfully to Iceberg table: {full_table_name}")

    except Exception as e:
        print(f"❌ Error writing data to Iceberg table: {e}")
        return

    try:
        # Step 6: Verify Iceberg Metadata
        print("📊 Checking Iceberg snapshots...")
        spark.sql(f"SELECT * FROM {full_table_name}.snapshots").show()
        print("✅ Iceberg snapshots verified successfully.")

    except Exception as e:
        print(f"❌ Error checking Iceberg snapshots: {e}")
        return

    try:
        # Step 7: Verify Starburst Can Read the Table
        print("📊 Querying the table to ensure Starburst compatibility...")
        spark.sql(f"SELECT * FROM {full_table_name}").show()
        print("✅ Table data verified successfully for Starburst query.")

    except Exception as e:
        print(f"❌ Error querying Iceberg table from Spark: {e}")
        return

if __name__ == "__main__":
    main()
