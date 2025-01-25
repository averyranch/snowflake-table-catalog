import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IcebergDataPipeline")

class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.spark = self._initialize_spark()

    def _initialize_spark(self):
        """Initialize Spark session with Iceberg and S3 configurations."""
        logger.info("Initializing Spark session.")
        return SparkSession.builder \
            .appName(self.config["app_name"]) \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hive") \
            .config("spark.sql.catalog.my_catalog.warehouse", self.config["warehouse"]) \
            .config("spark.hadoop.fs.s3a.access.key", self.config["aws_access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config["aws_secret_key"]) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .getOrCreate()

    def list_tables(self, catalog, database):
        """List tables in a given catalog and database."""
        logger.info(f"Listing tables in catalog: {catalog}, database: {database}")
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()
            table_list = [table['tableName'] for table in tables]
            logger.info(f"Found tables in {catalog}.{database}: {table_list}")
            return table_list
        except Exception as e:
            logger.error(f"Error listing tables in {catalog}.{database}: {e}")
            raise

    def read_table(self, catalog, database, table):
        """Read data from an Iceberg table."""
        logger.info(f"Reading table: {catalog}.{database}.{table}")
        try:
            df = self.spark.sql(f"SELECT * FROM {catalog}.{database}.{table}")
            logger.info(f"Successfully read table: {catalog}.{database}.{table}")
            return df
        except Exception as e:
            logger.error(f"Error reading table {catalog}.{database}.{table}: {e}")
            raise

    def transform_table(self, df):
        """Transform the data."""
        logger.info("Transforming table data.")
        try:
            # Example transformation: add a new column and filter rows
            df_transformed = df.withColumn("new_column", col("id") * 10) \
                               .filter(col("id").isNotNull())
            logger.info("Transformation completed.")
            return df_transformed
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            raise

    def insert_into_table(self, catalog, database, table, df):
        """Insert transformed data back into an Iceberg table."""
        logger.info(f"Inserting data into table: {catalog}.{database}.{table}")
        try:
            df.writeTo(f"{catalog}.{database}.{table}").append()
            logger.info(f"Data successfully inserted into {catalog}.{database}.{table}")
        except Exception as e:
            logger.error(f"Error inserting data into table {catalog}.{database}.{table}: {e}")
            raise

    def run(self):
        """Run the data pipeline."""
        logger.info("Starting data pipeline...")
        try:
            catalog = self.config["catalog"]
            database = self.config["database"]
            target_table = self.config["target_table"]

            # List tables in the catalog and database
            tables = self.list_tables(catalog, database)

            for table in tables:
                logger.info(f"Processing table: {table}")

                # Read data from the source table
                df = self.read_table(catalog, database, table)

                # Transform the data
                df_transformed = self.transform_table(df)

                # Insert transformed data into the target table
                self.insert_into_table(catalog, database, target_table, df_transformed)

                # Log a preview of the transformed data
                logger.info(f"Transformed data for {table}:")
                df_transformed.show(5)
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise


# Main Program
def main():
    # Configuration
    catalog = input("Enter catalog name: ")
    database = input("Enter database name: ")
    target_table = input("Enter target Iceberg table name: ")

    config = {
        "app_name": "IcebergDataPipeline",
        "catalog": catalog,
        "database": database,
        "target_table": target_table,
        "warehouse": "s3a://your-bucket-name/warehouse/",
        "aws_access_key": "YOUR_AWS_ACCESS_KEY",
        "aws_secret_key": "YOUR_AWS_SECRET_KEY"
    }

    # Run the pipeline
    pipeline = DataPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
