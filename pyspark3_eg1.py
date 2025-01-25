import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataPipeline")

class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.spark = self._initialize_spark()

    def _initialize_spark(self):
        """Initialize Spark session."""
        logger.info("Initializing Spark session.")
        return SparkSession.builder \
            .appName(self.config["app_name"]) \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
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
        """Read a table into a DataFrame."""
        logger.info(f"Reading table: {catalog}.{database}.{table}")
        try:
            df = self.spark.sql(f"SELECT * FROM {catalog}.{database}.{table}")
            logger.info(f"Successfully read table: {catalog}.{database}.{table}")
            return df
        except Exception as e:
            logger.error(f"Error reading table {catalog}.{database}.{table}: {e}")
            raise

    def transform_table(self, df):
        """Transform the data (example: filter and add a column)."""
        logger.info("Transforming table data.")
        try:
            df_transformed = df.withColumn("new_column", col("id") * 100)
            logger.info("Transformation completed.")
            return df_transformed
        except Exception as e:
            logger.error("Error during transformation: {e}")
            raise

    def run(self):
        """Run the data pipeline."""
        logger.info("Starting data pipeline...")
        try:
            catalog = self.config["catalog"]
            database = self.config["database"]

            # List tables
            tables = self.list_tables(catalog, database)
            for table in tables:
                logger.info(f"Processing table: {table}")

                # Read table
                df = self.read_table(catalog, database, table)

                # Transform data (optional)
                df_transformed = self.transform_table(df)

                # Print transformed data (for demonstration)
                logger.info(f"Transformed data for {table}:")
                df_transformed.show(5)
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise


# Main Program
def main():
    # Configuration with user inputs for catalog and database
    catalog = input("Enter catalog name: ")
    database = input("Enter database name: ")

    config = {
        "app_name": "FAANG Data Pipeline",
        "catalog": catalog,
        "database": database,
    }

    # Run the pipeline
    pipeline = DataPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
