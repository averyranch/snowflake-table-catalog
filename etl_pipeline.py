import os
import yaml
import logging
import trino
import time
from datetime import datetime
from trino.auth import BasicAuthentication

class TrinoETL:
    """Handles ETL operations for full table reloads using Trino (Starburst)."""

    def __init__(self, config_path: str = "config.yml"):
        """Initialize ETL process by loading configuration and setting up logging."""
        self.config = self._load_config(config_path)
        self.conn = None

    @staticmethod
    def _setup_logging():
        """Configures logging for the ETL process."""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + "_etl_log.txt"
        log_filepath = os.path.join(log_dir, log_filename)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_filepath, mode="w", encoding="utf-8"),
                logging.StreamHandler()  # Print logs to console
            ]
        )

    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Load configuration from a YAML file."""
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)
            logging.info("Configuration loaded successfully.")
            return config
        except FileNotFoundError:
            logging.error(f"Configuration file '{config_path}' not found.")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {e}")
            raise

    def _connect(self):
        """Establishes a connection to Trino."""
        try:
            self.conn = trino.dbapi.connect(
                host=self.config["connection"]["host"],
                port=self.config["connection"]["port"],
                user=self.config["connection"]["user"],
                auth=BasicAuthentication(
                    self.config["connection"]["user"],
                    self.config["connection"]["password"]
                ),
                catalog=self.config["connection"]["catalog"],
                schema=self.config["connection"]["schema"],
                http_scheme=self.config["connection"]["protocol"]
            )
            logging.info("Successfully connected to Trino.")
        except Exception as e:
            logging.error(f"Failed to connect to Trino: {e}")
            raise

    def _execute_query(self, query: str):
        """Executes a given SQL query in Trino and returns execution stats."""
        if not self.conn:
            logging.error("No active connection to Trino. Ensure connection is established before querying.")
            return None

        try:
            cursor = self.conn.cursor()
            start_time = time.time()  # Start time tracking
            cursor.execute(query)
            rows_affected = cursor.rowcount  # Number of rows affected
            end_time = time.time()  # End time tracking
            execution_time = round(end_time - start_time, 2)

            logging.info(f"Query executed successfully in {execution_time} seconds.")
            logging.info(f"Total records copied: {rows_affected}")

            cursor.close()
            return rows_affected, execution_time
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise

    def full_load_ctas(self, source_table: str, destination_table: str, s3_location: str):
        """Performs a full table load using `CREATE OR REPLACE TABLE` (CTAS) and logs stats."""
        query = f"""
        CREATE OR REPLACE TABLE {destination_table} 
        WITH (
            format = 'PARQUET',
            location = '{s3_location}'
        ) AS
        SELECT * FROM {source_table};
        """
        logging.info(f"Executing full load from {source_table} to {destination_table}...")
        rows_copied, execution_time = self._execute_query(query)

        # Log detailed stats
        logging.info(f"Full load completed for {destination_table}.")
        logging.info(f"Total rows copied: {rows_copied}")
        logging.info(f"Time taken: {execution_time} seconds")

    def run_etl(self):
        """Runs the full ETL process."""
        self._connect()
        for table_info in self.config["tables"]:
            self.full_load_ctas(
                table_info["source"],
                table_info["destination"],
                table_info["s3_location"]
            )
        logging.info("ETL Job Completed Successfully.")
        self._close_connection()

    def _close_connection(self):
        """Closes the Trino connection gracefully."""
        if self.conn:
            self.conn.close()
            logging.info("Trino connection closed.")

if __name__ == "__main__":
    TrinoETL._setup_logging()
    etl = TrinoETL()
    etl.run_etl()
