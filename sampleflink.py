"""Module to verify PyFlink installation and runtime functionality.

This script sets up a streaming PyFlink environment, creates a sample table with
generated data, performs a simple aggregation, and confirms operational status.
"""

import logging
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import lit
from pyflink.common import Row

# Configure logging (FAANG-style: detailed, structured logs)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class PyFlinkVerifier:
    """Class to encapsulate PyFlink setup and verification logic."""

    def __init__(self):
        """Initialize the PyFlink environment in streaming mode."""
        try:
            self.env_settings = EnvironmentSettings.in_streaming_mode()
            self.table_env = TableEnvironment.create(self.env_settings)
            logger.info("PyFlink environment initialized successfully.")
        except Exception as e:
            logger.error("Failed to initialize PyFlink environment: %s", e)
            raise RuntimeError("PyFlink setup failed.") from e

    def create_sample_table(self) -> None:
        """Create a temporary table with generated data."""
        create_table_sql = """
            CREATE TABLE temp_table (
                value INT
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '5',
                'number-of-rows' = '10'
            )
        """
        try:
            self.table_env.execute_sql(create_table_sql)
            logger.info("Sample table 'temp_table' created with datagen connector.")
        except Exception as e:
            logger.error("Failed to create sample table: %s", e)
            raise

    def get_static_data(self) -> List[Tuple[int]]:
        """Return a small static dataset for verification."""
        return [(1,), (2,), (3,)]  # Simple integers for sum

    def verify_execution(self) -> None:
        """Execute a query to sum values and confirm PyFlink is active."""
        try:
            # Static data as a table
            static_data = self.get_static_data()
            static_table = self.table_env.from_elements(static_data, ['value'])
            logger.info("Static data table created with values: %s", static_data)

            # Query: Sum from generated table and static data
            query = """
                SELECT SUM(value) AS total FROM temp_table
                UNION ALL
                SELECT SUM(value) AS total FROM {static_table}
            """.format(static_table="temp_table_static")  # Named for clarity
            self.table_env.register_table("temp_table_static", static_table)

            # Execute and print result
            result = self.table_env.sql_query(query)
            logger.info("Executing verification query...")
            result.execute().print()

            logger.info("PyFlink is enabled and active!")
        except Exception as e:
            logger.error("Execution failed: %s", e)
            raise

def main():
    """Main entry point to run the PyFlink verification."""
    verifier = PyFlinkVerifier()
    verifier.create_sample_table()
    verifier.verify_execution()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical("Program terminated due to error: %s", e)
        exit(1)
