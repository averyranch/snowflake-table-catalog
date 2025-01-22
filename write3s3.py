import boto3
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType
from pyiceberg.table import Table
from pyiceberg.data import Record

# Initialize S3 client
s3 = boto3.client('s3')

# Load or create Iceberg catalog
catalog = load_catalog("s3", uri="s3://your-bucket-name/path/to/catalog", client=s3)

# Define the schema for the Iceberg table
schema = Schema(
    IntegerType().field("id"),
    StringType().field("name")
)

# Create or load the Iceberg table
table = catalog.create_table("your_database.your_table", schema, location="s3://your-bucket-name/path/to/table")

# Create a record to insert
record = Record(
    id=1,
    name="example"
)

# Write the record to the table
with table.new_write() as writer:
    writer.write(record)

print("Data written successfully!")
