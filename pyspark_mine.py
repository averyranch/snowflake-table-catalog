# 2. Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("token_name", StringType(), True),
    StructField("value", DoubleType(), True)
])

# 3. Create sample data
data = [
    (1, "Bitcoin", 50000.0),
    (2, "Ethereum", 3000.0),
    (3, "Cardano", 1.2),
    (4, "Polkadot", 25.0),
    (5, "Ripple", 0.8),
    (6, "Solana", 100.0),
    (7, "Litecoin", 150.0),
    (8, "Chainlink", 20.0),
    (9, "Avalanche", 70.0),
    (10, "Dogecoin", 0.08)
]

# 4. Create DataFrame
df = spark.createDataFrame(data, schema)

# 5. Define S3 bucket path
s3_path = "s3a://your-bucket-name/path-to-save-data"

# 6. Write the DataFrame to S3 as Parquet for Iceberg
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(s3_path)

print(f"Data successfully written to {s3_path}")

# Stop the SparkSession
spark.stop()
