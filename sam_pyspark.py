from pyspark.sql import SparkSession
import os

# Step 1: Set Kerberos and Hadoop configurations
keytab_path = "/path/to/your.keytab"  # Replace with your keytab file path
krb5_conf_path = "/path/to/krb5.conf"  # Replace with your krb5.conf file path
hive_principal = "hive/_HOST@YOUR-REALM.COM"  # Replace with your Hive principal
hive_metastore_uri = "thrift://hive-metastore-host:9083"  # Replace with your Hive Metastore URI

# Explicitly set Kerberos environment variables
os.environ["KRB5_KTNAME"] = keytab_path
os.environ["KRB5_CONFIG"] = krb5_conf_path

# Authenticate using Kerberos (optional if using keytab in configuration)
os.system(f"kinit -kt {keytab_path} your-principal@YOUR-REALM.COM")  # Replace `your-principal`

# Step 2: Initialize SparkSession with Kerberos-enabled Hive support
spark = SparkSession.builder \
    .appName("HiveMetastoreWithKerberos") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", hive_metastore_uri) \
    .config("hive.metastore.sasl.enabled", "true") \
    .config("hive.metastore.kerberos.principal", hive_principal) \
    .config("hive.metastore.kerberos.keytab.file", keytab_path) \
    .config("spark.hadoop.security.authentication", "kerberos") \
    .config("spark.hadoop.security.authorization", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 3: Query Hive Metastore to list databases and tables
print("Databases in Hive Metastore:")
databases = spark.sql("SHOW DATABASES").collect()
for db in databases:
    print(db.databaseName)

database_name = "default"  # Replace with your database name
print(f"Tables in database '{database_name}':")
spark.sql(f"USE {database_name}")
tables = spark.sql("SHOW TABLES").collect()
for table in tables:
    print(table.tableName)

# Step 4: Read a table from Hive
table_name = "your_table_name"  # Replace with your table name
df = spark.sql(f"SELECT * FROM {table_name}")
df.show()

# Step 5: Stop the SparkSession
spark.stop()
