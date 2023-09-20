def read_oracle_write_kafka():

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import *


# Set your Oracle database connection properties
oracle_connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "oracle.jdbc.OracleDriver",
    "url": "jdbc:oracle:thin:@<hostname>:<port>/<sas_aml_DB>"
}

with open('column_names.json', 'r') as json_file:
    column_data = json.load(json_file)

column_names = column_data["columns"]

# Set your Kafka configuration
kafka_bootstrap_servers = "your_kafka_bootstrap_servers"
kafka_topic = "your_kafka_topic"

# Create a SparkSession
spark = SparkSession.builder 
    .appName("OracleToKafka").config() #Need to add serializiblity part for performance tuning
    .getOrCreate()

# Define the Oracle source table schema
source_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("NAME", StringType(), True)
])

# Read data from Oracle
oracle_df = spark.read.jdbc(url=oracle_connection_properties["url"],
                            table="party_fc_profile",
                            properties=oracle_connection_properties
                            column=column_names)

# Select columns and rename if needed
oracle_df = oracle_df.selectExpr("ID", "NAME as VALUE") # column_names

# Convert DataFrame to JSON
json_df = oracle_df.select(to_json(oracle_df.schema.names).alias("value")) 

# Write to Kafka topic
kafka_write_query = json_df \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_topic) \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

# Await termination of the streaming query
kafka_write_query.awaitTermination()
