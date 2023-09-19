from airflow import DAG
from airflow.operators import PythonOperator
import pandas


default_args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1
}

dag = DAG('my_dag', default_args=default_args, schedule_interval='@daily')


def read_data_from_oracle_and_produce_to_kafka():
    """Reads data from Oracle and produces it to Kafka using PySpark.

    Args:
        full_load (bool): Whether to perform a full load or an incremental load.
    """
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

    # Get the JDBC connection properties from Airflow variables
    jdbc_properties = airflow.models.Variable.get('oracle_connection_properties')
    #Provide a unique key (name) for your Oracle connection variable, for example, "oracle_conn_url," 
    # "oracle_conn_username," and "oracle_conn_password."
    #Variable Key: oracle_conn_url
    #Variable Value: jdbc:oracle:thin:@<hostname>:<port>/<sas_aml_DB>
    #Variable Key: oracle_conn_username
    #Variable Value: your_username
    #Variable Key: oracle_conn_password
    #Variable Value: your_password

    # Get the Kafka producer properties from Airflow variables
    #kafka_properties = airflow.models.Variable.get('kafka_producer_properties')
    kafka_bootstrap_servers = "your_kafka_bootstrap_servers"
    kafka_topic = "your_kafka_topic"

    # Get the full load and incremental load flags from Airflow variables
    #full_load_flag = airflow.models.Variable.get('full_load_flag')
    #incremental_load_flag = airflow.models.Variable.get('incremental_load_flag')
    
    # Create a Spark session
    spark = pyspark.sql.SparkSession.builder.config(which serilization needed).getOrCreate()

    # Read the data from Oracle
    #if full_load_flag == 'True' or full_load:
    #df = spark.read.format("jdbc").options(**jdbc_properties).option("table", "emp").load()
    #else:
        # Get the last time the function was run
        #last_run_datetime = airflow.utils.dates.days_ago(1)

        # Read the data from Oracle since the last run datetime
        #df = spark.read.format("jdbc").options(**jdbc_properties).option("table", "emp").where(F.col("last_update_timestamp") > last_run_datetime).load()
    # Cache the data in memory
    #kafka_bootstrap_servers = "your_kafka_bootstrap_servers"
    #kafka_topic = "your_kafka_topic"

# Create a SparkSession
  #spark = SparkSession.builder 
   # .appName("OracleToKafka").config() #Need to add serializiblity part for performance tuning
   # .getOrCreate()

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
oracle_df = oracle_df.selectExpr("ID", "NAME as VALUE","city as city_name") # column_names

oracle_df.cache()
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
     

    # Write the data to Kafka
    #df.select(F.col("empno").cast("string") as "key", F.col("ename").cast("string") as "value").write.format("kafka").options(**kafka_properties).option("topic", "my-topic").save()

    # Unpersist the data from memory
    


# Create a PythonOperator task to read the data from Oracle and produce it to Kafka
read_data_from_oracle_and_produce_to_kafka_task = PythonOperator(
    task_id='read_data_from_oracle_and_produce_to_kafka',
    python_callable=read_data_from_oracle_and_produce_to_kafka,
    dag=dag
)

# Schedule the DAG
read_data_from_oracle_and_produce_to_kafka.run()

