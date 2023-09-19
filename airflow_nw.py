def read_data_from_oracle_and_produce_to_kafka(full_load=False):
    """Reads data from Oracle and produces it to Kafka using PySpark.

    Args:
        full_load (bool): Whether to perform a full load or an incremental load.
    """
    
    import pyspark

    # Get the JDBC connection properties from Airflow variables
    jdbc_properties = airflow.models.Variable.get('oracle_jdbc_properties')

    # Get the Kafka producer properties from Airflow variables
    kafka_properties = airflow.models.Variable.get('kafka_producer_properties')

    # Get the full load and incremental load flags from Airflow variables
    full_load_flag = airflow.models.Variable.get('full_load_flag')
    incremental_load_flag = airflow.models.Variable.get('incremental_load_flag')

    # Create a Spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    # Read the data from Oracle
    if full_load_flag == 'True' or full_load:
        df = spark.read.format("jdbc").options(**jdbc_properties).option("table", "emp").load()
    else:
        # Get the last time the function was run
        last_run_datetime = airflow.utils.dates.days_ago(1)

        # Read the data from Oracle since the last run datetime
        df = spark.read.format("jdbc").options(**jdbc_properties).option("table", "emp").where(F.col("last_update_timestamp") > last_run_datetime).load()
    # Cache the data in memory
    df.cache()

    # Write the data to Kafka
    df.select(F.col("empno").cast("string") as "key", F.col("ename").cast("string") as "value").write.format("kafka").options(**kafka_properties).option("topic", "my-topic").save()

    # Unpersist the data from memory
    df.unpersist()
