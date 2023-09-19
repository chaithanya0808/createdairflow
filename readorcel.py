import pyspark

def get_jdbc_properties():
    """Returns a dictionary of JDBC connection properties."""
    return {
        "driver": "oracle.jdbc.driver.OracleDriver",
        "url": "jdbc:oracle:thin:@localhost:1521/" + database_name,
        "user": "scott",
        "password": "tiger"
    }

# Create a Spark session
spark = pyspark.sql.SparkSession.builder.getOrCreate()
database_name ="orcl"
# Get the JDBC connection properties
jdbc_properties = get_jdbc_properties(database)

table_name ="XYZ"



# Read the data from Oracle
df = spark.read.format("jdbc").options(**jdbc_properties).option("table", table_name).load()

# Save the data to a DataFrame
df.show()