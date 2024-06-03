# Import necessary libraries and modules
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession

# Function to create a Spark session
def create_spark_connection():
    try:
        # Build the Spark session with necessary configurations
        s_conn = (SparkSession.builder
                  .appName("Spark streaming from Kafka")  # Set the application name
                  # .master("local[*]")  # Optionally configure the master for Spark cluster
                  .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Add required packages
                  .config("spark.cassandra.connection.host", "localhost")  # Set Cassandra connection host
                  .getOrCreate())  # Create the Spark session

        s_conn.sparkContext.setLogLevel("ERROR")  # Set log level to ERROR
        logging.info("Spark connection successful")  # Log success message

        # Optionally print out the configurations of the Spark session
        # print("SparkSession configurations:")
        # print("Master:", s_conn.sparkContext.master)
        # print("Cassandra Connection Host:", s_conn.conf.get("spark.cassandra.connection.host"))

        return s_conn  # Return the Spark session

    except Exception as e:
        logging.error(f"Spark connection unsuccessful due to {e}")  # Log error message if connection fails
        return None  # Return None if connection fails

# Function to create a Cassandra connection
def create_cassandra_connection():
    try:
        # Set up authentication for Cassandra
        auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        cluster = Cluster(["localhost"], auth_provider=auth_provider)  # Connect to the Cassandra cluster
        session = cluster.connect()  # Create a session
        logging.info("Cassandra connection successful")  # Log success message
        return session  # Return the Cassandra session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")  # Log error message if connection fails
        return None  # Return None if connection fails

# Function to create a connection to Kafka using Spark
def create_spark_kafka_connection(spark_conn):
    # Define Kafka connection parameters
    kafka_params = {
        "kafka.bootstrap.servers": "localhost:9092",  # Set Kafka bootstrap servers
        "subscribe": "random_user_api",  # Subscribe to the Kafka topic
        "startingOffsets": "earliest",  # Start reading from the earliest offset
    }

    try:
        # Create a DataFrame by reading from Kafka stream
        spark_df = spark_conn \
            .readStream \
            .format("kafka") \
            .options(**kafka_params) \
            .load()

        logging.info("Kafka DataFrame created successfully")  # Log success message

        return spark_df  # Return the Kafka DataFrame
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created because: {e}")  # Log error message if connection fails
        return None  # Return None if connection fails
