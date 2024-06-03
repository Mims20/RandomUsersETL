# Import necessary libraries and modules
import logging
import uuid

from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType

# Import custom connection functions
from connections import create_spark_connection, create_cassandra_connection, create_spark_kafka_connection

# Function to create a keyspace in Cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS random_users 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    session.set_keyspace("random_users")
    logging.info("Keyspace created or already exists.")

# Function to create a table in the Cassandra keyspace
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS random_users.users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    logging.info("Table created successfully!")

# Commented-out function to insert data into the Cassandra table
# def insert_data(session, **kwargs):
#     logging.info("Inserting data...")
#
#     id = str(uuid.uuid4())
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     city = kwargs.get('city')
#     state = kwargs.get('state')
#     country = kwargs.get('country')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')
#
#     try:
#         session.execute("""
#             INSERT INTO random_users.users(id, first_name, last_name, gender, address, city, state,
#             country, post_code, email, username, dob, registered_date, phone, picture)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, gender, address, city, state, country, postcode, email,
#               username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")
#
#     except Exception as e:
#         logging.error(f"Could not insert data due to {e}")

# Function to format data from Kafka stream
def format_kafka_data(spark_dataframe):
    # Define schema for the data
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("country", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Select and parse the JSON data from Kafka stream
    formatted_data = spark_dataframe.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")

    return formatted_data

# Main execution block
if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Create Kafka DataFrame from Spark connection
        spark_df = create_spark_kafka_connection(spark_conn=spark_conn)
        if spark_df:
            spark_df.printSchema()  # Print schema of the Kafka DataFrame
            formatted_df = format_kafka_data(spark_df)  # Format the Kafka data
            formatted_df.printSchema()  # Print schema of the formatted DataFrame
            # formatted_df.show(truncate=False)  # Show formatted DataFrame (commented out)

            # Create Cassandra connection
            cassandra_session = create_cassandra_connection()
            if cassandra_session:
                # Create keyspace and table in Cassandra
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                logging.info("Streaming is being started...")

                # Define UDF to generate UUIDs
                def generate_uuid():
                    return str(uuid.uuid4())

                # Register the UDF with Spark
                uuid_udf = udf(generate_uuid, StringType())

                # Add a new column 'id' with UUIDs to the DataFrame
                formatted_df = formatted_df.withColumn("id", uuid_udf())

                # Define the streaming query to write data to Cassandra
                streaming_query = formatted_df.writeStream \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("checkpointLocation", "/tmp/checkpoint") \
                    .option("keyspace", "random_users") \
                    .option("table", "users") \
                    .start()

                # Await termination of the streaming query
                streaming_query.awaitTermination()
        else:
            logging.error("Failed to create Kafka DataFrame")
    else:
        logging.error("Failed to create Spark connection")
