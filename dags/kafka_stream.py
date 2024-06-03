# Import necessary libraries and modules
import json
import logging
import os
import time
from datetime import timedelta

import pendulum  # For timezone and datetime manipulation
import requests  # For making HTTP requests
from airflow.operators.python import PythonOperator  # For creating Airflow tasks using Python functions
from kafka import KafkaProducer, KafkaAdminClient  # For Kafka operations
# from kafka.admin import NewTopic  # Kafka topic creation (commented out)
from airflow import DAG  # For creating Directed Acyclic Graphs in Airflow
from sqlalchemy import create_engine, Column, String, Integer, DateTime  # For SQLAlchemy ORM and database operations
from sqlalchemy.ext.declarative import declarative_base  # For SQLAlchemy base class
from sqlalchemy.orm import sessionmaker  # For creating database sessions

from dotenv import load_dotenv

load_dotenv()

# Database configuration from environment variables
DATABASE_USER = os.getenv("user")
DATABASE_PASSWORD = os.getenv("password")
DATABASE_HOST = os.getenv("host")
DATABASE_PORT = os.getenv("port")
DATABASE_NAME = os.getenv("dbname")

# API URL for fetching random user data
URL = "https://randomuser.me/api"

# Database connection URL
DATABASE_URL = f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "random_user_api"

# Base class for SQLAlchemy models
Base = declarative_base()


# Define User model for SQLAlchemy
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    gender = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    country = Column(String, nullable=False)
    post_code = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True)
    username = Column(String, nullable=False, unique=True)
    dob = Column(DateTime, nullable=False)
    registered_date = Column(DateTime, nullable=False)
    phone = Column(String, nullable=False)
    picture = Column(String, nullable=False)


# Create SQLAlchemy engine and session maker
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create database tables if they do not exist
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logging.error(f"Error creating database tables: {e}")

# Default arguments for Airflow DAG
default_args = {
    "owner": "Selase",
    "start_date": pendulum.yesterday(),
    "email": ["selase@somemail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

# Define the Airflow DAG
dag = DAG(
    "random_user_automation",
    default_args=default_args,
    description="airflow, kafka, spark, cassandra streaming",
    schedule_interval=timedelta(days=1),
)


# Function to fetch random user data from the API
def get_random_user():
    response = requests.get(URL)
    response.raise_for_status()  # Raise an exception for HTTP errors
    response = response.json()
    return response["results"][0]


# Function to format the user data
def format_data(res):
    location = res['location']
    return {
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}",
        'city': location['city'],
        'state': location['state'],
        'country': location['country'],
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium'],
    }


# Function to stream data to Kafka and save to the database
def stream_data():
    logging.info("Starting data streaming...")
    current_time = time.time()
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                   client_id="random_user_api")
    db = SessionLocal()

    try:
        while time.time() < current_time + 60:  # Run for 60 seconds
            try:
                response = get_random_user()  # Fetch random user data
                formatted_data = format_data(res=response)  # Format the data
                kafka_producer.send(topic=KAFKA_TOPIC,
                                    value=json.dumps(formatted_data).encode("utf-8"))
                kafka_producer.flush()  # Ensure all messages are sent

                user = User(**formatted_data)  # Create a new User object
                db.add(user)  # Add to the session
                db.commit()  # Commit the session
            except Exception as e:
                db.rollback()  # Rollback in case of error
                logging.error(f"An error occurred: {e}")
    finally:
        kafka_producer.close()  # Close the Kafka producer
        db.close()  # Close the database session
        logging.info("Data streaming completed.")


# Task to create Kafka topic (commented out)
# def create_kafka_topic():
#     admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     new_topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
#     admin_client.create_topics(new_topics=[new_topic])

# Define the Airflow task
streaming_task = PythonOperator(
    task_id="streaming_task",
    python_callable=stream_data,
    dag=dag
)

# Set the task in the DAG
streaming_task
