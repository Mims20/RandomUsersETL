## Random User Data Pipeline

This project is a comprehensive data pipeline that integrates various technologies including Apache Kafka, Apache Spark, Cassandra, PostgreSQL, and Streamlit to process and visualize random user data fetched from an external API.

### Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Setup](#setup)
4. [Components](#components)
   - [Data Pipeline](#data-pipeline)
   - [Streaming and Processing](#streaming-and-processing)
   - [Visualization](#visualization)
   - [Docker Compose](#docker-compose)
5. [Usage](#usage)
6. [Contributing](#contributing)
7. [License](#license)

## Overview

The project demonstrates a real-time data pipeline using the following components:
- **Kafka** for message streaming
- **Spark** for data processing
- **Cassandra** for data storage
- **PostgreSQL** for relational data storage
- **Streamlit** for data visualization

The pipeline fetches random user data from an API, streams it using Kafka, processes it with Spark, and stores it in Cassandra and PostgreSQL databases. Finally, the data is visualized using Streamlit.

## Architecture

The architecture consists of:
1. Data fetching from the Random User API.
2. Data streaming to Kafka.
3. Data processing with Spark.
4. Data storage in Cassandra and PostgreSQL.
5. Data visualization using Streamlit.

## Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.9 or higher

### Installation

1. Clone the repository
2. Set up environment variables
3. Install libraries and packages in requirements.txt: `pip install -r requirements.txt`
4. Initialize airflow: `airflow init db` and create a user with Admin role using the `airflow users create [options]` command
5. Start airflow webserver and scheduler: `airflow webserver`. `airflow scheduler`
6. Start the services using Docker Compose: `docker-compose up -d`

### Components

#### Data Pipeline
The kafka_stream.py script sets up an Apache Airflow DAG to automate the process of fetching random user data from the API and streaming it to Kafka. The script includes configurations for the database and Kafka, functions to fetch and format data, and an Airflow task definition to stream data.

#### Streaming and Processing
The spark_strea.py script sets up a Spark job to read from Kafka, process the data, and write it to Cassandra. It includes functions to create the Cassandra keyspace and table, format Kafka data, and run the Spark streaming job.

#### Visualization
The streamlit_app.py script uses Streamlit to visualize the data stored in Cassandra. It initializes a Spark session, reads data from Cassandra, computes aggregations, and displays various plots and dataframes.

#### Docker Compose
The docker-compose.yml file sets up the necessary services for the project including Zookeeper, Kafka, Control Center, Spark Master and Worker, and Cassandra.

#### Usage
Start the services using Docker Compose: `docker-compose up -d`

