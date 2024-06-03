# Import necessary libraries and modules
import logging
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import year
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

from connections import create_spark_connection, create_cassandra_connection

# Initialize Spark session and cache it to optimize performance
@st.cache_resource(show_spinner=False)
def initialize_spark_connection():
    return create_spark_connection()

# Uncomment and use this function to initialize Cassandra connection if needed
# @st.cache_resource(show_spinner=False)
# def initialize_cassandra_connection():
#     return create_cassandra_connection()

# Function to load data from Cassandra into a Spark DataFrame
def load_data(spark):
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users", keyspace="random_users") \
        .load()
    return df

# Function to compute various aggregations from the DataFrame
# @st.cache_data(show_spinner=False)
def compute_aggregations(df):
    # Count by country
    count_by_country = df.groupBy("country").count().toPandas()
    count_by_country = count_by_country.rename(columns={"count": "total"})

    # Count by gender
    count_by_gender = df.groupBy("gender").count().toPandas()
    count_by_gender = count_by_gender.rename(columns={"count": "total"})

    # Count by year of registration
    df = df.withColumn("registration_year", year("registered_date"))
    count_by_year = df.groupBy("registration_year").count().toPandas()
    count_by_year = count_by_year.rename(columns={"count": "total"})
    count_by_year = count_by_year.sort_values(by="registration_year")

    return count_by_country, count_by_gender, count_by_year

# Initialize Spark session and load data from Cassandra
spark = initialize_spark_connection()
# cassandra_session = initialize_cassandra_connection()  # Uncomment if Cassandra session is used directly
cassandra_df = load_data(spark)

# Compute necessary aggregations
count_by_country, count_by_gender, count_by_year = compute_aggregations(cassandra_df)

# Streamlit app layout
st.title("User Data Aggregations")

# Plotting graphs with Plotly

# Count by country bar chart
fig_count_by_country = px.bar(count_by_country, x='country', y='total', color='country',
                              title='Total Users by Country',
                              labels={'total': 'Total', 'country': 'Country'}, template='plotly')
fig_count_by_country.update_layout(xaxis_tickangle=-45)

# Count by gender bar chart
fig_count_by_gender = px.bar(count_by_gender, x='gender', y='total', color='gender',
                             title='Total Users by Gender',
                             labels={'total': 'Total', 'gender': 'Gender'}, template='plotly')

# Count by year of registration line chart using Plotly
fig_count_by_year = px.line(count_by_year, x='registration_year', y='total',
                            title='Total Users by Year of Registration',
                            labels={'total': 'Total', 'registration_year': 'Registration Year'}, template='plotly')

# Pie chart for count by country
fig_pie_by_country = px.pie(count_by_country, names='country', values='total',
                            title='Pie Chart: Total Users by Country',
                            labels={'total': 'Total', 'country': 'Country'}, template='plotly')

# Map visualization using Plotly Choropleth
fig_map = px.choropleth(count_by_country, locations='country', locationmode='country names', color='total',
                        hover_name='country', title='Users by Country', color_continuous_scale='Viridis',
                        labels={'total': 'Total Users'})

# Map visualization using Plotly Scatter Geo
fig_map2 = px.scatter_geo(count_by_country, locations='country', locationmode='country names',
                          hover_name='country', title='Countries in Database', template='plotly',
                          projection='natural earth')

fig_map2.update_traces(marker=dict(symbol='circle', size=10, color='red'))

# Displaying graphs in Streamlit
st.plotly_chart(fig_count_by_country, use_container_width=True)
st.plotly_chart(fig_count_by_gender, use_container_width=True)
st.plotly_chart(fig_pie_by_country, use_container_width=True)
st.plotly_chart(fig_count_by_year, use_container_width=True)
st.plotly_chart(fig_map, use_container_width=True)
st.plotly_chart(fig_map2, use_container_width=True)

# Display dataframes with aggregations in Streamlit
st.subheader("Data Overview")
col1, col2, col3 = st.columns(3, gap="large")

with col1:
    st.write("Total by Country")
    st.dataframe(count_by_country)

with col2:
    st.write("Total by Gender")
    st.dataframe(count_by_gender)

with col3:
    st.write("Total by Year of Registration")
    st.dataframe(count_by_year)
