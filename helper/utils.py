import json
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
import streamlit as st

@st.cache_resource
def mongodb_setup():
    with open("../.nosync/mongoDB.json", "r") as file:
        credentials = json.load(file)

    url = (
        f"mongodb+srv://{credentials['usr']}:"
        + credentials["pwd"]
        + credentials["ext"]
    )

    mdb_client = MongoClient(url, server_api=ServerApi("1"))

    try:
        mdb_client.admin.command("ping")
        print("Pinged your deployment. Successfully connected to MongoDB.")
        return mdb_client
    except Exception as exceptionMsg:
        print(exceptionMsg)


@st.cache_resource
def spark_setup():
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/"
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
    os.environ["PYSPARK_HADOOP_VERSION"] = "without"

    spark = (
        SparkSession.builder.appName("SparkCassandraApp")
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
        )
        .config("spark.cassandra.connection.host", "localhost")
        .config(
            "spark.sql.extensions",
            "com.datastax.spark.connector.CassandraSparkExtensions",
        )
        .config(
            "spark.sql.catalog.mycatalog",
            "com.datastax.spark.connector.datasource.CassandraCatalog",
        )
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.task.maxFailures", "10")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    print("Successfully connected to Cassandra DB through Spark.")

    return spark

@st.cache_data
def retrieve_from_cas(_spark, table, keyspace):
    df_spark = (
        _spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load()
    )
    print(f"{keyspace}.{table} successfully retrieved.")
    return df_spark.toPandas()
