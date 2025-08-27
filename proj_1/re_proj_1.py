# %%
import sys
import os

sys.path.append(os.path.abspath(".."))

import pandas as pd
from helper.utils import mongodb_setup, spark_setup

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# PySpark<4.0.0
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import requests
from bs4 import BeautifulSoup

mdb_client = mongodb_setup()
spark = spark_setup()
cas_keyspace = "rewrite320"

# %% [markdown]
# ### Remote database MongoDB

# %%
mdb_name = "rewrite320"
mdb = mdb_client[mdb_name]

new_collections = ["gas", "municipalities"]

for name in new_collections:
    try:
        mdb.create_collection(name)
        print(f"Collection '{name}' was created successfully.")
    except:
        print(f"Collcetion '{name}' already exists.")

# %% [markdown]
# ### Local database, Cassandra accessed with Spark

# %%
# Connecting to Cassandra
from cassandra.cluster import Cluster

cluster = Cluster(["localhost"], port=9042)
session = cluster.connect()

# %%
cas_keyspace = "rewrite320"
session.execute(
    "CREATE KEYSPACE IF NOT EXISTS"
    + " "
    + cas_keyspace
    + " "
    + "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
)
session.set_keyspace(cas_keyspace)

# %% [markdown]
# ## Webscraping

# %% [markdown]
# ### Wikipedia table

# %%
# webscrape_url = "https://en.wikipedia.org/wiki/List_of_municipalities_of_Denmark"
# page = requests.get(webscrape_url)
# soup = BeautifulSoup(page.content, "html.parser")
# wiki_table = soup.find("table", attrs={"class": "wikitable sortable"})

# %%
# from io import StringIO
# df_table = pd.read_html(StringIO(str(wiki_table)))[0]

# %%
# mdb["municipalities"].delete_many({})

# %%
# df_table = df_table.rename(columns={
#     "LAU-1 code 1": "lau1_code",
#     "Municipality": "municipality",
#     "Administrative Center": "admin_center",
#     "Total Area (km²)": "area_km2",
#     "Population (2012-01-01)": "population_2021",
#     "Region": "region"
# })
# mdb["municipalities"].insert_many(df_table.to_dict("records"))

# %% [markdown]
# ### Retrieve data from API

# %%
# api_url = "https://api.energidataservice.dk/dataset/"
# filtering = "?offset=0&start=2022-01-01T00:00&end=2023-01-01T00:00"

# get_json = lambda dataset_url: requests.get(api_url + dataset_url + filtering).json()["records"]

# %%
# df_gas = pd.DataFrame.from_records(get_json("GasDailyBalancingPrice"))
# df_production = pd.DataFrame.from_records(get_json("ProductionMunicipalityHour"))
# df_consumption = pd.DataFrame.from_records(get_json("ConsumptionIndustry"))
# df_prodcons = pd.DataFrame.from_records(get_json("ProductionConsumptionSettlement"))

# %% [markdown]
# #### Insert into MongoDB

# %%
# df_gas = df_gas.rename(columns=str.lower)
# mdb["gas"].insert_many(df_gas.to_dict("records")) # Insert to MongoDB collection "gas"

# %% [markdown]
# ### Preprocessing before inserting into Cassandra

# %%
# for df in [df_gas, df_production, df_consumption, df_prodcons]:
#     print(df.dtypes)
#     print()

# %%
# df_gas["gasday"] = pd.to_datetime(df_gas["gasday"])

# df_production["HourDK"] = pd.to_datetime(df_production["HourDK"])
# df_production["HourUTC"] = pd.to_datetime(df_production["HourUTC"])
# df_production["MunicipalityNo"] = df_production["MunicipalityNo"].astype(int)

# df_consumption["HourDK"] = pd.to_datetime(df_consumption["HourDK"])
# df_consumption["HourUTC"] = pd.to_datetime(df_consumption["HourUTC"])
# df_consumption["MunicipalityNo"] = df_consumption["MunicipalityNo"].astype(int)
# df_consumption["Branche"] = df_consumption["Branche"].astype(str)

# df_prodcons["HourDK"] = pd.to_datetime(df_prodcons["HourDK"])
# df_prodcons["HourUTC"] = pd.to_datetime(df_prodcons["HourUTC"])
# df_prodcons["PriceArea"] = df_prodcons["PriceArea"].astype(str)
# df_prodcons['ExchangeGB_MWh'] = df_prodcons["ExchangeGB_MWh"].astype(float)


# %%
# for df in [df_gas, df_production, df_consumption, df_prodcons]:
#     print(df.dtypes)
#     print()

# %%
# df_prodcons = df_prodcons.rename(columns=str.lower)
# df_production = df_production.rename(columns=str.lower)
# df_consumption = df_consumption.rename(columns=str.lower)

# %% [markdown]
# ### Inserting to Cassandra

# %%
# def create_cassandra_table_query(df, keyspace, table_name):
#     # Define mapping between Pandas and Cassandra datatypes
#     dtype_mapping = {
#         'int64': 'int',
#         'float64': 'double',
#         'object': 'text',
#         'bool': 'boolean',
#         'datetime64[ns]': 'timestamp'
#     }

#     # Start constructing the CREATE TABLE query
#     query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (\n"

#     # Add primary key column with timeuuid
#     columns = ["id timeuuid"]

#     # Add remaining columns with mapped Cassandra data types
#     for col, dtype in df.dtypes.items():
#         if col != "id":  # Exclude 'id' to avoid duplication
#             cassandra_type = dtype_mapping.get(str(dtype), 'text')  # Default to 'text' if type is unrecognized
#             columns.append(f"{col} {cassandra_type}")

#     # Join columns with commas and specify primary key as 'id'
#     columns_str = ",\n    ".join(columns)
#     query += f"    {columns_str},\n"
#     query += "    PRIMARY KEY (id)\n);"

#     return query

# production_query = create_cassandra_table_query(df_production, cas_keyspace, "production")
# consumption_query = create_cassandra_table_query(df_consumption, cas_keyspace, "consumption")
# prodcons_query = create_cassandra_table_query(df_prodcons, cas_keyspace, "prodcons")

# %%
# for query in [prodcons_query, consumption_query, production_query]:
#     session.execute(query)

# %%
# query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{cas_keyspace}'"
# rows = session.execute(query)
# print(f"Tables in keyspace '{cas_keyspace}':")
# for row in rows:
#     print(row.table_name)

# %%
# import uuid
# def insert_to_cas(df: pd.DataFrame , table_name: str, keyspace_name: str):
# # Prompt: How can I add a timeuuid 'id' column when inserting into Cassandra with PySpark
#     def generate_timeuuid():
#         return str(uuid.uuid1())

#     timeuuid_udf = udf(generate_timeuuid, StringType())
#     spark.createDataFrame(df).withColumn("id", timeuuid_udf())\
#         .write.format("org.apache.spark.sql.cassandra")\
#         .mode("append")\
#         .options(table=table_name, keyspace=keyspace_name)\
#         .save()

# insert_to_cas(df_production, "production", cas_keyspace)
# insert_to_cas(df_consumption, "consumption", cas_keyspace)
# insert_to_cas(df_prodcons, "prodcons", cas_keyspace)

# %% [markdown]
# ## Plotting

# %% [markdown]
# ### Retrieve data from ...

# %%
cas_tables = ["production", "consumption", "prodcons"]

# %% [markdown]
# ##### Cassandra using PySpark


# %%
def retrieve_from_cas(table, keyspace=cas_keyspace):
    df_spark = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load()
    )
    return df_spark.toPandas()


# %%
df_prod = retrieve_from_cas(cas_tables[0])
df_cons = retrieve_from_cas(cas_tables[1])
df_prodcons = retrieve_from_cas(cas_tables[2])

# %%
df_prod.head()

# %%
df_cons.head()

# %%
df_prodcons.head()

# %% [markdown]
# ##### MongoDB

# %%
df_gas = pd.DataFrame(list(mdb["gas"].find()))

# %%
df_gas.head()

# %% [markdown]
# ### Import/export between Norway and Denmark

# %%
df_prodcons = df_prodcons.sort_values(by="hourdk", ascending=True)
df_exim = df_prodcons.groupby("hourdk").aggregate({"exchangeno_mwh": "sum"})

# %%
import matplotlib.pyplot as plt

plt.style.use("ggplot")

fig_exim, ax_exim = plt.subplots(figsize=(7, 5), dpi=100)
ax_exim.plot(df_exim.index, df_exim.exchangeno_mwh, label="Net export/import")
ax_exim.legend()
ax_exim.set_title("Net export/import of electricity, Denmark - Norway")
plt.show()

# %% [markdown]
# Negative values indicate export, while positive values show import of electricity.

# %% [markdown]
# ### Gas sale and purchase prices

# %%
df_gas = df_gas.sort_values("gasday", ascending=True)

# %%
fig_gas, ax_gas = plt.subplots(figsize=(7, 5), dpi=100)
ax_gas.plot(
    df_gas.gasday,
    df_gas.purchasepricedkk_kwh,
    label="Sales price per kWh (DKK)",
)
ax_gas.plot(
    df_gas.gasday,
    df_gas.neutralgaspricedkk_kwh,
    label="Purchase price per kwh (DKK)",
    linestyle="--",
)
ax_gas.tick_params(axis="x", rotation=30)
ax_gas.set_title("Gas sale and purchase prices October 2022 - December 2022")
ax_gas.legend()
plt.show()

# %% [markdown]
# ### Cassandra table plots

# %%
import numpy as np

df_mun = pd.DataFrame(list(mdb["municipalities"].find()))
random_no = np.random.choice(df_prod.municipalityno.unique())
np.random.seed(0)

# %% [markdown]
# #### Production plot

# %%
prod_columns = [
    "offshorewindge100mw_mwh",
    "offshorewindlt100mw_mwh",
    "onshorewindmwh",
    "solarmwh",
    "thermalpowermwh",
]
rand_prod_type = np.random.choice(prod_columns)
mun_name = df_mun[df_mun.lau1_code == random_no].municipality.values[0]

df_prod = df_prod.sort_values("hourdk", ascending=True)
df_prod_ = df_prod[df_prod.municipalityno == random_no]

fig_prod, ax_prod = plt.subplots(figsize=(7, 5), dpi=100)
ax_prod.plot(df_prod_["hourdk"], df_prod_[rand_prod_type])
ax_prod.set_title(rand_prod_type + " in " + mun_name)

plt.show()

# %% [markdown]
# #### Consumption plot

# %%
rand_branche = np.random.choice(df_cons.branche.unique())
rand_muni_no = np.random.choice(df_cons.municipalityno.unique())
mun_name = df_mun[df_mun.lau1_code == rand_muni_no].municipality.values[0]

df_cons = df_cons.sort_values("hourdk", ascending=True)
df_cons_ = df_cons[df_cons.municipalityno == rand_muni_no]

fig_cons, ax_cons = plt.subplots(figsize=(7, 5), dpi=100)
ax_cons.plot(df_cons_["hourdk"], df_cons_["consumptionkwh"])
ax_cons.set_title(f"'{rand_branche}' consumption in '{mun_name}'")

plt.show()

# %% [markdown]
# #### Production/consumption plot

# %%
prodcons_cols = [
    "centralpowermwh",
    "commercialpowermwh",
    "hydropowermwh",
    "localpowermwh",
    "localpowerselfconmwh",
    "offshorewindge100mw_mwh",
    "offshorewindlt100mw_mwh",
    "onshorewindge50kw_mwh",
    "onshorewindlt50kw_mwh",
    "powertoheatmwh",
    "solarpowerge10lt40kw_mwh",
    "solarpowerge40kw_mwh",
    "solarpowerlt10kw_mwh",
    "solarpowerselfconmwh",
    "unknownprodmwh",
]

# %%
rand_prodcons_col = np.random.choice(prodcons_cols)
rand_price_area = np.random.choice(df_prodcons.pricearea.unique())

df_prodcons_ = df_prodcons.sort_values("hourdk", ascending=True)
df_prodcons_ = df_prodcons_[df_prodcons_["pricearea"] == rand_price_area]

fig_prodcons, ax_prodcons = plt.subplots(figsize=(7, 5), dpi=100)
ax_prodcons.plot(df_prodcons_["hourdk"], df_prodcons_[rand_prodcons_col])
ax_prodcons.set_title(f"'{rand_prodcons_col}' in '{mun_name}'")
plt.show()

# %%
