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
# ## DB setups

# %% [markdown]
# ### Cassandra

# %%
from cassandra.cluster import Cluster

cluster = Cluster(["localhost"], port=9042)
session = cluster.connect()
# session.execute(f"DROP KEYSPACE IF EXISTS {cas_keyspace}")
session.execute(
    "CREATE KEYSPACE IF NOT EXISTS"
    + " "
    + cas_keyspace
    + " "
    + "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
)
session.set_keyspace(cas_keyspace)

# %% [markdown]
# ### MongoDB

# %%
new_collections = ["gas", "municipalities"]

for name in new_collections:
    try:
        mdb_client[cas_keyspace].create_collection(name)
        print(f"Collection '{name}' was created successfully.")
    except:
        print(f"Collcetion '{name}' already exists.")

# %% [markdown]
# ## Webscraping

# %% [markdown]
# ### Municipalities of Denmark

# %%
wiki_url = "https://en.wikipedia.org/wiki/List_of_municipalities_of_Denmark"
page_request = requests.get(wiki_url)
page_parsed = BeautifulSoup(page_request.content, "html.parser")
wiki_table = page_parsed.find("table", attrs={"class": "wikitable sortable"})

# %%
from io import StringIO

df_table = pd.read_html(StringIO(str(wiki_table)))[0]
df_table.head()

# %%
df_table = df_table.rename(
    columns={
        "LAU-1 code 1": "municipalityNo",
        "Municipality": "municipality",
        "Administrative Center": "adminCenter",
        "Total Area (km²)": "totalArea_km2",
        "Population (2012-01-01)": "population",
        "Region": "region",
    }
)
df_table.head()

# %% [markdown]
# ##### Insert table to MongoDB

# %%
mdb_client[cas_keyspace]["municipalities"].insert_many(
    df_table.to_dict("records")
)

# %% [markdown]
# ### Retrieve data from API

# %%
api_url = "https://api.energidataservice.dk/dataset/"
filtering = "?offset=0&start=2022-01-01T00:00&end=2023-01-01T00:00"
get_json = lambda dataset_url: requests.get(
    api_url + dataset_url + filtering
).json()["records"]

# %%
df_gas = pd.DataFrame.from_records(get_json("GasDailyBalancingPrice"))
df_production = pd.DataFrame.from_records(
    get_json("ProductionMunicipalityHour")
)
df_consumption = pd.DataFrame.from_records(get_json("ConsumptionIndustry"))
df_prodcons = pd.DataFrame.from_records(
    get_json("ProductionConsumptionSettlement")
)

# %%
df_gas = df_gas.rename(columns=str.lower)
df_production = df_production.rename(columns=str.lower)
df_consumption = df_consumption.rename(columns=str.lower)
df_prodcons = df_prodcons.rename(columns=str.lower)

# %%
df_gas.gasday = pd.to_datetime(df_gas.gasday)

df_production.hourutc = pd.to_datetime(df_production.hourutc)
df_production.hourdk = pd.to_datetime(df_production.hourdk)

df_consumption.hourutc = pd.to_datetime(df_consumption.hourutc)
df_consumption.hourdk = pd.to_datetime(df_consumption.hourdk)

df_prodcons.hourutc = pd.to_datetime(df_prodcons.hourutc)
df_prodcons.hourdk = pd.to_datetime(df_prodcons.hourdk)

# %%
for df in [df_production, df_consumption, df_prodcons]:
    print(df.dtypes)
    print()

# %%
df_production.municipalityno = df_production.municipalityno.astype(int)

df_consumption.municipalityno = df_consumption.municipalityno.astype(int)
df_consumption.branche = df_consumption.branche.astype(str)

df_prodcons.pricearea = df_prodcons.pricearea.astype(str)
df_prodcons.exchangegb_mwh = df_prodcons.exchangegb_mwh.astype(float)

# %% [markdown]
# ## Inserting to Cassandra


# %%
def create_cassandra_table_query(df, keyspace, table_name):
    # Define mapping between Pandas and Cassandra datatypes
    dtype_mapping = {
        "int64": "int",
        "float64": "double",
        "object": "text",
        "bool": "boolean",
        "datetime64[ns]": "timestamp",
    }

    # Start constructing the CREATE TABLE query
    query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (\n"

    # Add primary key column with timeuuid
    columns = ["id timeuuid"]

    # Add remaining columns with mapped Cassandra data types
    for col, dtype in df.dtypes.items():
        if col != "id":  # Exclude 'id' to avoid duplication
            cassandra_type = dtype_mapping.get(
                str(dtype), "text"
            )  # Default to 'text' if type is unrecognized
            columns.append(f"{col} {cassandra_type}")

    # Join columns with commas and specify primary key as 'id'
    columns_str = ",\n    ".join(columns)
    query += f"    {columns_str},\n"
    query += "    PRIMARY KEY (id)\n);"

    return query


production_query = create_cassandra_table_query(
    df_production, cas_keyspace, "production"
)
consumption_query = create_cassandra_table_query(
    df_consumption, cas_keyspace, "consumption"
)
prodcons_query = create_cassandra_table_query(
    df_prodcons, cas_keyspace, "prodcons"
)

# for query in [production_query, consumption_query, prodcons_query]:
#     session.execute(query)

# %%
# session.execute("DROP TABLE IF EXISTS rewrite320.production")
# session.execute("DROP TABLE IF EXISTS rewrite320.consumption")
# session.execute("DROP TABLE IF EXISTS rewrite320.prodcons")

# %%
import uuid


def insert_to_cas(
    df: pd.DataFrame, table_name: str, keyspace_name: str, insert=True
):
    def generate_timeuuid():
        return str(uuid.uuid1())

    mode = "append" if insert else "overwrite"

    timeuuid_udf = udf(generate_timeuuid, StringType())
    spark.createDataFrame(df).withColumn("id", timeuuid_udf()).write.format(
        "org.apache.spark.sql.cassandra"
    ).mode(mode).options(table=table_name, keyspace=keyspace_name).save()


# insert_to_cas(df_production, "production", cas_keyspace)
# insert_to_cas(df_consumption, "consumption", cas_keyspace)
# insert_to_cas(df_prodcons, "prodcons", cas_keyspace)

# %%
mdb_client[cas_keyspace]["gas"].insert_many(df_gas.to_dict("records"))

# %% [markdown]
# ## Plotting

# %%
from helper.utils import retrieve_from_cas

df_prod = retrieve_from_cas(spark, "production", cas_keyspace)
df_cons = retrieve_from_cas(spark, "consumption", cas_keyspace)
df_prodcons = retrieve_from_cas(spark, "prodcons", cas_keyspace)

df_gas = pd.DataFrame(list(mdb_client[cas_keyspace]["gas"].find()))
df_mun = pd.DataFrame(list(mdb_client[cas_keyspace]["municipalities"].find()))

# %% [markdown]
# ### Import/export of electricity from/to Norway

# %%
df_prodcons = df_prodcons.sort_values("hourutc")
df_prodcons_ie = df_prodcons.groupby("hourdk").aggregate(
    {"exchangeno_mwh": "sum"}
)
df_prodcons_ie.head()

# %%
import matplotlib.pyplot as plt

fig_ie, ax_ie = plt.subplots(figsize=(8, 5), dpi=100)
ax_ie.plot(
    df_prodcons_ie.index,
    df_prodcons_ie.exchangeno_mwh,
    label="Net import/export",
)
ax_ie.set_title("Net import/export of electricity, Denmark – Norway")
plt.show()

# %% [markdown]
# ### Gas sale and purchas prices

# %%
df_gas = df_gas.sort_values("gasday")
df_gas.head()

# %%
fig_gas, ax_gas = plt.subplots(figsize=(8, 5), dpi=100)
ax_gas.plot(
    df_gas.gasday, df_gas.purchasepricedkk_kwh, label="purchase price in DKK"
)
ax_gas.plot(
    df_gas.gasday,
    df_gas.salespricedkk_kwh,
    "--",
    label="sales price in DKK",
)
ax_gas.set_title("Gas sale/purchase prices of electricity")
ax_gas.legend()
plt.show()

# %% [markdown]
# ### Random choice of municipality and column in 'production', 'consumption', and 'prodcons'

# %% [markdown]
# ##### production

# %%
df_prod.head()

# %%
import random

random.seed(591301)
unique_mun_no = df_prod.municipalityno.unique()
prod_types_prod = [
    "offshorewindge100mw_mwh",
    "offshorewindlt100mw_mwh",
    "onshorewindmwh",
    "solarmwh",
    "thermalpowermwh",
]

rand_mun_no_prod = random.choice(unique_mun_no)
rand_prod_type = random.choice(prod_types_prod)

df_prod_r = df_prod[df_prod.municipalityno == rand_mun_no_prod].sort_values(
    "hourutc"
)
fig_prod_r, ax_prod_r = plt.subplots(figsize=(8, 5), dpi=100)
ax_prod_r.plot(df_prod_r.hourutc, df_prod_r[rand_prod_type])
ax_prod_r.set_title(
    f"{rand_prod_type} production in municipality no. {rand_mun_no_prod}"
)
plt.show()

# %% [markdown]
# ##### consumption

# %%
unique_mun_no_cons = df_cons.municipalityno.unique()
branches = df_cons.branche.unique()

rand_mun_no_cons = random.choice(unique_mun_no_cons)
rand_branche = random.choice(branches)

df_cons_r = df_cons[df_cons.municipalityno == rand_mun_no_cons].sort_values(
    "hourutc"
)
fig_cons_r, ax_cons_r = plt.subplots(figsize=(8, 5), dpi=100)
ax_cons_r.plot(df_cons_r.hourutc, df_cons_r.consumptionkwh)
ax_cons_r.set_title(
    f"Consumption for branche '{rand_branche}' in municipality no. {rand_mun_no_cons}"
)
plt.show()

# %% [markdown]
# ##### prodcons

# %%
price_area = df_prodcons.pricearea.unique()
prod_types_prodcons = [
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

rand_price_area = random.choice(price_area)
rand_prod_type_prodcons = random.choice(prod_types_prodcons)

df_prodcons_r = df_prodcons[
    df_prodcons.pricearea == rand_price_area
].sort_values("hourutc")
fig_prodcons_r, ax_prodcons_r = plt.subplots(figsize=(8, 5), dpi=100)
ax_prodcons_r.plot(
    df_prodcons_r.hourutc, df_prodcons_r[rand_prod_type_prodcons]
)
ax_prodcons_r.set_title(
    f"{rand_prod_type_prodcons} production in price area {rand_price_area}"
)
plt.show()
