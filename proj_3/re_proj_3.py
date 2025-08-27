# %%
import sys
import os

sys.path.append(os.path.abspath(".."))

import pandas as pd
from helper.utils import *

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
# ## Pivoting

# %%
# def retrieve_from_cas(table, keyspace=cas_keyspace):
#     df_spark = spark.read.format("org.apache.spark.sql.cassandra") \
#         .options(table=table, keyspace=keyspace).load()
#     return df_spark.toPandas()

# %%
df_consumption = retrieve_from_cas(spark, "consumption", cas_keyspace)
df_consumption.head()

# %%
df_consumption = df_consumption.sort_values("hourdk", ascending=False)


# %%
def group_pivot_df(df, branche=True, agg_method="sum"):
    group_df = (
        df.groupby("branche") if branche else df.groupby("municipalityno")
    )
    return group_df.agg({"consumptionkwh": agg_method})


# %%
df_mun = pd.DataFrame(list(mdb_client[cas_keyspace]["municipalities"].find()))
df_mun.head()

# %%
import matplotlib.pyplot as plt


def plot_group_pivot(df, branche=True, agg_method="sum"):
    grouped_df = group_pivot_df(df, branche=branche, agg_method=agg_method)
    grouped_df = pd.merge(
        grouped_df,
        df_mun[["lau1_code", "municipality"]],
        left_on="municipalityno",
        right_on="lau1_code",
    )

    fig, ax = plt.subplots(figsize=(10, 5), dpi=200)
    ax.bar(
        grouped_df["municipality"],
        grouped_df["consumptionkwh"],
        label="Consumption kWh",
    )
    title_str = "industry group" if branche else "municipality"
    ax.set_title(f"{agg_method.capitalize()} consumption in every {title_str}")
    ax.tick_params("x", rotation=90)
    ax.tick_params(axis="x", labelsize=5)
    plt.show()

    return grouped_df


# %%
plot_group_pivot(df_consumption, branche=False, agg_method="max")

# %%
import plotly.express as px


def grouped_barplot(df: pd.DataFrame, branche=True):
    grouped_df = pd.merge(
        df,
        df_mun[["lau1_code", "municipality"]],
        left_on="municipalityno",
        right_on="lau1_code",
    )
    main_group = "branche" if branche else "municipality"
    sub_group = "municipality" if branche else "branche"
    fig = px.histogram(
        grouped_df,
        x=main_group,
        y="consumptionkwh",
        color=sub_group,
        barmode="group",
    )
    fig.show(renderer="notebook_connected")


# %%
grouped_barplot(df_consumption, branche=False)

# %%
grouped_barplot(df_consumption, branche=True)

# %% [markdown]
# ## Correlation

# %%
from helper.project_helper import sync_weather_gas

df_gas = pd.DataFrame(list(mdb_client[cas_keyspace]["gas"].find()))
df_weather = retrieve_from_cas(spark, "weather", cas_keyspace)

# %%
df_gas.head()

# %%
df_weather.head()

# %%
df_gas_prices = df_gas[["gasday", "purchasepricedkk_kwh"]].sort_values(
    "gasday", ascending=True
)

df_wspd = df_weather[["time_dk", "wspd"]].copy()
df_wspd.set_index("time_dk", inplace=True)
df_wspd = df_wspd[["wspd"]].resample("D").mean()
df_wspd.reset_index(inplace=True)
df_wspd = df_wspd[
    (df_wspd["time_dk"] >= min(df_gas_prices["gasday"]))
    & (df_wspd["time_dk"] <= max(df_gas_prices["gasday"]))
]
df_wspd.reset_index(drop=True, inplace=True)
df_wspd

# %%
import numpy as np
from scipy.fft import dct, idct


def dct_swc(
    ts_prices: pd.Series,
    ts_avg_wspd: pd.Series,
    swc_width=7,
    lag=0,
    dct_cutoff=0,
    dct_filter=False,
):
    # The function assumes that you have calculated the average wind speed
    # for each day of the period the gas prices are defined.

    # Lagging back the wind speed series and filling using back fill
    if lag > 0:
        ts_avg_wspd = ts_avg_wspd.shift(lag).bfill()

    # DCT
    if dct_filter:
        w = np.arange(0, len(ts_prices))

        # Perform DCT on the time series
        dct_prices = dct(ts_prices)
        dct_avg_wspd = dct(ts_avg_wspd)

        # Filter the DCT coefficients
        dct_prices[(w > dct_cutoff)] = 0
        dct_avg_wspd[(w > dct_cutoff)] = 0

        # Convert the filtered DCT to get back to the time domain
        ts_prices = pd.Series(idct(dct_prices))
        ts_avg_wspd = pd.Series(idct(dct_avg_wspd))

    # SWC
    # Calculating the sliding window correlation
    price_avg_wspd_swc = ts_prices.rolling(swc_width, center=True).corr(
        ts_avg_wspd
    )

    return price_avg_wspd_swc, ts_avg_wspd


# %%
lag = 7
swc_width = 7

price_wspd_swc, lag_avg_wspd = dct_swc(
    df_gas_prices["purchasepricedkk_kwh"],
    df_wspd["wspd"],
    swc_width=swc_width,
    lag=lag,
    dct_cutoff=15,
    dct_filter=True,
)

fig, ax = plt.subplots(2, 1, figsize=(10, 7))

# Top plot, purchase price and (lagged) wind speeds

# First curve, top plot
ax[0].plot(
    df_wspd["time_dk"], df_gas_prices["purchasepricedkk_kwh"], color="blue"
)
ax[0].set_ylabel("Purchase price (DKK)", color="blue")
ax[0].tick_params(axis="y", labelcolor="blue")

# Second curve, top plot
twin_ax0 = ax[0].twinx()
twin_ax0.plot(df_wspd["time_dk"], lag_avg_wspd)
twin_ax0.set_ylabel("(Lagged) avg. wind speed", color="red")
twin_ax0.tick_params(axis="y", labelcolor="red")

# Bottom plot, sliding window correlation
ax[1].plot(
    df_wspd["time_dk"],
    df_gas_prices["purchasepricedkk_kwh"],
    color="blue",
    alpha=0,
)  # Plotting an invsible figure to align the x-axis with the top plot
ax[1].plot(df_wspd["time_dk"], price_wspd_swc, color="blue")
ax[1].set_title(
    f"Sliding window correlation (window = {swc_width}, lag = {lag})"
)
ax[1].set_ylabel("SWC")
ax[1].set_xlabel("Date")


plt.show()

# %%
