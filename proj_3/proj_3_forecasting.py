# %%
import sys
import os

sys.path.append(os.path.abspath(".."))

import pandas as pd
from helper.utils import *

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

mdb_client = mongodb_setup()
spark = spark_setup()
cas_keyspace = "rewrite320"


# %%
def nan_overview(df):
    df_raw = df.isna().sum()[df.isna().sum() > 0]
    df_pct = df.isna().sum()[df.isna().sum() > 0] / df.shape[0]
    final_df = pd.concat([df_raw, df_pct], axis=1)
    final_df.columns = ["num", "pct"]
    final_df = final_df.sort_values(by="num", ascending=False)
    # return df.isna().sum()[ df.isna().sum() > 0 ] / df.shape[0]
    return final_df


# %%
df_mun = pd.DataFrame(list(mdb_client[cas_keyspace]["municipalities"].find()))
df_mun.head()

# %%
df_prod = retrieve_from_cas(spark, "production", cas_keyspace)
df_prod = df_prod.sort_values("hourutc", ascending=True)
df_prod.head()

# %%
df_prod = pd.merge(
    df_prod,
    df_mun[["municipalityNo", "municipality"]],
    left_on="municipalityno",
    right_on="municipalityNo",
)
df_prod.drop(columns=["municipalityNo"], axis=1)
df_prod.head()

# %%
df_weather = retrieve_from_cas(spark, "weather", cas_keyspace)
df_weather[
    [
        "coco",
        "dwpt",
        "prcp",
        "pres",
        "rhum",
        "snow",
        "temp",
        "tsun",
        "wdir",
        "wpgt",
        "wspd",
    ]
] = df_weather[
    [
        "coco",
        "dwpt",
        "prcp",
        "pres",
        "rhum",
        "snow",
        "temp",
        "tsun",
        "wdir",
        "wpgt",
        "wspd",
    ]
].astype(float)
df_weather = df_weather.sort_values("time")

# %%
nan_overview(df_weather)

# %% [markdown]
# ## Testing SARIMAX modeling

# %%
# def time_to_utc(df, time_column):
#     """
#     A function in order to set a proper frequency for the date index.
#     """
#     try:
#         # If there is a duplicate time when subtracting an hour
#         df.index = pd.DatetimeIndex(df[time_column], tz="Europe/Copenhagen", ambiguous="infer")
#     except:
#         # ... duplicate time when adding an hour
#         df.index = pd.DatetimeIndex(df[time_column], tz="Europe/Copenhagen", ambiguous=True)
#     df.index = df.index.tz_convert("UTC")
#     df = df[~df.index.duplicated(keep='first')]
#     df = df.asfreq("h", method="ffill")
#     return df

# %% [markdown]
# ### Data setup
#
# Both the production and weather data have the time tracked in UTC, so by setting the index to the time column is enough.

# %%
mun = "Morsø"
df_cph_w = df_weather[df_weather.municipality == mun]
df_cph_w.index = pd.DatetimeIndex(df_cph_w["time"])
df_cph_w.head()

# %%
df_cph_p = df_prod[df_prod.municipality == mun]
df_cph_p.index = pd.DatetimeIndex(df_cph_p["hourutc"])
df_cph_p.head()

# %% [markdown]
# ### Modeling

# %%
import statsmodels.api as sm

df_endog = df_cph_p.solarmwh
# df_exog = df_cph_w[["temp", "wspd"]]
df_exog = df_cph_w["temp"]

mod = sm.tsa.statespace.SARIMAX(
    endog=df_endog,
    exog=df_exog,
    trend="c",
    order=(2, 1, 2),
    seasonal_order=(2, 0, 2, 24),
)
fit_mod = mod.fit(disp=False)  # Fit the model
res = mod.filter(fit_mod.params)

# In-sample one-step-ahead prediction
predict_step = res.get_prediction()


# %%
# Dynamic forecasting
# "dynamic=cutoff/dt_cutoff" does not work,
# setting both a start= and dynamic=True, enables dynamic forecasting!
cutoff = "2022-07-31"
dt_cutoff = pd.to_datetime(cutoff)
predict_dy = res.get_prediction(start=dt_cutoff, dynamic=True)

ci_step = predict_step.conf_int().loc[cutoff:]
ci_dy = predict_dy.conf_int().loc[cutoff:]

display(fit_mod.summary())  # Show the model summary

# %% [markdown]
# ### Plot of the resulting forecast

# %%
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(8, 5), dpi=200)
# ax.plot(df_endog.index, df_endog.values, marker="o", markersize=5, linewidth=1)
ax.plot(
    predict_dy.predicted_mean.index,
    predict_dy.predicted_mean.values,
    color="blue",
    linewidth=0.5,
    label="Dynamic forecasting",
)
# ax.fill_between(ci_dy.index, ci_dy.iloc[:, 0], ci_dy.iloc[:, 1],
#                     color="b", alpha=0.2)
ax.plot(
    predict_step.predicted_mean.loc[cutoff:],
    color="green",
    linewidth=0.5,
    label="One-step forecasting",
)
ax.fill_between(
    ci_step.loc[cutoff:].index,
    ci_step.loc[cutoff:].iloc[:, 0],
    ci_step.loc[cutoff:].iloc[:, 1],
    color="green",
    alpha=0.4,
)
ax.scatter(
    df_endog.loc[cutoff:].index,
    df_endog.loc[cutoff:].values,
    s=5,
    label="Underlying observations",
)
ax.legend()
plt.show()

# %% [markdown]
# ## SARIMAX modeling function


# %%
def model_sarimax(
    df_endogenous,
    df_exogenous,
    prod_type: str = "solarmwh",
    municipality: str = "Copenhagen",
    cutoff: str = "2022-07-31",
    wspd=True,
    temp=True,
):
    cutoff = pd.to_datetime(cutoff)

    # Choice of endogenous variable
    endog = df_endogenous[df_endogenous.municipality == municipality]
    endog = endog[prod_type]

    # Inclusion of exogenous variable(s)
    exog_vars = {"temp": temp, "wspd": wspd}
    selected_vars = [var for var, include in exog_vars.items() if include]
    exog = df_exogenous[df_exogenous.municipality == municipality]
    exog = exog[selected_vars] if selected_vars else None

    # SARIMAX modeling
    model = sm.tsa.statespace.SARIMAX(
        endog=endog,
        exog=exog,
        trend="c",
        order=(2, 1, 2),
        seasonal_order=(2, 0, 2, 24),
    )
    fit_model = model.fit(disp=False)  # Fit the model
    results = model.filter(fit_model.params)

    # One-step and dynamic predictions
    one_step_pred = results.get_prediction()
    dynamic_pred = results.get_prediction(start=dt_cutoff, dynamic=True)
    predictions = {"one step": one_step_pred, "dynamic": dynamic_pred}

    return fit_model, predictions


# %%
df_prod_c = df_prod.copy()
df_weather_c = df_weather.copy()
df_prod_c.index = pd.DatetimeIndex(df_prod_c["hourutc"])
df_weather_c.index = pd.DatetimeIndex(df_weather_c["time"])
cutoff = "2022-07-31"
mod_test, res_test = model_sarimax(
    df_endogenous=df_prod_c,
    df_exogenous=df_weather_c,
    municipality="Copenhagen",
    prod_type="solarmwh",
    cutoff=cutoff,
)
display(mod_test.summary())


# %%
def plot_forecast(
    one_step_pred, dynamic_pred, obs_data, cutoff="2022-07-31", ci=True
):
    cutoff = pd.to_datetime(cutoff)
    plot_start = cutoff - pd.DateOffset(weeks=1)
    plot_end = cutoff + pd.DateOffset(months=1)
    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)

    # Dynamic
    dynamic_idx = dynamic_pred.predicted_mean.index
    dynamic_val = dynamic_pred.predicted_mean.values
    dynamic_ci = dynamic_pred.conf_int()

    ax.plot(
        dynamic_idx,
        dynamic_val,
        color="blue",
        linewidth=0.75,
        label="Dynamic Forecast",
        #     marker="o", markersize=2
    )
    if ci:
        ax.fill_between(
            dynamic_idx,
            dynamic_ci.iloc[:, 0],
            dynamic_ci.iloc[:, 1],
            color="b",
            alpha=0.2,
        )

    # One step
    one_step_idx = one_step_pred.predicted_mean.index
    one_step_val = one_step_pred.predicted_mean.values
    one_step_ci = one_step_pred.conf_int()

    ax.plot(
        one_step_idx,
        one_step_val,
        color="green",
        linewidth=0.75,
        label="One-step Forecast",
        #     marker="o", markersize=2
    )
    if ci:
        ax.fill_between(
            one_step_idx,
            one_step_ci.iloc[:, 0],
            one_step_ci.iloc[:, 1],
            color="green",
            alpha=0.2,
        )

    # Actual data
    ax.plot(
        obs_data.index,
        obs_data.values,
        linewidth=0.75,
        label="Underlying observations",
        # marker="o", markersize=2
    )

    ax.legend()
    ax.set_xlim(plot_start, plot_end)
    ax.tick_params(axis="x", rotation=45)
    if ci:
        ax.set_ylim(-50, 50)
    plt.show()

    return ax, fig


# %%
dy = res_test["dynamic"]
one = res_test["one step"]
plot_forecast(one, dy, df_cph_p.solarmwh, ci=True)
plt.show()

# %%
