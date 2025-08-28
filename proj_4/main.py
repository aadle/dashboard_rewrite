import sys
import os
sys.path.append(os.path.abspath(".."))

import streamlit as st
import pandas as pd
# from helper.utils import mongodb_setup, spark_setup, retrieve_from_cas
from helper.utils import mongodb_setup, spark_setup, retrieve_from_cas

# ------ DB setup ------
mdb_client = mongodb_setup()
spark = spark_setup()

# --- Load in the data ---
mongodb_tables = ["municipalities", "gas"]
for table in mongodb_tables:
    if table not in st.session_state:
        st.session_state[table] = pd.DataFrame(
                list(mdb_client["rewrite320"][table].find())
            )
        print(f"{table} is now in the session state.")

st.session_state["municipalities"] = st.session_state["municipalities"].sort_values("municipality")

cas_tables = ["production", "prodcons", "consumption"]
for table in cas_tables:
    if table not in st.session_state:
        st.session_state[table] = retrieve_from_cas(
            spark, table, "rewrite320"
        ).sort_values("hourutc")
    print(f"{table} is now in the session state.")

if "weather" not in st.session_state:
    df_weather = retrieve_from_cas(
        spark, "weather", "rewrite320"
    ).sort_values("time")

    df_weather = df_weather.rename(columns={"time": "hourutc"})
    df_weather[
    [
        "coco", "dwpt", "prcp", "pres", "rhum", "snow", "temp", "tsun", "wdir",
        "wpgt","wspd",
    ]
] = df_weather[
    [
        "coco", "dwpt", "prcp", "pres", "rhum", "snow", "temp", "tsun", "wdir",
        "wpgt","wspd",
    ]
].astype(float)
    st.session_state["weather"] = df_weather
    print("Weather is now in the session state.")


# ------ Page setup ------
main_page = st.Page("pages/1_dashboard.py", title="Dashboard")
correlation_page = st.Page("pages/2_correlation_page.py", title="Correlation")
summaries_page = st.Page("pages/3_summaries_page.py", title="Summaries")
forecasting_page = st.Page("pages/4_forecasting_page.py", title="Forecasting")

pages = [main_page, summaries_page, correlation_page, forecasting_page]
pg = st.navigation(pages)

pg.run()

st.set_page_config(layout="wide")


