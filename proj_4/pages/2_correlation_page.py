import streamlit as st
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.fft import dct, idct

def dct_swc(
    ts_prices,
    ts_avg_wspd,
    swc_width=7,
    lag=0,
    dct_cutoff=0,
    dct_filter=False
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

# Data setup
df_gas = st.session_state["gas"][["gasday", "purchasepricedkk_kwh"]]
df_gas = df_gas.set_index("gasday")

df_wspd = st.session_state["weather"][["hourutc", "wspd"]].set_index("hourutc")
df_wspd = df_wspd[["wspd"]].resample("D").mean()
df_wspd = df_wspd[
    (df_wspd.index >= min(df_gas.index)) &\
    (df_wspd.index <= max(df_gas.index)) 
]

# Page elements
st.markdown("# Correlation 📈📉")
st.sidebar.markdown("# Correlation 📈📉")

col_controls = st.columns(4)

with col_controls[0]:
    dct_filter = st.selectbox(label="Turn on DCT filtering", 
                              options=[True, False], index=1)
with col_controls[1]:
    swc_width = st.number_input(
        "Width of sliding window", min_value=0, max_value=14, value=7)
with col_controls[2]:
    lag_ts = st.number_input("Lag of time series", min_value=0, max_value=14,
                         value=0)
with col_controls[3]:
    dct_cutoff = st.number_input("DCT cutoff", min_value=0, max_value=75, value=20) if dct_filter else 0

# st.write(df_wspd.head(10))
# st.write(df_gas.head(10))

swc_purchase_wspd, dct_avg_wspd = dct_swc(df_gas["purchasepricedkk_kwh"], 
                                          df_wspd["wspd"], 
                                          swc_width=swc_width,
                                          lag=lag_ts,
                                          dct_cutoff=dct_cutoff,
                                          dct_filter=dct_filter
                                          )

fig = make_subplots(
    rows=2, cols=1,
    specs=[[{"secondary_y": True}],
           [{"secondary_y": False}]],
    subplot_titles=('Purchase price, average wind speed', 
                    'Sliding Window Correlation between purchase price and wind speed'
                    )
)

fig.add_trace(
    go.Scatter(
        x=df_gas.index,
        y=df_gas.purchasepricedkk_kwh,
        yaxis="y1",
        line=dict(color="red")
    ),
    row=1, col=1,
    secondary_y=True,
)

fig.add_trace(
    go.Scatter(
        x=df_gas.index,
        y=dct_avg_wspd.values,
        yaxis="y2",
        line=dict(color="blue")
    ),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(
        x=df_gas.index,
        y=swc_purchase_wspd.values,
        line=dict(color="green")
    ),
    row=2, col=1
)

fig.update_yaxes(
    title_text="Average Wind Speed (m/s)",
    title_font=dict(color="#1f77b4"),
    tickfont=dict(color="#1f77b4"),
    row=1, col=1, 
    secondary_y=False
)

fig.update_yaxes(
    title_text="Purchase Price (DKK/kWh)",
    title_font=dict(color="#d62728"),
    tickfont=dict(color="#d62728"),
    row=1, col=1, 
    secondary_y=True
)

fig.update_layout(showlegend=False)

st.plotly_chart(fig)
