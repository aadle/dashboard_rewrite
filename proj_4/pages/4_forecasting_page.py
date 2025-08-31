import streamlit as st
import pandas as pd
import statsmodels.api as sm
# import matplotlib.pyplot as plt
import plotly.graph_objects as go

def model_sarimax(df_endogenous, 
                   df_exogenous, 
                   prod_type:str="solarmwh", 
                   municipality:str="Copenhagen",
                   cutoff=pd.to_datetime("2022-07-31"),
                   wspd=True, 
                   temp=True):

    # cutoff = pd.to_datetime(cutoff)

    # Choice of endogenous variable
    endog = df_endogenous[df_endogenous.municipality == municipality]
    endog = endog[prod_type]

    # Inclusion of exogenous variable(s)
    exog_vars = {"temp": temp, "wspd": wspd}
    selected_vars = [var for var, include in exog_vars.items() if include]
    exog = df_exogenous[df_exogenous.municipality == municipality]
    exog = exog[selected_vars] if selected_vars else None
    
    # SARIMAX modeling
    model = sm.tsa.statespace.SARIMAX(endog=endog, exog=exog, trend="c",
                                order=(2, 1, 2), seasonal_order=(2, 0, 2, 24))
    fit_model = model.fit(disp=False) # Fit the model
    results = model.filter(fit_model.params)
    print("Modeling completed!")

    # One-step and dynamic predictions
    one_step_pred = results.get_prediction()
    dynamic_pred = results.get_prediction(start=cutoff, dynamic=True)
    predictions = {"one step": one_step_pred,
               "dynamic": dynamic_pred}

    return fit_model, predictions

def plot_forecast(one_step_pred, dynamic_pred, obs_data, 
                  cutoff=pd.to_datetime("2022-07-31"),):
    # cutoff = pd.to_datetime(cutoff) 
    plot_start = cutoff - pd.DateOffset(weeks=1)
    plot_end = cutoff + pd.DateOffset(months=2)
    
    fig = go.Figure()
    
    # Dynamic forecast
    dynamic_idx = dynamic_pred.predicted_mean.index
    dynamic_val = dynamic_pred.predicted_mean.values
    dynamic_ci = dynamic_pred.conf_int()

    # Add dynamic forecast line
    fig.add_trace(go.Scatter(
        x=dynamic_idx, 
        y=dynamic_val,
        mode='lines',
        name='Dynamic Forecast',
        line=dict(color='blue', width=1.5)
    ))
    
    # Add dynamic confidence interval
    fig.add_trace(go.Scatter(
        x=dynamic_idx,
        y=dynamic_ci.iloc[:, 1],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    fig.add_trace(go.Scatter(
        x=dynamic_idx,
        y=dynamic_ci.iloc[:, 0],
        mode='lines',
        line=dict(width=0),
        fill='tonexty',
        fillcolor='rgba(0, 0, 255, 0.2)',
        name='Dynamic CI',
        hoverinfo='skip'
    ))

    # One step forecast
    one_step_idx = one_step_pred.predicted_mean.index
    one_step_val = one_step_pred.predicted_mean.values
    one_step_ci = one_step_pred.conf_int()

    # Add one-step forecast line
    fig.add_trace(go.Scatter(
        x=one_step_idx, 
        y=one_step_val,
        mode='lines',
        name='One-step Forecast',
        line=dict(color='green', width=1.5)
    ))
    
    # Add one-step confidence interval
    fig.add_trace(go.Scatter(
        x=one_step_idx,
        y=one_step_ci.iloc[:, 1],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    fig.add_trace(go.Scatter(
        x=one_step_idx,
        y=one_step_ci.iloc[:, 0],
        mode='lines',
        line=dict(width=0),
        fill='tonexty',
        fillcolor='rgba(0, 128, 0, 0.2)',
        name='One-step CI',
        hoverinfo='skip'
    ))
    
    # Actual data
    fig.add_trace(go.Scatter(
        x=obs_data.index, 
        y=obs_data.values,
        mode='lines',
        name='Underlying observations',
        line=dict(width=1.5)
    ))

    # Update layout
    fig.update_layout(
        width=800,
        height=500,
        xaxis=dict(
            range=[plot_start, plot_end],
            tickangle=45
        ),
        showlegend=True,
        template='plotly_white'
    )

    y_min = obs_data.min() * 1.1 
    y_max = obs_data.max() *1.1

    fig.update_layout(yaxis=dict(range=[-100, y_max]))
    
    return fig

# Data setup
df_prod = st.session_state["production"]
df_prod.index = pd.DatetimeIndex(df_prod["hourutc"])
df_weather = st.session_state["weather"]
df_weather.index = pd.DatetimeIndex(df_weather["hourutc"])

# Page setup
st.markdown("# Forecasting 📈")
st.sidebar.markdown("# Forecasting 📈")

municipality_list = sorted(df_prod.municipality.unique().tolist())
exclude_cols = ["id", "hourdk", "hourutc", "municipality", "municipalityno"]
prod_types = [
    col for col in sorted(df_prod.columns.tolist()) if col not in exclude_cols
] 

cols = st.columns(3)

with cols[0]:
    municipality = st.selectbox("Municipality", options=municipality_list)
    df_weather = df_weather[df_weather.municipality == municipality]
with cols[1]:
    prod_type = st.selectbox("Production type", options=prod_types, index=3)
with cols[2]:
    incl_wspd = st.selectbox("Include wind speed as exogenous variable", 
                             options=[True, False], index=0
                             )

date_range = pd.date_range(start="2022-06-30", end="2022-12-01", freq="D")
cutoff = st.select_slider("Cutoff date", options=date_range)

model, results = model_sarimax(
    df_endogenous=df_prod,
    df_exogenous=df_weather,
    municipality=municipality,
    prod_type=prod_type,
    cutoff=cutoff
)

fig = plot_forecast(
    one_step_pred=results["one step"], 
    dynamic_pred=results["dynamic"],
    obs_data=df_prod[prod_type], 
    cutoff=cutoff
)


st.plotly_chart(fig)
st.write(model.summary())
