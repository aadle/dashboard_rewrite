import streamlit as st
import pandas as pd
import plotly.express as px


def plot_group_pivot(df:pd.DataFrame, branche=True, agg_method="sum"):
    title_str = "industry group" if branche else "municipality"

    grouped_df = df.groupby("branche") if branche else\
        df.groupby("municipality")
    grouped_df = grouped_df.agg({"consumptionkwh": agg_method})

    fig = px.bar(grouped_df, x=grouped_df.index, y="consumptionkwh", 
                 title=f"{agg_method.capitalize()} of consumption in every {title_str}"
                 )

    if not branche:
        fig.update_xaxes(tickfont=dict(size=8),
                     dtick=1)
    else:
        fig.update_xaxes(tickfont=dict(size=12),
                     dtick=1)

    return fig

def grouped_barplot(df:pd.DataFrame, branche=True):
    main_group = "branche" if branche else "municipality"
    sub_group = "branche" if not branche else "municipality"

    fig = px.histogram(
        df, x=main_group, y="consumptionkwh", color=sub_group, 
        barmode="group", title=f"Consumption grouped by {main_group.capitalize()}"
    )

    if not branche:
        fig.update_xaxes(tickfont=dict(size=6),
                     dtick=1)
    else:
        fig.update_xaxes(tickfont=dict(size=12),
                     dtick=1)
    fig.update_traces(hovertemplate=None, hoverinfo="skip")  # Disable hover
    fig.update_layout(dragmode=False) 

    return fig


st.markdown("# Summaries 📊")
st.sidebar.markdown("# Summaries 📊")

df_cons = st.session_state["consumption"]

branche_pivot = st.selectbox(label="Group by industry group?", 
                              options=[True, False], index=1)

st.plotly_chart(plot_group_pivot(df_cons, branche=branche_pivot))

st.plotly_chart(grouped_barplot(df_cons, branche=branche_pivot))


# st.write(df_cons.head())
# st.write(q.head())
