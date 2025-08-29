import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy.fft import dct, idct

def dct_smoothing(df:pd.DataFrame, column:str, max_val:int=10):

    df_values = df[column].values
    W = np.arange(0, df_values.shape[0])
    dct_values = dct(df_values)
    smoothed_signal = dct_values.copy()
    smoothed_signal[(W > max_val)] = 0

    return idct(smoothed_signal)

def line_plot(df:pd.DataFrame, column:str):
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df.hourutc,
            y=df[column].values,
            line={ "color":"grey"},
            fill="tonexty",
            # mode="none",
            opacity=0.5,
            name="Observed"
        ),
    )
    
    fig.add_trace(
        go.Scatter(
            x=df.hourutc,
            y=df["smoothed_signal"].values,
            line=dict(color="purple"),
            name="DCT-filtered",
        )
    )

    df_current_pos = df.iloc[-1, :]
    fig.add_trace(
        go.Scatter(x=[ df_current_pos.hourutc ], 
                   y=[ df_current_pos[column] ],
                   mode="markers", marker=dict(color="red", size=7),
                   name="Current time point"
                   ),
    )

    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        xanchor="right",
        y=1.02,
        x=1
    )
                      )
    return fig

def map_plot(df:pd.DataFrame, column:str, df_municipality:pd.DataFrame,
             selected_table:str):
    if selected_table == "Weather":
        marker_values = df["normalized_values"].values
        size = 15
    elif selected_table == "Consumption":
        marker_values = df["sqrt_values"].values
        size = 0.05
    else:
        marker_values = df["sqrt_values"].values
        size = 3

    fig = go.Figure()
    fig.add_trace(
        go.Scattermap(
            lat=df_municipality.lat.values,
            lon=df_municipality.lon.values,
            hovertext=df_municipality.municipality,
            customdata=df[column].values,
            hovertemplate="%{hovertext}<br>" +
                "(%{lat:.5f}, %{lon:.5f})<br>" + "%{customdata:.2f}",
            name=None,
            marker=dict(
                size=marker_values * size,
                sizemin=2,
            ),
            ),
        )
    fig.update_layout(
        map=dict(
            center=dict(
                lat=56.1865, 
                lon=11.5683
            ),
            zoom=5
        ),
        hoverlabel=dict(
            bgcolor="white",
            font_size=11,
        )
    )
    return fig

def power_map_plot(df:pd.DataFrame):
    df_cities = pd.DataFrame(
            [
            {"city": "Oslo", "lat": 59.9139, "lon": 10.7522, "country":"Norway"},
            {"city": "Stockholm", "lat": 59.3327, "lon": 18.0656,
             "country":"Sweden"},
            {"city": "Copenhagen", "lat": 55.6761, "lon": 12.5683,
             "country":"Denmark"},
            {"city": "Amsterdam", "lat": 52.3676, "lon": 4.9041,
             "country":"Netherlands"},
            {"city": "London", "lat": 51.5072, "lon": 0.1276, 
             "country":"Great Britain"},
            {"city": "Berlin", "lat": 52.5200, "lon": 13.4050,
             "country":"Germany"},
            ]
        )
    fig = go.Figure()
    fig.add_trace(
        go.Scattermap(
            lon=df_cities.lon,
            lat=df_cities.lat,
            marker=dict(
                size=14
            ),
        )
    )
    fig.update_layout(
    map=dict(
        center=dict(
            lat=55.1865, 
            lon=11.5683
        ),
        zoom=3.1
    ),
    hoverlabel=dict(
        bgcolor="white",
        font_size=11,
    )
)
    return fig

# Sidebar
st.sidebar.markdown("# Main page 🎈")

# Main page content
# data_sources = {"Production": "production",
#                 "Consumption": "consumption",
#                 "Prodcons": "prodcons",
#                 "Weather": "weather"}

data_sources = {
    "Production": {
        "session_state": "production",
        "dropdown_title": "Production Type",
        # "municipality": True,
    },
    "Consumption": {
        "session_state": "consumption",
        "dropdown_title": "Industry Group",
        # "municipality": True,
    },
    "Power import/export": {
        "session_state": "prodcons",
        "dropdown_title": None,
        # "municipality": False,
    },
    "Weather": {
        "session_state": "weather",
        "dropdown_title": "Weather Property",
        # "municipality": True,
    },
}

top_col_1, top_col_2 = st.columns(spec=[0.65, 0.35])
with top_col_1:
    st.markdown("# Main page 🎈")

with top_col_2:
    selected_table = st.selectbox(
        "Data source",
        options=["Weather", "Production", "Consumption", "Power import/export",],
        index=0,
    )

    if selected_table is not None:
        current_source = data_sources[selected_table]
        access_table = current_source["session_state"]
        df_table = st.session_state[access_table]
        print(f"The current data source is set to {selected_table}.")

    else:
        df_table = None
        print("No data source is currently set.")
        st.text("No data source is currently set.")


col_1, col_2 = st.columns(spec=[0.5, 0.5])

with col_1:
    # st.image(
    #     "https://freevector-images.s3.amazonaws.com/uploads/vector/preview/165256/vecteezy-CountryName_-WorldMap-RH0223_generated.jpg"
    # )
    df_mun = st.session_state["municipalities"]

    # df_current_date = df_table.loc[df_table.hourutc == date_time,
    #     :].sort_values("municipality")
    if df_table is not None:
        time_values = df_table.hourutc.unique()
        date_time = st.select_slider(
            label="Time slider", options=time_values, 
            value=pd.to_datetime("2022-06-01")
        )
        df_table = df_table.loc[df_table.hourutc <= date_time, :]

    
data_column = None 
with col_2:
    col_21, col_22, col_23 = st.columns(spec=[0.4, 0.4, 0.2])
    # st.image(
    #     "https://owlcation.com/.image/c_fill,w_1200,h_675,g_faces:center/MTc0MTYyMTE1NzA2NDMwOTcy/how-to-draw-a-scientific-graph.gif"
    # )

    if df_table is not None:
        dropdown_title = current_source["dropdown_title"]
        exclude_data_columns = [
            "id", "dist_to_station", "hourutc", "tsun", "municipality", 
            "hourdk", "municipalityno", "pricearea", "coco"
        ]
        data_columns = [
                data_column
                for data_column in df_table.columns.tolist()
                if data_column not in exclude_data_columns
        ]

        if selected_table == "Power import/export":
            municipality_list = sorted(df_table.pricearea.unique().tolist())
        elif selected_table == "Consumption":
            data_columns = df_table.branche.unique().tolist()
            municipality_list = sorted(df_table.municipality.unique().tolist())
        elif selected_table != "Consumption":
            municipality_list = sorted(df_table.municipality.unique().tolist())

        with col_21:
            if selected_table != "Power import/export":
                data_column = st.selectbox(
                    dropdown_title, options=data_columns, index=0
                )
        with col_22:
            if selected_table != "Power import/export":
                municipality = st.selectbox("Municipality",
                                            options=municipality_list)
        with col_23:
            if selected_table != "Power import/export":
                max_val = st.number_input("DCT filtering", 0, 100, 10, 1)

        # Plotting the line plot
        if selected_table == "Power import/export":
            df_pie = df_table[
            [
                'exchangegb_mwh', 'exchangege_mwh','exchangenl_mwh', 
                'exchangeno_mwh', 'exchangese_mwh', "hourutc",
            ]
        ].loc[df_table.hourutc == date_time, :]
            st.text(df_pie.columns)
        elif selected_table != "Consumption":
            df_line = df_table[df_table.municipality ==
            municipality][["hourutc", "municipality", data_column]]
            smoothed_data = dct_smoothing(df_line, data_column, max_val=max_val)
            df_line["smoothed_signal"] = smoothed_data
            st.plotly_chart(line_plot(df_line, data_column))
        elif selected_table == "Consumption":
            # st.text("Need to aggregate")
            df_line = df_table[df_table.municipality == municipality]
            df_line = df_line[df_table.branche == data_column][
            ["hourutc", "consumptionkwh"]
        ]
            smoothed_data = dct_smoothing(df_line, 
                                          "consumptionkwh", 
                                          max_val=max_val
                                          )
            df_line["smoothed_signal"] = smoothed_data
            st.plotly_chart(line_plot(df_line, "consumptionkwh"))

        # if data_column != "Power import/export":
        #     st.plotly_chart(line_plot(df_line, data_column))
        

# Refactor
with col_1:
    if data_column is not None:
        # Preparing the data which is to be plotted
        if selected_table != "Consumption":
            df_map = df_table.loc[
            df_table.hourutc == date_time,:].sort_values(
                "municipality"
            )[
            ["hourutc", "municipality", data_column]
        ]
        elif selected_table == "Consumption":
            df_map = df_table.loc[
            df_table.hourutc == date_time,:].sort_values(
                "municipality"
            )[
            ["hourutc", "municipality", "consumptionkwh", "branche"]
        ]
            df_map = df_map[df_map.branche == data_column]
        elif selcted_table == "Power import/export":
            pass
        
        if selected_table == "Weather":
            min_val = df_map[data_column].min()
            max_val = df_map[data_column].max()
            df_map["normalized_values"] = (df_map[data_column]-min_val) / \
                (max_val-min_val)
            st.plotly_chart(
                map_plot(df_map, data_column, df_mun, selected_table)
            )
        elif selected_table == "Consumption":
            df_map["sqrt_values"] = np.sqrt(df_map["consumptionkwh"])
            st.plotly_chart(
                map_plot(df_map, "consumptionkwh", df_mun, selected_table)
            )
        elif selected_table == "Production":
            df_map["sqrt_values"] = np.sqrt(df_map[data_column])        
            st.plotly_chart(
                map_plot(df_map, data_column, df_mun, selected_table)
            )
    else:
        st.plotly_chart(
            power_map_plot(df_pie)
        )
        pass

