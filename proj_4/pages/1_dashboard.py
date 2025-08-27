import streamlit as st 

# Sidebar
st.sidebar.markdown("# Main page 🎈")

# Main page content
top_col_1, top_col_2 = st.columns(spec=[0.65, 0.35])
data_sources = {"Production": "production", 
                "Consumption": "consumption", 
                "Prodcons": "prodcons", 
                "Weather": "weather"}

# q = {
#     "Production": {
#         "session_state": "production",
#         "dropdown_title": "Production Type",
#         "municipality": True,
#     },
#     "Consumption": {
#         "session_state": "consumption",
#         "dropdown_title": "Industry Group",
#         "municipality": True,
#     },
#     "Prodcons": {
#         "session_state": "prodcons",
#         "dropdown_title": None,
#         "municipality": False,
#     },
#     "Weather": {
#         "session_state": "weather",
#         "dropdown_title": "Weather Property",
#         "municipality": True,
#     },
# }

with top_col_1:
    st.markdown("# Main page 🎈")

with top_col_2:

    selected_table = st.selectbox(
        "Data source",
        options=["Production", "Consumption", "Power import/export", "Weather"],
        index=None
    )

    # TODO: Finne en måte å hente ut de kommunenavnene til de DataFrame-ne som
    # kun har nummerene ("production" og "consumption"). Kan jo legge til dem nå
    # lowkey?

    df_table = (
        None
        if selected_table is None
        else st.session_state[data_sources[selected_table]]
    )  # Retrieve DataFrame from the session state

    if df_table is not None:
        print(f"The current data source is set to {selected_table}.")
    else:
        print("No data source is currently set.")

    

# if st.checkbox("Show dataframe"):
#     # st.table( df_mun ) # static DataFrame?
#     st.session_state["municipalities"]# Shows the DataFrame?

col_1, col_2 = st.columns(spec=[0.6, 0.4])

with col_1:
    st.image("https://freevector-images.s3.amazonaws.com/uploads/vector/preview/165256/vecteezy-CountryName_-WorldMap-RH0223_generated.jpg")

with col_2:
    col_21, col_22 = st.columns(spec=[0.5, 0.5])
    
    # st.selectbox("dog", options=[x for x in range(0, 11)])
    if df_table is not None:
        municipality_list = sorted(df_table.municipality.unique().tolist()) # if ... else
        with col_21:
            st.selectbox("dog", options=df_table.columns.tolist())
        with col_22:
            st.selectbox("Municipality",
                     options=municipality_list)
        # st.selectbox("dog", options=df.columns.tolist())
        # st.selectbox("Municipality",
        #              options=municipality_list)
    st.image("https://owlcation.com/.image/c_fill,w_1200,h_675,g_faces:center/MTc0MTYyMTE1NzA2NDMwOTcy/how-to-draw-a-scientific-graph.gif")
