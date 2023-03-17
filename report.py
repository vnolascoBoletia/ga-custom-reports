import streamlit as st
import pandas as pd
import utils
import plotly.express as px
from datetime import datetime


# Organizer_id and info
st.sidebar.subheader("ORGANIZADOR")
st.session_state["organizer_id"] = st.sidebar.text_input("USER ID:", 2531331)

# Dataframes with the GA reports responses
if "df_cities" not in st.session_state:
    st.session_state["df_cities"] = pd.DataFrame()
if "df_ages" not in st.session_state:
    st.session_state["df_ages"] = pd.DataFrame()
if "df_genders" not in st.session_state:
    st.session_state["df_genders"] = pd.DataFrame()

# Getting all the active events of the organizer 
all_events = utils.load_organizer_active_events(st.session_state["organizer_id"])
#st.write(all_events)   # all events details


# SIDEBAR
st.sidebar.write("Nombre:   ", all_events["ORGANIZER_NAME"][0])
st.sidebar.write("Email:    ", all_events["EMAIL"][0])
st.sidebar.write("---")
st.sidebar.subheader("SUS EVENTOS")

# Selectbox with the categories
categories = ["Todas las categorías"] + all_events["CATEGORY"].unique().tolist()
cat = st.sidebar.selectbox("Selecciona la categoría de los eventos que deseas visualizar.", categories)

# Filter the events of the selected category
if cat == "Todas las categorías":
    events_cat = all_events.copy()
else:
    events_cat = all_events.loc[all_events["CATEGORY"] == cat]

# List to store the checked events
events_list = []

# Checkboxes of the filtered events
st.sidebar.caption("Haz check en los eventos que deseas consultar.")
all_checked = st.sidebar.checkbox("Seleccionar todos") # Select all option
for i in events_cat.index:
        check = st.sidebar.checkbox(events_cat['NAME'][i] + ' - ' + str(events_cat['STARTED_AT'][i]),
                                        key=f"checkbox_{events_cat['EVENT_ID'][i]}",
                                        value=all_checked)  # Take the value of the 'Select all checkbox'
        if check:
            # Add the subdomain of the event to the list
            events_list.append(events_cat['SUBDOMAIN'][i] + ".boletia.com")


# Button to run the reports request to GA using the checked events
if st.sidebar.button("CONSULTAR EVENTOS"):
    # Customers cities report
    st.session_state["df_cities"] = utils.customers_cities_report(events_list)
    # Customers ages report
    st.session_state["df_ages"] = utils.customers_ages_report(events_list)
    # Customers genders report
    st.session_state["df_genders"] = utils.customers_genders_report(events_list)


# BODY OF THE PAGE
df_cities = st.session_state["df_cities"]
df_ages = st.session_state["df_ages"]
df_genders = st.session_state["df_genders"]
#st.write(events_list)  # list of events to be queried
#st.write(df_cities)   # dataframe with the GA report response

# Customers localization
customers_map_container = st.container()
with customers_map_container:

    st.subheader("Localización de los compradores")

    # First check if the df with the data is not null
    if df_cities.empty:
        st.info("No hay información para visualizar de los eventos seleccionados.")
    else:
        # Filter just Mexico data
        mexico_data = df_cities.loc[df_cities['PAIS'] == 'Mexico']

        # Group the users by region (state)
        users_per_state = mexico_data.groupby('ESTADO')['COMPRADORES'].sum()
        users_per_state = users_per_state.reset_index()
        users_per_state = users_per_state.rename(columns={"index": "ESTADO"})

        # Creating visualization
        fig = px.choropleth(data_frame = users_per_state, 
                            geojson = utils.mx_regions_geo, 
                            locations = 'ESTADO', 
                            featureidkey='properties.name', 
                            color='COMPRADORES',
                            color_continuous_scale="Peach")
        fig.update_geos(showcountries=True, 
                        showcoastlines=True, 
                        showland=True, 
                        fitbounds="locations", 
                        showsubunits=True, 
                        landcolor='#E0E0E0')
        fig.update_layout({
                            'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                            'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                            'margin': {"r": 0, "t": 0, "l": 0, "b": 0} })
        
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Datos por ciudad")
        st.table(df_cities)


# Customers ages
customers_age_container = st.container()
with customers_age_container:
    st.subheader("Edad de los compradores")

    # First check if the df with the data is not null
    if df_ages.empty:
        st.info("No hay información para visualizar de los eventos seleccionados.")
    else:
        # Creating visualization
        fig = px.bar(df_ages, x="EDAD", y="COMPRADORES", orientation="v",
                            color="EDAD")
        fig.update_layout({
            'plot_bgcolor': 'rgba(0, 0, 0, 0)',
            'paper_bgcolor': 'rgba(0, 0, 0, 0)',
        })
        fig.update_yaxes(gridcolor="rgba(0,0,0,0.1)")
        st.plotly_chart(fig, use_container_width=True)


# Customers genders
customers_gender_container = st.container()
with customers_gender_container:
    st.subheader("Género de los compradores")

    # First check if the df with the data is not null
    if df_genders.empty:
        st.info("No hay información para visualizar de los eventos seleccionados.")
    else:
        # Creating visualization
        fig = px.bar(df_genders, x="GENERO", y="COMPRADORES", orientation="v",
                            color="GENERO",
                            labels={'GENERO': 'GÉNERO'})
        fig.update_layout({
            'plot_bgcolor': 'rgba(0, 0, 0, 0)',
            'paper_bgcolor': 'rgba(0, 0, 0, 0)',
        })
        fig.update_yaxes(gridcolor="rgba(0,0,0,0.1)")
        st.plotly_chart(fig, use_container_width=True)
