import snowflake.connector
import config
import streamlit as st
import numpy as np
import pandas as pd
import datetime
import requests
import os
from pandas.api.types import CategoricalDtype
from geopy.geocoders import Nominatim
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
     FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)

# Database connection
USER = str(os.getenv("SNOWFLAKE_USER", ''))
PASSWORD = str(os.getenv("SNOWFLAKE_PASSWORD", ''))
ACCOUNT = str(os.getenv("SNOWFLAKE_ACCOUNT", ''))
WAREHOUSE = str(os.getenv("SNOWFLAKE_WAREHOUSE", ''))
DATABASE = str(os.getenv("SNOWFLAKE_DATABASE", ''))
SCHEMA = str(os.getenv("SNOWFLAKE_SCHEMA", ''))

ctx = snowflake.connector.connect(
    user=config.USER,
    password=config.PASSWORD,
    account=config.ACCOUNT,
    warehouse=config.WAREHOUSE,
    database=config.DATABASE,
    schema=config.SCHEMA
)
cur = ctx.cursor()


# Importing GeoJson Mexico data
repo_url = 'https://raw.githubusercontent.com/angelnmara/geojson/master/mexicoHigh.json' # Mexico GeoJson file
mx_regions_geo = requests.get(repo_url).json()


# Get the organizer's active events information
@st.cache
def load_organizer_active_events(organizer_id):
    # Execute a query to extract the data
    sql = f"""select
                event_id,
                organizer_id,
                name,
                category,
                subcategory,
                subdomain,
                organizer_name,
                email,
                date(started_at) as started_at
            from EVENTS
            where organizer_id = {organizer_id}
            and status = 'active'
            order by name, started_at
            """
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
    except:
        df = pd.DataFrame()
    
    return df


# Run the report request to google Analytics
@st.cache
def customers_cities_report(events_list):
    """Runs a simple report on a Google Analytics 4 property."""
    property_id = "251040423"

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="city"),
            Dimension(name="region"),
            Dimension(name="country"),
            #Dimension(name="hostName"),
        ],
        metrics=[Metric(name="totalUsers")],
        date_ranges=[DateRange(start_date="365daysAgo", end_date="today")],
        dimension_filter = FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name="pagePath",
                            string_filter=Filter.StringFilter(
                                match_type="CONTAINS",
                                value="/finish"
                            ),
                        )
                    ),
                    FilterExpression(
                        filter=Filter(
                            field_name="hostName",
                            in_list_filter=Filter.InListFilter(
                                values=events_list
                            ),
                        )
                    ),
                ]
            )

        ),
    )
    response = client.run_report(request)

    return get_cities_to_dataframe(response)


# Convert the GA report response to a pandas dataframe
@st.cache
def get_cities_to_dataframe(response):

    # Empty dataframe with the structure of the report
    df = pd.DataFrame(columns=["CIUDAD", "ESTADO", "PAIS", "COMPRADORES"])

    # Iterate the rows of the response
    for row in response.rows:
        # Create an auxiliary dictionary to create a new row
        new_row = {
            "CIUDAD": row.dimension_values[0].value,
            "ESTADO": row.dimension_values[1].value,
            "PAIS": row.dimension_values[2].value,
            #"hostname": row.dimension_values[3].value,
            "COMPRADORES": row.metric_values[0].value
        }
        
        # Add the new row to the dataframe
        df = df.append(new_row, ignore_index=True)
    
    # Fix the dtype of the users column
    df = df.astype({'COMPRADORES': int})
    # Replace the name of some Mexico regions (states) to match the geo json data
    df['ESTADO'] = df['ESTADO'].replace({
        'Mexico City': 'Ciudad de México',
        'State of Mexico': 'México',
        'Nuevo Leon': 'Nuevo León',
        'Yucatan': 'Yucatán',
        'Michoacan': 'Michoacán',
        'Queretaro': 'Querétaro',
        'San Luis Potosi': 'San Luis Potosí'
    })

    return df


# Run the report request to google Analytics
@st.cache
def customers_ages_report(events_list):
    """Runs a simple report on a Google Analytics 4 property."""
    property_id = "251040423"

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="userAgeBracket"),
            #Dimension(name="hostName"),
        ],
        metrics=[Metric(name="totalUsers")],
        date_ranges=[DateRange(start_date="365daysAgo", end_date="today")],
        dimension_filter = FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name="pagePath",
                            string_filter=Filter.StringFilter(
                                match_type="CONTAINS",
                                value="/finish"
                            ),
                        )
                    ),
                    FilterExpression(
                        filter=Filter(
                            field_name="hostName",
                            in_list_filter=Filter.InListFilter(
                                values=events_list
                            ),
                        )
                    ),
                ]
            )

        ),
    )
    response = client.run_report(request)

    return get_ages_to_dataframe(response)


# Convert the GA report response to a pandas dataframe
@st.cache
def get_ages_to_dataframe(response):

    # Empty dataframe with the structure of the report
    df = pd.DataFrame(columns=["EDAD", "COMPRADORES"])

    # Iterate the rows of the response
    for row in response.rows:
        # Create an auxiliary dictionary to create a new row
        new_row = {
            "EDAD": row.dimension_values[0].value,
            "COMPRADORES": row.metric_values[0].value
        }
        
        # Add the new row to the dataframe
        df = df.append(new_row, ignore_index=True)
    
    # Fix the dtype of the users column
    df = df.astype({'COMPRADORES': int})
    # Replace the name of some Mexico regions (states) to match the geo json data
    df['EDAD'] = df['EDAD'].replace({
        'unknown': 'Desconocida'
    })

    return df


# Run the report request to google Analytics
@st.cache
def customers_genders_report(events_list):
    """Runs a simple report on a Google Analytics 4 property."""
    property_id = "251040423"

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="userGender"),
            #Dimension(name="hostName"),
        ],
        metrics=[Metric(name="totalUsers")],
        date_ranges=[DateRange(start_date="365daysAgo", end_date="today")],
        dimension_filter = FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name="pagePath",
                            string_filter=Filter.StringFilter(
                                match_type="CONTAINS",
                                value="/finish"
                            ),
                        )
                    ),
                    FilterExpression(
                        filter=Filter(
                            field_name="hostName",
                            in_list_filter=Filter.InListFilter(
                                values=events_list
                            ),
                        )
                    ),
                ]
            )

        ),
    )
    response = client.run_report(request)

    return get_genders_to_dataframe(response)


# Convert the GA report response to a pandas dataframe
@st.cache
def get_genders_to_dataframe(response):

    # Empty dataframe with the structure of the report
    df = pd.DataFrame(columns=["GENERO", "COMPRADORES"])

    # Iterate the rows of the response
    for row in response.rows:
        # Create an auxiliary dictionary to create a new row
        new_row = {
            "GENERO": row.dimension_values[0].value,
            "COMPRADORES": row.metric_values[0].value
        }
        
        # Add the new row to the dataframe
        df = df.append(new_row, ignore_index=True)
    
    # Fix the dtype of the users column
    df = df.astype({'COMPRADORES': int})
    # Replace the name of some Mexico regions (states) to match the geo json data
    df['GENERO'] = df['GENERO'].replace({
        'unknown': 'Desconocido',
        'female': 'Femenino',
        'male': 'Masculino'
    })

    return df