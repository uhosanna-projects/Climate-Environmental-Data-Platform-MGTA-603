import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

from sqlalchemy import create_engine
from config_sql import get_engine

from pymongo import MongoClient
from config_mongo import MONGO_URI, MONGO_DB, MONGO_COLLECTION

from neo4j import GraphDatabase
from config_neo4j import host, port, user, password
from ETL_pipelines.climate_neo4j import run_query

# ==============================
# Streamlit App Layout
# ==============================
st.title("🌦 Climate & Environmental Data Platform — Dashboard Demo")
st.write("Use the buttons below to run SQL, MongoDB, and Neo4j queries.")


# ==============================
# Helper Functions
# ==============================

@st.cache_data
def run_sql(query):
    engine = get_engine()
    df = pd.read_sql_query(query, engine)
    return df


@st.cache_data
def run_mongo(pipeline):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]
    return list(col.aggregate(pipeline))


@st.cache_data
def get_neo4j_data():
    uri = f"neo4j://{host}:{port}"
    driver = GraphDatabase.driver(uri, auth=(user, password))

    stations_query = """
    MATCH (s:Station)
    RETURN s.station_id AS station_id,
        s.station_name AS station_name,
        s.latitude AS latitude,
        s.longitude AS longitude
    """

    near_query = """
    MATCH (a:Station)-[r:NEAR]-(b:Station)
    RETURN a.station_id AS from_id,
        b.station_id AS to_id,
        r.distance_km AS distance_km
    """

    stations_df = run_query(stations_query)
    near_df = run_query(near_query)

    return stations_df, near_df


# ==============================
# SECTION 1 — SQL Query Buttons
# ==============================

st.header("📘 SQL Queries")

# ---- SQL Query 1: Rainfall Heatmap ----
if st.button("Show Rainfall Heatmap (Months × Years)"):
    query = """
            SELECT d.year, d.month, d.month_name, SUM(f.value) AS total_rainfall
            FROM fact_measurement f
                     JOIN dim_sensor se ON f.sensor_id = se.sensor_id
                     JOIN dim_date d ON f.date_key = d.date_key
            WHERE se.sensor_name = 'Total Rain'
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month; \
            """

    df = run_sql(query)
    st.dataframe(df)

    heatmap_df = df.pivot(index="month_name", columns="year", values="total_rainfall")

    order = ["January", "February", "March", "April", "May", "June",
             "July", "August", "September", "October", "November", "December"]
    heatmap_df = heatmap_df.reindex(order)

    fig, ax = plt.subplots(figsize=(12, 8))
    im = ax.imshow(heatmap_df, aspect='auto')
    plt.colorbar(im, ax=ax, label='Total Rainfall (mm)')
    ax.set_xticks(np.arange(len(heatmap_df.columns)))
    ax.set_xticklabels(heatmap_df.columns, rotation=45)
    ax.set_yticks(np.arange(len(heatmap_df.index)))
    ax.set_yticklabels(heatmap_df.index)
    ax.set_title("Rainfall Heatmap (Months × Years)")
    st.pyplot(fig)

# ---- SQL Query 2: Annual Temperature Variability ----
if st.button("Show Annual Temperature Variability"):
    query = """
            SELECT d.year, STDDEV(f.value) AS annual_temp_variability
            FROM fact_measurement f
                     JOIN dim_sensor se ON f.sensor_id = se.sensor_id
                     JOIN dim_date d ON f.date_key = d.date_key
            WHERE se.sensor_name = 'Mean Temperature'
            GROUP BY d.year
            ORDER BY d.year; \
            """

    df = run_sql(query)
    st.dataframe(df)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df['year'], df['annual_temp_variability'], marker='o')
    ax.set_title("Annual Temperature Variability (Std Dev)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Variability (°C)")
    ax.grid(True)
    st.pyplot(fig)

# ---- SQL Query 3: Box Plot for 2024 Monthly Variability ----
if st.button("Show 2024 Monthly Temperature Boxplot"):

    query = """
            SELECT d.year, d.month, d.month_name, d.full_date, f.value AS temperature
            FROM fact_measurement f
                     JOIN dim_sensor se ON f.sensor_id = se.sensor_id
                     JOIN dim_date d ON f.date_key = d.date_key
            WHERE se.sensor_name = 'Mean Temperature'
              AND d.year = 2024
            ORDER BY d.month, d.full_date; \
            """

    df = run_sql(query)
    st.dataframe(df)

    month_order = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]

    df["month_name"] = pd.Categorical(df["month_name"], categories=month_order, ordered=True)
    df = df.sort_values("month_name")

    groups, labels = [], []
    for month, group in df.groupby("month_name"):
        groups.append(group["temperature"].values)
        labels.append(month)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.boxplot(groups, labels=labels, showfliers=False)
    ax.set_title("Temperature Variability per Month (2024)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Mean Temperature (°C)")
    plt.xticks(rotation=45)
    st.pyplot(fig)

# ==============================
# SECTION 2 — MongoDB Query
# ==============================
st.header("🟦 MongoDB Query")

if st.button("Show Top 3 Warmest Months (MongoDB)"):
    pipeline = [
        {"$match": {"sensor_name": "Mean Temperature"}},
        {"$group": {"_id": "$month_name", "avg_monthly_temp": {"$avg": "$avg_value"}}},
        {"$sort": {"avg_monthly_temp": -1}},
        {"$limit": 3}
    ]

    data = run_mongo(pipeline)
    st.write(data)

# ==============================
# SECTION 3 — Neo4j Query & Graph
# ==============================
st.header("🔵 Neo4j — Station Network Graph")

if st.button("Show Station Graph"):

    stations_df, near_df = get_neo4j_data()

    st.subheader("Stations Data")
    st.dataframe(stations_df)

    st.subheader("NEAR Relationships")
    st.dataframe(near_df)

    G = nx.Graph()
    for _, row in stations_df.iterrows():
        G.add_node(row['station_id'], latitude=row['latitude'], longitude=row['longitude'])

    for _, row in near_df.iterrows():
        G.add_edge(row['from_id'], row['to_id'], distance=row['distance_km'])

    pos = {row['station_id']: (row['longitude'], row['latitude'])
           for _, row in stations_df.iterrows()}

    fig, ax = plt.subplots(figsize=(12, 8))
    nx.draw_networkx_edges(G, pos, alpha=0.3)
    nx.draw_networkx_nodes(G, pos, node_size=40, node_color='blue')
    ax.set_title("Station Map with NEAR Relationships (Neo4j)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True)
    st.pyplot(fig)
