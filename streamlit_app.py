# ===========================================================
#   Climate & Environmental Data Platform - Streamlit App
#   Single-file version (SQL + MongoDB + Neo4j + Scenarios)
# ===========================================================

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

# ---- Your config files ----
from config_sql import get_engine
from config_mongo import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from pymongo import MongoClient
from ETL_pipelines.climate_neo4j import run_query
from python_queries_forNosql.leaderboard_utils import fetch_leaderboard



# ===========================================================
#   Page Setup
# ===========================================================
st.set_page_config(page_title="Climate Platform", page_icon="🌦", layout="wide")

st.title("🌦 Climate & Environmental Data Platform")
st.write("A unified analytics dashboard built using SQL, MongoDB, and Neo4j.")


# ===========================================================
#   Helper Functions
# ===========================================================

@st.cache_data
def run_sql_query(query):
    engine = get_engine()
    return pd.read_sql_query(query, engine)

@st.cache_data
def run_mongo_query(pipeline):
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]
    return list(collection.aggregate(pipeline))

@st.cache_data
def load_neo4j_data():
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

####### procedure
# --------------------- Stored Procedure: Monthly Rainfall by Year ---------------------
st.subheader("🌀 Stored Procedure: Monthly Rainfall for Selected Year")

year_input = st.number_input("Enter Year:", min_value=1990, max_value=2030, value=2024, step=1)

if st.button("Run Stored Procedure"):
    engine = get_engine()

    # Use raw connection for stored procedures
    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        cursor.callproc("get_monthly_rainfall_for_year", [int(year_input)])

        # Handle MySQL stored_results()
        for result in cursor.stored_results():
            rows = result.fetchall()
            columns = [col[0] for col in result.description]
            df_sp = pd.DataFrame(rows, columns=columns)

    finally:
        cursor.close()
        conn.close()

    st.success(f"Stored Procedure Result for {year_input}")
    st.dataframe(df_sp)

    # Plot result
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(df_sp["month_name"], df_sp["total_rainfall"], color="steelblue")
    ax.set_title(f"Total Rainfall by Month in {year_input}")
    ax.set_xlabel("Month")
    ax.set_ylabel("Rainfall (mm)")
    ax.tick_params(axis="x", rotation=45)
    st.pyplot(fig)







# ===========================================================
#   Section: SQL ANALYTICS
# ===========================================================
st.header("📘 SQL Analytics")

# --------------------- Query 1: Rainfall Heatmap ---------------------
st.subheader("🌧 Rainfall Heatmap (Months × Years)")

if st.button("Run Rainfall Heatmap Query"):
    query = """
    SELECT d.year, d.month, d.month_name, SUM(f.value) AS total_rainfall
    FROM fact_measurement f
    JOIN dim_sensor se ON f.sensor_id = se.sensor_id
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE se.sensor_name = 'Total Rain'
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month;
    """

    df = run_sql_query(query)
    st.dataframe(df)

    # Pivot to heatmap format
    heatmap_df = df.pivot(index="month_name", columns="year", values="total_rainfall")

    # Month ordering
    order = ["January","February","March","April","May","June",
             "July","August","September","October","November","December"]
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



# --------------------- Query 2: Annual Temperature Variability ---------------------
st.subheader("🌡 Annual Temperature Variability (Standard Deviation)")

if st.button("Run Temperature Variability Query"):
    query = """
    SELECT d.year, STDDEV(f.value) AS annual_temp_variability
    FROM fact_measurement f
    JOIN dim_sensor se ON f.sensor_id = se.sensor_id
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE se.sensor_name = 'Mean Temperature'
    GROUP BY d.year
    ORDER BY d.year;
    """

    df = run_sql_query(query)
    st.dataframe(df)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df['year'], df['annual_temp_variability'], marker='o')
    ax.set_title("Annual Temperature Variability (Std Dev)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Variability (°C)")
    ax.grid(True)
    st.pyplot(fig)



# --------------------- Query 3: Monthly Boxplot (2024) ---------------------
st.subheader("📦 Monthly Temperature Boxplot (2024)")

if st.button("Run 2024 Boxplot Query"):
    query = """
    SELECT d.year, d.month, d.month_name, d.full_date, f.value AS temperature
    FROM fact_measurement f
    JOIN dim_sensor se ON f.sensor_id = se.sensor_id
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE se.sensor_name = 'Mean Temperature'
      AND d.year = 2024
    ORDER BY d.month, d.full_date;
    """

    df = run_sql_query(query)
    st.dataframe(df)

    # Month order
    order = ["January","February","March","April","May","June",
             "July","August","September","October","November","December"]

    df["month_name"] = pd.Categorical(df["month_name"], categories=order, ordered=True)
    df = df.sort_values("month_name")

    groups = [group["temperature"].values for _, group in df.groupby("month_name")]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.boxplot(groups, labels=order, showfliers=False)
    ax.set_title("Temperature Variability per Month in 2024 (Boxplot)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Mean Temperature (°C)")
    st.pyplot(fig)



# ===========================================================
#   Section: MongoDB Insights
# ===========================================================
st.header("🍃 MongoDB Insights")

if st.button("Show Top 3 Warmest Months from MongoDB"):

    pipeline = [
        {"$match": {"sensor_name": "Mean Temperature"}},
        {"$group": {"_id": "$month_name", "avg_monthly_temp": {"$avg": "$avg_value"}}},
        {"$sort": {"avg_monthly_temp": -1}},
        {"$limit": 3}
    ]

    result = run_mongo_query(pipeline)
    st.json(result)



# ===========================================================
#   Section: Neo4j Graph Visualization
# ===========================================================
st.header("🔵 Neo4j Station Network Graph")

if st.button("Show Station Graph"):

    stations_df, near_df = load_neo4j_data()

    st.subheader("Station Nodes")
    st.dataframe(stations_df)

    G = nx.Graph()

    # Add nodes
    for _, row in stations_df.iterrows():
        G.add_node(row["station_id"], pos=(row["longitude"], row["latitude"]))

    # Add edges
    for _, row in near_df.iterrows():
        G.add_edge(row["from_id"], row["to_id"], weight=row["distance_km"])

    pos = nx.get_node_attributes(G, "pos")

    fig, ax = plt.subplots(figsize=(12, 8))
    nx.draw(G, pos, node_size=40, node_color="blue", edge_color="gray", alpha=0.5)
    ax.set_title("Weather Station Proximity Network (Neo4j)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    st.pyplot(fig)



# ===========================================================
#   Section: Redis Leaderboards
# ===========================================================
st.header("🏆 Climate Station Leaderboards (Redis)")

st.markdown(
    """
    This section reads from **Redis** and shows:
    - Top **hottest** stations (by mean temperature)
    - Top **wettest** stations (by total precipitation)
    """
)

# --- Sidebar controls for leaderboards ---
st.sidebar.header("Leaderboard Controls")

metric = st.sidebar.selectbox(
    "Metric",
    options=["hottest", "wettest"],
    format_func=lambda x: "Hottest stations" if x == "hottest" else "Wettest stations"
)

# your ETL currently uses 30 days in the key
days = 30
top_n = st.sidebar.slider(
    "Number of stations to show (leaderboard)",
    min_value=5,
    max_value=50,
    value=10,
    step=5,
)

# --- Fetch data from Redis ---
df_lb = fetch_leaderboard(metric=metric, days=days, top_n=top_n)

if df_lb.empty:
    st.warning("No leaderboard data found. Did you run the Redis ETL pipeline?")
else:
    title_map = {
        "hottest": "Top Hottest Stations (Mean Temperature)",
        "wettest": "Top Wettest Stations (Total Precipitation)",
    }
    st.subheader(title_map[metric])

    # Show table
    st.dataframe(df_lb, use_container_width=True)

    # Simple bar chart of scores
    st.markdown("#### Score comparison")
    chart_df = df_lb.set_index("Station Name")["Score"]
    st.bar_chart(chart_df)






# ===========================================================
#   Section: Industry Scenarios
# ===========================================================
st.header("🏭 Real Industry Scenarios")

st.markdown("""
### 🌾 **1. Agriculture (Crop Yield & Irrigation Planning)**
- Predict droughts using rainfall variability  
- Identify heatwaves from temperature distributions  
- Improve planting and irrigation decisions  

### 🏦 **2. Insurance (Climate Risk Models)**
- Flood risk scoring  
- Severe rainfall trend analysis  
- Pricing based on historical climate volatility  

### 🚛 **3. Logistics & Transportation**
- Weather-aware routing  
- Snow, ice, and rainfall impact on delivery times  
- Road hazard prediction  

### 🔌 **4. Renewable Energy**
- Solar radiation & temperature patterns  
- Wind speed distribution  
- Energy production forecasting  

### 🌍 **5. Environmental Research & Climate Science**
- Long-term temperature variability  
- Urban heat island analysis  
- Extreme weather event detection  
""")
