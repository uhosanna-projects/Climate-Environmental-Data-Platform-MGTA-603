import streamlit as st
import pandas as pd

from leaderboard_utils import fetch_leaderboard

st.set_page_config(page_title="Climate Leaderboards", layout="wide")

st.title("🌡️❄️ Climate Station Leaderboards")

st.markdown(
    """
    This dashboard reads from **Redis** and shows:
    - Top **hottest** stations (by mean temperature)
    - Top **wettest** stations (by total precipitation)
    """
)

# --- Sidebar controls ---
st.sidebar.header("Controls")

metric = st.sidebar.selectbox(
    "Metric",
    options=["hottest", "wettest"],
    format_func=lambda x: "Hottest stations" if x == "hottest" else "Wettest stations"
)

days = 30  # right now your ETL uses 30 in the key name, even though it's "all data"
top_n = st.sidebar.slider("Number of stations to show", min_value=5, max_value=50, value=10, step=5)

# --- Fetch data from Redis ---
df = fetch_leaderboard(metric=metric, days=days, top_n=top_n)

if df.empty:
    st.warning("No leaderboard data found. Did you run the Redis ETL pipeline?")
else:
    # Title for current view
    title_map = {
        "hottest": "Top Hottest Stations (Mean Temperature)",
        "wettest": "Top Wettest Stations (Total Precipitation)",
    }
    st.subheader(title_map[metric])

    # Show table
    st.dataframe(df, use_container_width=True)

    # Simple bar chart of scores
    st.markdown("#### Score comparison")
    chart_df = df.set_index("Station Name")["Score"]
    st.bar_chart(chart_df)
