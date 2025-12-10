import pandas as pd
from sqlalchemy import text

from config_sql import get_engine
from config_redis import get_redis_client


# --- Create shared clients (like driver in Neo4j script) ---
from config_redis import get_redis_client
engine = get_engine()
r = get_redis_client()

# functions to read from leaderboards...


# ---------- 1) CACHE STATION METADATA IN REDIS ----------

def cache_station_metadata():
    """
    Load dim_station from MySQL and cache station info in Redis.

    - Hash per station:  station:<station_id>
    - Also a global hash station:names (id -> name) for convenience.
    """
    df = pd.read_sql_table("dim_station", engine)

    pipe = r.pipeline()

    for _, row in df.iterrows():
        station_id = str(row["station_id"])
        name = row.get("station_name")
        lat = float(row.get("latitude")) if row.get("latitude") is not None else None
        lon = float(row.get("longitude")) if row.get("longitude") is not None else None

        # Hash per station with basic metadata
        station_key = f"station:{station_id}"
        mapping = {
            "station_id": station_id,
            "station_name": name or "",
        }
        if lat is not None:
            mapping["latitude"] = lat
        if lon is not None:
            mapping["longitude"] = lon

        pipe.hset(station_key, mapping=mapping)

        # Simple id -> name lookup hash
        if name:
            pipe.hset("station:names", station_id, name)

    pipe.execute()
    print(f"Cached {len(df)} stations in Redis ✔")


# ---------- 2) BUILD HOTTEST STATION LEADERBOARD ----------

def build_hottest_leaderboard(days: int = 30):
    """
    Leaderboard: stations with highest average *mean temperature* over all available data.
    (days is kept in the key name for consistency, but not used in the filter.)
    """
    leaderboard_key = f"leaderboard:hottest:{days}d"

    query = text("""
        SELECT
            f.station_id,
            AVG(f.value) AS avg_temp
        FROM fact_measurement f
        JOIN dim_sensor se ON f.sensor_id = se.sensor_id
        WHERE se.sensor_name = 'Mean Temperature'
        GROUP BY f.station_id
    """)

    df = pd.read_sql(query, engine)

    # reset leaderboard
    r.delete(leaderboard_key)

    pipe = r.pipeline()
    for _, row in df.iterrows():
        station_id = str(row["station_id"])
        score = float(row["avg_temp"])
        pipe.zadd(leaderboard_key, {station_id: score})
    pipe.execute()

    print(f"Hottest leaderboard built (all data, {len(df)} stations) ✔")

# ---------- 3) BUILD WETTEST STATION LEADERBOARD ----------

def build_wettest_leaderboard(days: int = 30):
    """
    Leaderboard: stations with highest *total precipitation* over all available data.
    """
    leaderboard_key = f"leaderboard:wettest:{days}d"

    query = text("""
        SELECT
            f.station_id,
            SUM(f.value) AS total_precip
        FROM fact_measurement f
        JOIN dim_sensor se ON f.sensor_id = se.sensor_id
        WHERE se.sensor_name = 'Total Precipitation'
        GROUP BY f.station_id
    """)

    df = pd.read_sql(query, engine)

    # reset leaderboard
    r.delete(leaderboard_key)

    pipe = r.pipeline()
    for _, row in df.iterrows():
        station_id = str(row["station_id"])
        score = float(row["total_precip"])
        pipe.zadd(leaderboard_key, {station_id: score})
    pipe.execute()

    print(f"Wettest leaderboard built (all data, {len(df)} stations) ✔")


# ---------- 4) MAIN ETL ENTRYPOINT ----------

if __name__ == "__main__":
    cache_station_metadata()
    build_hottest_leaderboard(days=30)
    build_wettest_leaderboard(days=30)
    print("Redis ETL pipeline completed ✔")
