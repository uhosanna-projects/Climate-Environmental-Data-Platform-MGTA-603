import pandas as pd
from config_redis import get_redis_client

def fetch_leaderboard(metric: str, days: int = 30, top_n: int = 10) -> pd.DataFrame:
    """
    Fetch leaderboard from Redis and return as a DataFrame.

    metric: 'hottest' or 'wettest'
    days:   30 (just used in the Redis key name)
    """
    r = get_redis_client()
    key = f"leaderboard:{metric}:{days}d"

    rows = r.zrevrange(key, 0, top_n - 1, withscores=True)

    data = []
    for rank, (station_id, score) in enumerate(rows, start=1):
        name = r.hget("station:names", station_id) or "(unknown)"
        data.append({
            "Rank": rank,
            "Station ID": station_id,
            "Station Name": name,
            "Score": score
        })

    return pd.DataFrame(data)
