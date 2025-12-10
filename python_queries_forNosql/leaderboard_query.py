from config_redis import get_redis_client

def show_leaderboard(key: str, top_n: int = 10):
    """
    Prints a leaderboard stored in Redis sorted set.
    Uses station metadata cached at: station:<id> and station:names
    """

    r = get_redis_client()

    print(f"\n=== Leaderboard: {key} (Top {top_n}) ===")

    # Get top stations (highest scores)
    results = r.zrevrange(key, 0, top_n - 1, withscores=True)

    if not results:
        print("No leaderboard data found.")
        return

    # For reading station names (cached earlier)
    for rank, (station_id, score) in enumerate(results, start=1):
        name = r.hget("station:names", station_id) or "(unknown)"
        print(f"{rank}. Station {station_id} — {name} — Score: {score:.2f}")

    print("\n====================================\n")


if __name__ == "__main__":
    # Keys created by your ETL pipeline
    hottest_key = "leaderboard:hottest:30d"
    wettest_key = "leaderboard:wettest:30d"

    show_leaderboard(hottest_key)
    show_leaderboard(wettest_key)
