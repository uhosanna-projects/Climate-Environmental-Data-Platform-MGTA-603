from config_redis import get_redis_client

r = get_redis_client()

RECENT_QUERY_KEY = "recent:queries"
MAX_RECENT = 10   # keep last 10 queries


def cache_recent_query(query_text: str):
    """
    Store the user's recent queries in Redis.
    Uses an LPUSH queue and trims to MAX_RECENT.
    """
    r.lpush(RECENT_QUERY_KEY, query_text)
    r.ltrim(RECENT_QUERY_KEY, 0, MAX_RECENT - 1)


def get_recent_queries(n=10):
    """
    Retrieve the most recent n queries.
    """
    return [q for q in r.lrange(RECENT_QUERY_KEY, 0, n - 1)]

from redis_query_log import get_recent_queries

print(get_recent_queries(10))
