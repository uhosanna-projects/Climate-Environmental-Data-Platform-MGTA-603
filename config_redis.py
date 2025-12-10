import os
import redis
from dotenv import load_dotenv

load_dotenv()

def get_redis_client():
    host = os.getenv('REDIS_HOST', '127.0.0.1')
    port = int(os.getenv('REDIS_PORT', '6379'))
    password = os.getenv('REDIS_PASSWORD', '')
    ssl = os.getenv('REDIS_SSL', 'False').lower() == 'true'

    return redis.Redis(
        host=host,
        port=port,
        password=password,
        ssl=ssl,                  # Redis Cloud requires SSL=True
        decode_responses=True     # ensures strings instead of bytes
    )
