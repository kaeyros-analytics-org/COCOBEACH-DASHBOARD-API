import redis
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Redis client
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.getenv("REDIS_DB", 0)),
    decode_responses=True
)

def get_cache(key: str):
    return r.get(key)

def set_cache(key: str, value: str, expire: int = None):
    ttl = expire or int(os.getenv("REDIS_TTL", 300))
    r.set(key, value, ex=ttl)

def make_cache_key(prefix: str, **kwargs):
    parts = [prefix] + [f"{k}={v}" for k, v in kwargs.items() if v is not None]
    return ":".join(parts)

# PostgreSQL connection util
def get_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    return conn

# Default company
DEFAULT_COMPANY = os.getenv("COMPANY")
