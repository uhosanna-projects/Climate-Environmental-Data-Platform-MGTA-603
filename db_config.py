# db_config.py
import os
import urllib

from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "database": os.getenv("MYSQL_DB", "climate"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "port": os.getenv("MYSQL_PORT", 3306),
}

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "climate")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "station_month")

# db_config.py
from sqlalchemy import create_engine

def get_engine():
    # change user, password, host, db name to yours
    user = MYSQL_CONFIG["user"]
    password = MYSQL_CONFIG["password"]
    host = MYSQL_CONFIG["host"]
    db = MYSQL_CONFIG["database"]
    port = MYSQL_CONFIG["port"]

    encoded_password = urllib.parse.quote_plus(password)

    url = f"mysql+mysqlconnector://{user}:{encoded_password}@{host}:{port}/{db}"
    engine = create_engine(url)
    return engine

def load_table(df, table_name, if_exists="replace"):
    """
    Generic loader: takes a pandas DataFrame and pushes it into MySQL.
    Column names in df must match the SQL table columns.
    """
    engine = get_engine()
    # method="multi" = insert many rows per statement (faster)
    df.to_sql(
        table_name,
        con=engine,
        if_exists=if_exists,   # "append" or "replace"
        index=False,
        chunksize=1000,
        method="multi",
    )