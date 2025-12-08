import os

from dotenv import load_dotenv




load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "climate")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "station_month")
