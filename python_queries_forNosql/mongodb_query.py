from pymongo import MongoClient

from config_mongo import MONGO_DB, MONGO_COLLECTION, MONGO_URI

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

pipeline = [
    {"$match": {"sensor_name": "Mean Temperature"}},
    {"$group": {
        "_id": "$month_name",
        "avg_monthly_temp": {"$avg": "$avg_value"}
    }},
    {"$sort": {"avg_monthly_temp": -1}},
    {"$limit": 3}
]
print(list(collection.aggregate(pipeline)))