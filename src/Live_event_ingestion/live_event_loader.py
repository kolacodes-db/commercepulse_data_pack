# Connects to MongoDB, reads live event data from JSON files, and upserts it into the events_raw collection. The script is designed to handle large volumes of data efficiently by processing each event individually and using MongoDB's upsert functionality to avoid duplicates while ensuring that existing records are updated with the latest information.
from pymongo import MongoClient

def get_collection(
    uri="mongodb://localhost:27017/",
    db_name="commercepulse",
    collection_name="events_raw"
):
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]


from pathlib import Path
import json

def get_event_files(root_dir="data/live_events"):
    root_path = Path(root_dir)
    return list(root_path.glob("*/events.jsonl"))

def read_events_from_file(file_path):
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def upsert_event(collection, event):
    """
    Upsert event into MongoDB using event_id.
    Payload remains untouched.
    """
    return collection.update_one(
        {"event_id": event["event_id"]},
        {"$set": event},
        upsert=True
    )
