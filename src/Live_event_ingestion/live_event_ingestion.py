# Live Event Ingestion : This is responsible for ingesting live event data from JSON files, processing each event, and merging it into the events_raw collection in MongoDB. The script will read all JSON files from the specified directory, parse the events, and perform an upsert operation to ensure that existing records are updated while new records are inserted.

from Live_event_ingestion.live_event_loader import get_collection, get_event_files, read_events_from_file, upsert_event

def ingest_live_events():
    collection = get_collection()

    event_files = get_event_files()
    print(f"Found {len(event_files)} event file(s).\n")

    processed = 0
    inserted = 0
    updated = 0

    for file_path in event_files:
        print(f"Processing: {file_path}")

        for event in read_events_from_file(file_path):
            processed += 1

            result = upsert_event(collection, event)

            if result.upserted_id:
                inserted += 1
            else:
                updated += 1

    print("\n Summary:")
    print(f"Processed : {processed}")
    print(f"Inserted  : {inserted}")
    print(f"Updated   : {updated}")
    print("Live events successfully merged into events_raw")


if __name__ == "__main__":
    ingest_live_events()
