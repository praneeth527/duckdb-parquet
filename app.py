import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import time
from collections import defaultdict

from event_generator import EventGenerator

# Directory to store parquet files
OUTPUT_DIR = "data/events"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BATCH_SIZE = 100
POLLING_INTERVAL = 5


def fetch_events():
    return EventGenerator().generate_events(BATCH_SIZE)


def append_events_to_parquet(events_by_date):
    for event_date, events in events_by_date.items():
        file_path = f'{OUTPUT_DIR}/event_date={event_date}/data.parquet'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path):
            existing_table = pq.read_table(file_path)
            existing_df = existing_table.to_pandas()
            new_df = pd.DataFrame(events)

            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = pd.DataFrame(events)

        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, file_path)


def main():
    while True:
        try:
            events = fetch_events()
            events_by_date = defaultdict(list)
            print(events)
            if len(events) > 0:
                for event in events:
                    event_date = event['event_time'].isoformat()[:10]
                    events_by_date[event_date].append(event)

                append_events_to_parquet(events_by_date)
        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(POLLING_INTERVAL)


if __name__ == "__main__":
    main()
