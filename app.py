import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import time

from event_generator import EventGenerator

# Directory to store parquet files
output_dir = "data/events"
os.makedirs(output_dir, exist_ok=True)

batch_size = 10


def fetch_events():
    return EventGenerator().generate_events(batch_size)


def write_to_parquet(data):
    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(data)

    # Write to Parquet with partitioning by date
    pq.write_to_dataset(table, root_path=output_dir, partition_cols=['event_date'])


def main():
    while True:
        try:
            events = fetch_events()
            print(events)
            if events:
                df = pd.DataFrame(events)

                # Ensure the event time is in datetime format
                if 'event_time' in df.columns:
                    df['event_time'] = pd.to_datetime(df['event_time'])
                    df['event_date'] = df['event_time'].dt.strftime('%Y-%m-%d')
                    write_to_parquet(df)
                else:
                    print("event_time field not found in the data.")
        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(5)


if __name__ == "__main__":
    main()
