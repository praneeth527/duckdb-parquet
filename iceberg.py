import time

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from event_generator import EventGenerator
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    DoubleType,
    StringType,
    NestedField, LongType
)

warehouse_path = "/Users/praneethreddy/personal/duckdb-parquet/data/warehouse"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

table_location = f"{warehouse_path}/events"

try:
    catalog.create_namespace("default")
except Exception as e:
    print(e)

BATCH_SIZE = 100
POLLING_INTERVAL = 5

iceberg_schema = Schema(
    NestedField(field_id=1, name="event_time", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="event_name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="name", field_type=StringType(), required=False),
    NestedField(field_id=4, name="city", field_type=StringType(), required=False),
    NestedField(field_id=4, name="amount", field_type=DoubleType(), required=False),
)

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="event_day"
    )
)

arrow_schema = pa.schema([
    pa.field("event_time", pa.timestamp('us'), nullable=False),  # Iceberg TimestampType() -> PyArrow timestamp
    pa.field("event_name", pa.string(), nullable=False),  # Iceberg StringType() -> PyArrow string
    pa.field("name", pa.string(), nullable=True),  # Iceberg StringType() -> PyArrow string
    pa.field("city", pa.string(), nullable=True),  # Iceberg StringType() -> PyArrow string
    pa.field("amount", pa.float64(), nullable=False)  # Iceberg LongType() -> PyArrow int64
])

table_properties = {
    "commit.manifest.min-count-to-merge": 2
}


def fetch_events():
    return EventGenerator().generate_events(BATCH_SIZE)


def write_data(events, table_name):
    df = pd.DataFrame(events)
    pa_table = pa.Table.from_pandas(df, arrow_schema)
    if not catalog.table_exists(table_name):
        catalog.create_table(identifier=table_name, location=table_location, schema=iceberg_schema,
                             partition_spec=partition_spec,
                             properties=table_properties)
    iceberg_table = catalog.load_table(table_name)
    iceberg_table.append(pa_table)
    print(iceberg_table.scan().to_pandas())


def main():
    while True:
        try:
            events = fetch_events()
            write_data(events, "default.events")
        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(POLLING_INTERVAL)
        break


if __name__ == "__main__":
    main()
