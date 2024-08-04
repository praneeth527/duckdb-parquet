Install required dependencies
```shell
pip install -r requirements.tx
```

Install duckdb-engine package in superset to enable duckdb
```shell
pip install duckdb-engine
```

Specify the SQLAlchemy URI as following
```shell
duckdb:///:memory:
```

Query the events using read_parquet function
```shell
select * from read_parquet('full_path_to_events_folder/data/events/*/*.parquet', hive_partitioning=true)
```