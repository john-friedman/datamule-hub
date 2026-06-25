# Athena Queries

`query_database` runs SQL through the v3 Athena API and saves the result locally as Parquet part files.

```python
from datamulehub import databases

result = databases.query(
    "SELECT * FROM simple_xbrl LIMIT 10",
    output_dir="simple_xbrl_sample",
)
print(result)
```

Athena writes Parquet results. If the Worker transports multiple parts as TAR, `datamulehub` extracts them automatically. Users see a directory of `part-*.parquet` files.

The return value includes billing headers:

```python
{
    "output_dir": "simple_xbrl_sample",
    "files": [
        "simple_xbrl_sample/part-00000.parquet",
        "simple_xbrl_sample/part-00001.parquet"
    ],
    "query_id": "...",
    "cost": 0.001,
    "remaining_balance": 99.99,
    "billing": {
        "athena_scan": 0.001,
        "s3_download": 0.001,
        "total_charge": 0.002,
        "remaining_balance": 99.99,
    },
}
```

To read the full query result into Python rows, use `read_query`.

```python
from datamulehub import databases

rows = databases.read_query("SELECT * FROM simple_xbrl LIMIT 10")
print(rows[0])
```

`read_query` uses a temporary directory, downloads the Parquet result parts, reads them with `pyarrow`, and deletes the temporary files.
