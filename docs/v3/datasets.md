# Datasets

`download_dataset` downloads v3 S3-gated dataset objects.

```python
from datamulehub import datasets

datasets.download("simple_xbrl")
datasets.download("xml2tables/dos", filename="dos.parquet")
datasets.download("datasets/xml2tables/d/data.parquet")
datasets.download("metadata/submissions_metadata/data.parquet")
```

XML table downloads include recently processed filings from S3 Express by default. The package downloads the published Parquet dataset, finds newer filing candidates, and merges Express files that are not already in the dataset. A missing Express file is tried once more after three seconds; filings that do not produce that XML table are skipped. Set `include_live=False` to download only the published dataset.

Each Express file that is downloaded uses a one-use link and is charged at the normal S3 download rate. The returned `cost` includes the published dataset and all Express file links, and `live_files` reports how many files were merged.

Accepted dataset names:

- exact S3 object keys, such as `datasets/simple_xbrl/data.parquet`
- XML table shorthand, such as `dos` or `xml2tables/dos`
- metadata aliases, such as `sec_master_submissions`
- `simple_xbrl`

The function returns metadata:

```python
{
    "filename": "data.parquet",
    "object_key": "datasets/simple_xbrl/data.parquet",
    "size_bytes": 123,
    "size_gb": 0.000000123,
    "cost": 0.001,
    "remaining_balance": 99.99,
}
```
