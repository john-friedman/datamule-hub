# Datasets

`download_dataset` downloads v3 S3-gated dataset objects.

```python
from datamulehub import datasets

datasets.download("simple_xbrl")
datasets.download("xml2tables/dos", filename="dos.parquet")
datasets.download("datasets/xml2tables/d/data.parquet")
datasets.download("metadata/submissions_metadata/data.parquet")
```

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
