# Datasets

`datasets.download` downloads v3 S3-gated dataset objects.

```python
from datamulehub import datasets

datasets.download("simple_xbrl")
datasets.download("xml2tables/dos", filename="dos.parquet")
datasets.download("xml2tables/dos", from_storage="both")
datasets.download("xml2tables/dos", from_storage="express")
datasets.download("xml2tables/dos", from_storage="standard")
datasets.download("datasets/xml2tables/d/data.parquet")
datasets.download("metadata/submissions_metadata/data.parquet")
```

For XML tables, `from_storage="both"` is the default. It downloads the published Parquet dataset while looking up Express filing accessions, then combines the files. Files already present in the published dataset are removed by source key or filing date and accession before the merge. `from_storage="express"` downloads only Express files; `from_storage="standard"` downloads only the published dataset. For other dataset types, `both` behaves like `standard` and `express` is unavailable.

Express lookup uses Eastern Time and narrows filings to the XML document types that can produce the requested table, using a map generated from secinfrarust. Before 2:30 a.m., it checks yesterday's filing date. From 2:30 a.m. to 6:30 a.m., it checks no Express files while the daily dataset takes over. From 6:30 a.m. onward, it checks today's filing date. This window also applies to `from_storage="express"`; when there are no Express files, that mode writes an empty Parquet file containing only the `_sourceKey` column. A missing Express file is tried once more after three seconds; filings that do not produce the requested XML table are skipped. The Express progress bar shows completed checks and the number of files downloaded.

Each Express file that is downloaded uses a one-use link and is charged at the normal S3 download rate. The returned `cost` includes the links used by the selected download mode.

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
