# Datasets

`datasets.download` downloads v3 S3-gated dataset objects.

```python
from datamulehub import datasets


datasets.download("simple_xbrl")  # both: daily dataset plus recent filings
datasets.download("simple_xbrl", from_storage="daily")
datasets.download("simple_xbrl", from_storage="delta")
datasets.download(
    "simple_xbrl",
    accession=["0000320193-24-000123"],
    filename="filing_simple_xbrl.parquet",
)
datasets.download("xml2tables/dos", filename="dos.parquet")
datasets.download("xml2tables/dos", from_storage="daily")
datasets.download("xml2tables/dos", from_storage="delta")
datasets.download("datasets/xml2tables/d/data.parquet")
datasets.download("metadata/submissions_metadata/data.parquet")
datasets.download("filer_metadata", filename="filer_metadata.parquet")
datasets.download("filer_names", filename="filer_names.parquet")
```

For XML tables and simple XBRL, `from_storage="both"` is the default. It downloads the published daily Parquet dataset and recent per-filing files, then merges them. Files already present in the daily dataset are removed by `_sourceKey` or filing date and accession before the merge. `from_storage="daily"` downloads only the published dataset. `from_storage="delta"` downloads only per-filing files for the selected filing date and may include files already in the daily dataset. For other dataset types, `both` behaves like `daily` and `delta` is unavailable.

Delta lookup uses Eastern Time. Before 2:30 a.m., it checks yesterday's filing date. From 2:30 a.m. to 6:30 a.m., it checks no per-filing files while the daily dataset takes over. From 6:30 a.m. onward, it checks today's filing date. XML table lookup narrows filings to document types that can produce the requested table, using a map generated from secinfrarust. Simple XBRL lookup uses the XBRL availability flag. Missing per-filing files are tried once more after three seconds, then skipped.

When there are no per-filing files, `delta` writes an empty Parquet file containing only the `_sourceKey` column. The progress bar shows completed checks and the number of files downloaded. XML table deltas are stored in S3 Express; simple XBRL deltas are stored in standard S3. Each downloaded file uses a one-use link and is charged at the normal S3 download rate. The returned `cost` includes the links used by the selected mode.

Accepted dataset names:

- exact S3 object keys, such as `datasets/simple_xbrl/data.parquet`
- XML table shorthand, such as `dos` or `xml2tables/dos`
- metadata aliases, such as `sec_master_submissions`, `filer_metadata`, and `filer_names`
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

## Simple XBRL by accession

Supply `accession` as a non-empty list, including for one filing. List items may
be dashed strings, undashed strings, or integer accession numbers. Duplicate
accessions are removed. Scalars, tuples, empty lists, and invalid accession
numbers are rejected before lookup or download.

The client resolves filing dates and downloads the individual simple XBRL Parquet
objects for those filings, then combines them into one output file. This supports
historical filings and is independent of the daily/delta time window. This mode
does not download the combined dataset and cannot use `from_storage="daily"`.
The default `both` and explicit `delta` both select per-filing objects in this mode.

The return value also includes `accession` (normalized accession numbers),
`object_keys` (downloaded per-filing keys), and `missing_accessions`. `object_key`
is None because the result combines individual objects. If no files are available,
the result is an empty Parquet file with the simple XBRL schema.
