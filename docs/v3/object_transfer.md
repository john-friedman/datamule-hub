# Transfer to your Cloud

```python
from datamulehub import object_transfer
```

## Credentials

### S3

```python
s3_credentials = {
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "region_name": "us-east-1",
    "bucket_name": "your-bucket",
}
```

### Google Cloud Storage

```python
gcs_credentials = {
    "bucket_name": "your-bucket",
    "service_file": "service_account.json",
}
```

Omit `service_file` to use Application Default Credentials, for example after
running `gcloud auth application-default login`.

Set `DATAMULE_API_KEY` for Datamule access, or pass `api_key` explicitly to any
dataset or archive transfer function.

## Updating Daily

### Datasets

Datasets update both instantly and daily. The daily dataset takes up less space.
Daily updates run at 06:00 UTC Tuesday–Saturday (1–2 a.m. Eastern Time, depending
on daylight saving time). For daily refreshes, pull after 09:00 UTC; pulling after
06:15 UTC should normally be fine.

Transfer helpers copy the published daily Parquet objects without merging recent
per-filing updates. They run once per call; use your own scheduler for recurring
transfers. Dataset names such as `simple_xbrl`, `xml2tables/dos`, and `xmltables/345`
are supported.

```python
object_transfer.transfer_dataset_s3(
    datasets=["simple_xbrl", "xml2tables/dos"],
    s3_credentials=s3_credentials,
    prefix="datamule",
)
```

Dataset object keys are preserved under `prefix`, for example
`datamule/datasets/simple_xbrl/data.parquet`.

### Archives

Archives update instantly. Filing filters are similar to
[downloading filings](sec_filings_archive.md): `cik`, `ticker`, `accession`,
`submission_type`, `filing_date`, `report_date`, and `detected_time`, with the same
date-range and pagination options. XBRL `accession` must be a non-empty list,
including for one filing. Only filings containing XBRL are selected for XBRL
transfers.

```python
object_transfer.transfer_sgml_archive_s3(
    s3_credentials=s3_credentials,
    filing_date="2024-01-02",
    prefix="sec-filings",
)
```

```python
object_transfer.transfer_tar_archive_s3(
    s3_credentials=s3_credentials,
    filing_date="2024-01-02",
    prefix="sec-filings",
)
```

```python
object_transfer.transfer_xbrl_archive_s3(
    s3_credentials=s3_credentials,
    filing_date="2024-01-02",
    prefix="sec-filings",
)
```

Archive transfers copy stored objects unchanged, without local staging,
decompression, or repacking. Their destination keys are:

| Archive | Key under `prefix` |
| --- | --- |
| SGML | `filingDate/accession.sgml.zst` |
| TAR | `filingDate/accession.tar` |
| XBRL | `filingDate/accession.json.zst` |

TAR transfers copy whole submission archives. SGML and TAR accept `contains_xbrl`,
`document_type`, `filename`, and `sequence` to select matching filings; these
filters do not extract individual TAR members. Download-specific output options
such as `output_dir`, `decompress`, and `tar_max_size_mb` do not apply.

Archive transfers accept `max_workers` (default 4), `retry_errors` (default 3),
and `errors_json_filename` (default `errors_archive_transfer.json`). Failed
objects from all batches are saved together; set `errors_json_filename=None` to
disable the error file. Transfers overwrite matching destination keys.

## Google Cloud Storage transfers

Use the matching `_gcs` functions with `gcs_credentials`:

```python
object_transfer.transfer_dataset_gcs(
    datasets=["simple_xbrl", "xml2tables/dos"],
    gcs_credentials=gcs_credentials,
    prefix="datamule",
)

object_transfer.transfer_sgml_archive_gcs(
    gcs_credentials=gcs_credentials,
    filing_date="2024-01-02",
    prefix="sec-filings",
)
```

`transfer_tar_archive_gcs` and `transfer_xbrl_archive_gcs` accept the same archive
options as their S3 counterparts.
