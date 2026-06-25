# Object Transfer

Object transfer helpers copy Datamule data into your own cloud bucket.

Dataset transfer uses the v3 S3-gated dataset API.

```python
from datamulehub import object_transfer

s3_credentials = {
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "region_name": "us-east-1",
    "bucket_name": "your-bucket",
}

object_transfer.s3_dataset_transfer(
    datasets=["simple_xbrl", "xml2tables/dos"],
    s3_credentials=s3_credentials,
    prefix="datamule",
)
```

Bucket transfer can copy selected SEC filing archive objects.

```python
from datamulehub import object_transfer

object_transfer.s3_archive_transfer(
    datamule_bucket="sec_filings_sgml_r2",
    s3_credentials=s3_credentials,
    cik=320193,
    submission_type="10-K",
    filing_date=("2024-01-01", "2024-12-31"),
    prefix="sec-filings",
)
```

Internally, filing lookups use v3 Athena results read with `pyarrow`.
