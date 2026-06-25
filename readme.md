# Datamule Hub

Python package for Datamule cloud APIs.

## Installation

```bash
pip install datamule-hub
```

Set your API key:

```bash
export DATAMULE_API_KEY="..."
```

## Datasets

```python
from datamulehub import datasets

datasets.download("simple_xbrl")
datasets.download("xml2tables/dos", filename="dos.parquet")
datasets.download("metadata/submissions_metadata/data.parquet")
```

Dataset downloads use the v3 S3-gated API.

## Athena Queries

Download query results as a folder of Parquet parts:

```python
from datamulehub import databases

result = databases.query(
    "SELECT * FROM simple_xbrl LIMIT 10",
    output_dir="simple_xbrl_sample",
)
print(result)
```

Read the full query result into Python rows:

```python
from datamulehub import databases

rows = databases.read_query("SELECT * FROM simple_xbrl LIMIT 10")
print(rows[0])
```

## Archive Helpers

```python
from datamulehub import sec_filings_archive

sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="out")
sec_filings_archive.download_tar(cik=320193, document_type=["10-K", "10-Q"], output_dir="out")
```

## Object Transfer

Object transfer helpers can copy Datamule results into your S3 or GCS bucket. See `examples/`.
