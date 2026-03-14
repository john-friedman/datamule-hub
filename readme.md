# Datamule Hub

Python package to use [datamule.xyz](https://datamule.xyz/) endpoints.

> Currently moving over the cloud functionality from [datamule-python](https://github.com/john-friedman/datamule-python) to this repository, to keep datamule-python dependency light. 

## Installation

```
pip install datamule_hub
```


## Functions

- query_database
- download_dataset
- bucket_transfer (transfer objects in a bucket to your cloud)
- dataset_transfer (transfer datasets to your cloud)

## Supported Providers

Current supported methods. Subject to change.

- AWS S3 via `aioboto3` — authenticate via S3 credentials
- GCP Cloud Storage via service account credentials

## Quickstart

1. Set `DATAMULE_API_KEY` in your environment
2. read [examples](examples/)

## Docs

Tbd.