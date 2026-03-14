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

- AWS S3 via `aioboto3` — authenticate via [IAM access keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)
- GCP Cloud Storage via `gcloud-aio-storage` — authenticate via service account JSON (`service_file`) or [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) (`gcloud auth application-default login`)

## Quickstart

1. Set `DATAMULE_API_KEY` in your environment
2. read [examples](examples/)

## Docs

Tbd.