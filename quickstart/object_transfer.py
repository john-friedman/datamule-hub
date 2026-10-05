import logging

from datamulehub import object_transfer


logging.basicConfig(level=logging.INFO)


gcs_credentials = {
    'bucket_name': 'your-bucket',
    'service_file': 'service_account.json' # Optional. Can also do (gcloud auth application-default login), in which case remove this.
}

# transfer filings
object_transfer.gcs_archive_transfer(
    datamule_bucket='sec_filings_sgml_r2',
    gcs_credentials=gcs_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2020-01-01', '2024-01-01'),
    force_daily=True,
    prefix = ""
)
input("Proceed?")

# transfer datasets
object_transfer.gcs_dataset_transfer(
    datasets=['xml2tables/dos'],
    gcs_credentials=gcs_credentials,
    prefix = ""
)
input("Proceed?")

s3_credentials = {
    's3_provider': 'aws',
    'aws_access_key_id': 'YOUR_KEY',
    'aws_secret_access_key': 'YOUR_SECRET',
    'region_name': 'us-east-1',
    'bucket_name': 'your-bucket'
}

object_transfer.s3_archive_transfer(
    datamule_bucket='sec_filings_sgml_r2',
    s3_credentials=s3_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2001-01-01','2024-01-01'),
    force_daily=False,
    prefix = ""
)
input("Proceed?")

# transfer datasets
object_transfer.s3_dataset_transfer(
    datasets=['xml2tables/dos'],
    s3_credentials=s3_credentials,
    prefix = ""
)
