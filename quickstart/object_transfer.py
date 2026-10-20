import logging

from datamulehub import object_transfer


logging.basicConfig(level=logging.INFO)


gcs_credentials = {
    'bucket_name': 'your-bucket',
    'service_file': 'service_account.json' # Optional. Can also do (gcloud auth application-default login), in which case remove this.
}

# transfer filings
object_transfer.transfer_sgml_archive_gcs(
    gcs_credentials=gcs_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2020-01-01', '2024-01-01'),
    prefix = ""
)
input("Proceed?")

# transfer datasets
object_transfer.transfer_dataset_gcs(
    datasets=['xml2tables/dos'],
    gcs_credentials=gcs_credentials,
    prefix = ""
)
input("Proceed?")

s3_credentials = {
    'aws_access_key_id': 'YOUR_KEY',
    'aws_secret_access_key': 'YOUR_SECRET',
    'region_name': 'us-east-1',
    'bucket_name': 'your-bucket'
}

object_transfer.transfer_sgml_archive_s3(
    s3_credentials=s3_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2001-01-01','2024-01-01'),
    prefix = ""
)
input("Proceed?")

# transfer datasets
object_transfer.transfer_dataset_s3(
    datasets=['xml2tables/dos'],
    s3_credentials=s3_credentials,
    prefix = ""
)

# Transfer whole submission TARs instead of SGML.
input("Proceed with TAR transfer?")
object_transfer.transfer_tar_archive_s3(
    s3_credentials=s3_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2020-01-01', '2024-01-01'),
    prefix="sec-filings",
)

# Transfer full XBRL facts (.json.zst).
input("Proceed with XBRL transfer?")
object_transfer.transfer_xbrl_archive_s3(
    s3_credentials=s3_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2020-01-01', '2024-01-01'),
    prefix="sec-filings",
)
