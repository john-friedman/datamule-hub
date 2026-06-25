from datamulehub import object_transfer

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
