from datamule_hub.object_transfer.gcs.bucket_transfer import bucket_transfer


gcs_credentials = {
    'bucket_name': 'your-bucket',
    'service_file': 'service_account.json'
}

bucket_transfer(
    datamule_bucket='sec_filings_sgml_r2',
    gcs_credentials=gcs_credentials,
    cik=320193,
    submission_type='10-K',
    filing_date=('2020-01-01', '2024-01-01')
)
