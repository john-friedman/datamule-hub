from datamule_hub.object_transfer.s3.datasets_transfer import datasets_transfer

s3_credentials = {
    's3_provider': 'aws',
    'aws_access_key_id': 'YOUR_KEY',
    'aws_secret_access_key': 'YOUR_SECRET',
    'region_name': 'us-east-1',
    'bucket_name': 'your-bucket'
}

datasets_transfer(
    datasets=['sec_accessions','sec_accession_cik_table'],
    s3_credentials=s3_credentials
)