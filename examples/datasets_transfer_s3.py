from datamulehub import object_transfer

s3_credentials = {
    's3_provider': 'aws',
    'aws_access_key_id': 'YOUR_KEY',
    'aws_secret_access_key': 'YOUR_SECRET',
    'region_name': 'us-east-1',
    'bucket_name': 'your-bucket'
}

object_transfer.s3_dataset_transfer(
    datasets=['simple_xbrl', 'xml2tables/dos'],
    s3_credentials=s3_credentials,
    prefix = ""
)
