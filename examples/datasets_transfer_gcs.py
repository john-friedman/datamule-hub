from datamulehub import object_transfer

gcs_credentials = {
    'bucket_name': 'your-bucket',
    'service_file': 'service_account.json' # Optional. Can also do (gcloud auth application-default login), in which case remove this.
}

object_transfer.gcs_dataset_transfer(
    datasets=['simple_xbrl', 'xml2tables/dos'],
    gcs_credentials=gcs_credentials,
    prefix = ""
)
