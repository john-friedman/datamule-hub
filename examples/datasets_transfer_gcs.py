from datamule_hub.object_transfer.gcs.datasets_transfer import datasets_transfer

gcs_credentials = {
    'bucket_name': 'your-bucket',
    'service_file': 'service_account.json' # Optional. Can also do (gcloud auth application-default login), in which case remove this.
}

datasets_transfer(
    datasets=['sec_accessions', 'sec_master_submissions'],
    gcs_credentials=gcs_credentials,
    prefix = ""
)