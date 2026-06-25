import json
import urllib
import urllib.request
from tqdm import tqdm
from datetime import datetime, timezone
from google.cloud import storage as gcs

from ...api_key import get_api_key
from ...v3.datasets import resolve_path


def _get_dataset_url(dataset, api_key=None):
    object_key = resolve_path(dataset)
    body = json.dumps({"path": object_key}).encode("utf-8")
    request = urllib.request.Request(
        "https://api.datamule.xyz/v3/get-s3-link",
        data=body,
        headers={
            "Authorization": f"Bearer {get_api_key(api_key)}",
            "Content-Type": "application/json",
            "User-Agent": "datamule-hub",
        },
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        data = json.loads(response.read().decode('utf-8'))
    if not data.get('success'):
        raise Exception(f"API error: {data.get('error', 'Unknown error')}")
    billing = data.get('metadata', {}).get('billing', {})
    return data['data']['download_url'], data['data']['size_gb'], billing, data['data'].get('object_key', object_key)


def _get_gcs_client(gcs_credentials):
    if 'service_file' in gcs_credentials:
        return gcs.Client.from_service_account_json(gcs_credentials['service_file'])
    return gcs.Client()


def _transfer_dataset(client, bucket, dataset, prefix=None, retry_errors=3):
    download_url, size_gb, billing, object_key = _get_dataset_url(dataset)

    key = object_key.strip("/") or f"{dataset}.download"
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    last_error = None
    for attempt in range(retry_errors + 1):
        try:
            request = urllib.request.Request(download_url, headers={'User-Agent': 'datamule-python'})
            with urllib.request.urlopen(request) as response:
                content_type = response.headers.get('Content-Type', 'application/octet-stream')
                blob = bucket.blob(key)
                blob.metadata = {
                    'source-url': download_url,
                    'transfer-date': datetime.now(timezone.utc).isoformat(),
                }
                blob.upload_from_file(response, content_type=content_type)
                return {'success': True, 'dataset': dataset, 'size_bytes': blob.size, 'billing': billing}
        except Exception as e:
            if attempt < retry_errors:
                download_url, _, _, _ = _get_dataset_url(dataset)
            last_error = e

    return {'success': False, 'dataset': dataset, 'error': str(last_error)}


def datasets_transfer(datasets, gcs_credentials, errors_json_filename='errors_datasets_transfer.json', retry_errors=3, prefix=None):
    client = _get_gcs_client(gcs_credentials)
    bucket = client.bucket(gcs_credentials['bucket_name'])

    failed, total_bytes = [], 0
    with tqdm(total=len(datasets), desc="Transferring datasets", unit="dataset") as pbar:
        for dataset in datasets:
            result = _transfer_dataset(client, bucket, dataset, prefix, retry_errors)
            if result['success']:
                total_bytes += result.get('size_bytes', 0)
            else:
                failed.append(result)
            pbar.set_postfix({'Total': f"{total_bytes / (1024**3):.2f} GB"})
            pbar.update(1)

    if failed and errors_json_filename:
        with open(errors_json_filename, 'w') as f:
            json.dump(failed, f, indent=2)
        print(f"Saved {len(failed)} errors to {errors_json_filename}")

    print(f"Transfer complete: {len(datasets) - len(failed)}/{len(datasets)} datasets successful")
