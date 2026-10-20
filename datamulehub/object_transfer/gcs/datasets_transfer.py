import json
import urllib
import urllib.request
from tqdm import tqdm

from ...api_key import get_api_key
from ...v3.datasets import resolve_path
from .utils import _get_gcs_client, _upload_response


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


def _transfer_dataset(client, bucket, dataset, prefix=None, retry_errors=3, api_key=None):
    download_url, size_gb, billing, object_key = _get_dataset_url(dataset, api_key=api_key)

    key = object_key.strip("/") or f"{dataset}.download"
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    last_error = None
    for attempt in range(retry_errors + 1):
        try:
            request = urllib.request.Request(download_url, headers={'User-Agent': 'datamule-python'})
            with urllib.request.urlopen(request) as response:
                size = _upload_response(bucket, key, response, download_url)
                return {'success': True, 'dataset': dataset, 'size_bytes': size, 'billing': billing}
        except Exception as e:
            if attempt < retry_errors:
                download_url, _, _, _ = _get_dataset_url(dataset, api_key=api_key)
            last_error = e

    return {'success': False, 'dataset': dataset, 'error': str(last_error)}


def transfer_dataset(datasets, gcs_credentials, errors_json_filename='errors_datasets_transfer.json', retry_errors=3, prefix=None, api_key=None):
    client = _get_gcs_client(gcs_credentials)
    bucket = client.bucket(gcs_credentials['bucket_name'])

    failed, total_bytes = [], 0
    with tqdm(total=len(datasets), desc="Transferring datasets", unit="dataset") as pbar:
        for dataset in datasets:
            result = _transfer_dataset(client, bucket, dataset, prefix, retry_errors, api_key=api_key)
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
