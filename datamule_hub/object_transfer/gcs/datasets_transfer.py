import asyncio
import aiohttp
import ssl
import json
import urllib
from tqdm import tqdm
from datetime import datetime, timezone
from gcloud.aio.storage import Storage

from ...api_key import api_key
from ...datasets import DATASET_NAME_MAP
from .utils import _get_storage



async def _get_dataset_url(session, dataset_name):
    api_url = f"https://api.datamule.xyz/dataset/{urllib.parse.quote(dataset_name)}?api_key={api_key}"
    async with session.get(api_url, headers={'User-Agent': 'datamule-python'}) as response:
        data = await response.json()
    if not data.get('success'):
        raise Exception(f"API error: {data.get('error', 'Unknown error')}")
    billing = data.get('metadata', {}).get('billing', {})
    return data['data']['download_url'], data['data']['size_gb'], billing


async def _transfer_dataset(session, storage, semaphore, dataset, bucket, prefix=None, retry_errors=3):
    dataset_name = DATASET_NAME_MAP.get(dataset)
    if not dataset_name:
        return {'success': False, 'dataset': dataset, 'error': f"Unknown dataset: {dataset}"}

    download_url, size_gb, billing = await _get_dataset_url(session, dataset_name)

    parsed = urllib.parse.urlparse(download_url)
    path = urllib.parse.parse_qs(parsed.query).get('path', [''])[0]
    key = urllib.parse.unquote(path.split('/')[-1]) or f"{dataset}.download"
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    async with semaphore:
        last_error = None
        for attempt in range(retry_errors + 1):
            try:
                async with session.get(download_url) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
                    content = await response.read()
                    await storage.upload(bucket, key, content,
                        content_type=response.headers.get('Content-Type', 'application/octet-stream'),
                        metadata={'source-url': download_url, 'transfer-date': datetime.now(timezone.utc).isoformat()}
                    )
                    return {'success': True, 'dataset': dataset, 'size_bytes': len(content), 'billing': billing}
            except Exception as e:
                if attempt < retry_errors:
                    download_url, _, _ = await _get_dataset_url(session, dataset_name)
                    await asyncio.sleep(2 ** attempt)
                last_error = e
        return {'success': False, 'dataset': dataset, 'error': str(last_error)}


async def _transfer_datasets(datasets, gcs_credentials, max_workers, retry_errors, prefix=None):
    connector = aiohttp.TCPConnector(limit=max_workers, ssl=ssl.create_default_context())
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=600)) as session:
        async with _get_storage(gcs_credentials, session) as storage:
            semaphore = asyncio.Semaphore(max_workers)
            tasks = [_transfer_dataset(session, storage, semaphore, d, gcs_credentials['bucket_name'], prefix, retry_errors) for d in datasets]

            failed, total_bytes = [], 0
            with tqdm(total=len(datasets), desc="Transferring datasets", unit="dataset") as pbar:
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    if result['success']:
                        total_bytes += result['size_bytes']
                    else:
                        failed.append(result)
                    pbar.set_postfix({'Total': f"{total_bytes / (1024**3):.2f} GB"})
                    pbar.update(1)

            return failed


def datasets_transfer(datasets, gcs_credentials, max_workers=4, errors_json_filename='errors_datasets_transfer.json', retry_errors=3, prefix=None):
    failed = asyncio.run(_transfer_datasets(datasets, gcs_credentials, max_workers, retry_errors, prefix=prefix))

    if failed and errors_json_filename:
        with open(errors_json_filename, 'w') as f:
            json.dump(failed, f, indent=2)
        print(f"Saved {len(failed)} errors to {errors_json_filename}")

    print(f"Transfer complete: {len(datasets) - len(failed)}/{len(datasets)} datasets successful")