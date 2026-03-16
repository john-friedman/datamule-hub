import asyncio
import aiohttp
import aioboto3
import ssl
import json
import urllib
from tqdm import tqdm
from datetime import datetime, timezone

from ...api_key import api_key
from ...datasets import DATASET_NAME_MAP


async def _get_dataset_url(session, dataset_name):
    api_url = f"https://api.datamule.xyz/dataset/{urllib.parse.quote(dataset_name)}?api_key={api_key}"
    async with session.get(api_url, headers={'User-Agent': 'datamule-python'}) as response:
        data = await response.json()
    if not data.get('success'):
        raise Exception(f"API error: {data.get('error', 'Unknown error')}")
    billing = data.get('metadata', {}).get('billing', {})
    return data['data']['download_url'], data['data']['size_gb'], billing


async def _transfer_dataset(session, s3_client, semaphore, dataset, bucket, prefix=None, retry_errors=3, multipart_threshold_mb=100, chunk_size_mb=8):
    dataset_name = DATASET_NAME_MAP.get(dataset)
    if not dataset_name:
        return {'success': False, 'dataset': dataset, 'error': f"Unknown dataset: {dataset}"}

    download_url, size_gb, billing = await _get_dataset_url(session, dataset_name)

    parsed = urllib.parse.urlparse(download_url)
    path = urllib.parse.parse_qs(parsed.query).get('path', [''])[0]
    key = urllib.parse.unquote(path.split('/')[-1]) or f"{dataset}.download"
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    use_multipart = size_gb * 1024 > multipart_threshold_mb
    chunk_size = chunk_size_mb * 1024 * 1024

    async with semaphore:
        last_error = None
        for attempt in range(retry_errors + 1):
            mpu_id = None
            try:
                async with session.get(download_url) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)

                    if use_multipart:
                        mpu = await s3_client.create_multipart_upload(
                            Bucket=bucket, Key=key,
                            ContentType=response.headers.get('Content-Type', 'application/octet-stream'),
                            Metadata={'source-url': download_url, 'transfer-date': datetime.now(timezone.utc).isoformat()}
                        )
                        mpu_id = mpu['UploadId']
                        parts = []
                        part_number = 1
                        buffer = b""

                        async for chunk in response.content.iter_chunked(chunk_size):
                            buffer += chunk
                            if len(buffer) >= chunk_size:
                                part = await s3_client.upload_part(
                                    Bucket=bucket, Key=key,
                                    PartNumber=part_number, UploadId=mpu_id, Body=buffer
                                )
                                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                                part_number += 1
                                buffer = b""

                        if buffer:
                            part = await s3_client.upload_part(
                                Bucket=bucket, Key=key,
                                PartNumber=part_number, UploadId=mpu_id, Body=buffer
                            )
                            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})

                        await s3_client.complete_multipart_upload(
                            Bucket=bucket, Key=key,
                            UploadId=mpu_id, MultipartUpload={'Parts': parts}
                        )

                    else:
                        content = await response.read()
                        await s3_client.put_object(
                            Bucket=bucket, Key=key, Body=content,
                            ContentType=response.headers.get('Content-Type', 'application/octet-stream'),
                            Metadata={'source-url': download_url, 'transfer-date': datetime.now(timezone.utc).isoformat()}
                        )

                    return {'success': True, 'dataset': dataset, 'size_bytes': int(size_gb * 1024**3), 'billing': billing}

            except Exception as e:
                if mpu_id:
                    try:
                        await s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu_id)
                    except Exception:
                        pass
                if attempt < retry_errors:
                    download_url, _, _ = await _get_dataset_url(session, dataset_name)
                    await asyncio.sleep(2 ** attempt)
                last_error = e

        return {'success': False, 'dataset': dataset, 'error': str(last_error)}


async def _transfer_datasets(datasets, s3_credentials, max_workers, retry_errors, prefix=None):
    connector = aiohttp.TCPConnector(limit=max_workers, ssl=ssl.create_default_context())
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=7200)) as session:
        async with aioboto3.Session().client(
            's3',
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key'],
            region_name=s3_credentials['region_name']
        ) as s3_client:
            semaphore = asyncio.Semaphore(max_workers)
            tasks = [_transfer_dataset(session, s3_client, semaphore, d, s3_credentials['bucket_name'], prefix, retry_errors) for d in datasets]

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


def datasets_transfer(datasets, s3_credentials, max_workers=4, errors_json_filename='errors_datasets_transfer.json', retry_errors=3, prefix=None):
    failed = asyncio.run(_transfer_datasets(datasets, s3_credentials, max_workers, retry_errors, prefix=prefix))

    if failed and errors_json_filename:
        with open(errors_json_filename, 'w') as f:
            json.dump(failed, f, indent=2)
        print(f"Saved {len(failed)} errors to {errors_json_filename}")

    print(f"Transfer complete: {len(datasets) - len(failed)}/{len(datasets)} datasets successful")