import asyncio
from contextlib import suppress
from datetime import datetime, timezone

import aioboto3
import aiohttp
from tqdm import tqdm

from ..archives import _destination_key, _transfer_archive
from .datasets_transfer import _get_dataset_url


CHUNK_SIZE = 8 * 1024 * 1024


async def _upload_response(response, client, bucket, key, url):
    """Stream large objects through multipart upload with bounded memory."""
    options = {
        "Bucket": bucket,
        "Key": key,
        "ContentType": response.headers.get("Content-Type", "application/octet-stream"),
        "Metadata": {"source-url": url, "transfer-date": datetime.now(timezone.utc).isoformat()},
    }
    upload_id = None
    parts = []
    total_bytes = 0
    buffer = bytearray()
    # Keep even very large objects below S3's 10,000-part limit.
    size = int(response.headers.get("Content-Length", 0))
    chunk_size = max(CHUNK_SIZE, (size + 9999) // 10000)

    async def upload_part(content):
        nonlocal upload_id
        if upload_id is None:
            upload = await client.create_multipart_upload(**options)
            upload_id = upload["UploadId"]
        part_number = len(parts) + 1
        part = await client.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id,
            PartNumber=part_number, Body=content,
        )
        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

    try:
        async for chunk in response.content.iter_chunked(chunk_size):
            total_bytes += len(chunk)
            buffer.extend(chunk)
            if len(buffer) >= chunk_size:
                await upload_part(bytes(buffer))
                buffer.clear()
        if "Content-Length" in response.headers and total_bytes != size:
            raise IOError(f"Incomplete source download: expected {size} bytes, received {total_bytes}.")
        if upload_id is None:
            await client.put_object(**options, Body=bytes(buffer))
        else:
            if buffer:
                await upload_part(bytes(buffer))
            await client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts},
            )
        return total_bytes
    except BaseException:
        if upload_id is not None:
            with suppress(Exception):
                await client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


async def _transfer_object(session, client, semaphore, item, bucket, retry_errors, prefix, api_key):
    key = _destination_key(item, prefix)
    async with semaphore:
        for attempt in range(retry_errors + 1):
            try:
                if "path" in item:
                    url, _, _, _ = await _get_dataset_url(session, item["path"], api_key=api_key)
                else:
                    url = item["url"]
                async with session.get(url) as response:
                    response.raise_for_status()
                    size = await _upload_response(response, client, bucket, key, url)
                return {"success": True, "key": key, "size_bytes": size}
            except Exception as exc:
                if attempt == retry_errors:
                    return {"success": False, "key": key, "source": item, "error": str(exc)}
                await asyncio.sleep(2 ** attempt)


async def _transfer_objects(items, credentials, max_workers, retry_errors, prefix, api_key):
    connector = aiohttp.TCPConnector(limit=max_workers)
    async with aiohttp.ClientSession(
        connector=connector, timeout=aiohttp.ClientTimeout(total=7200),
        auto_decompress=False,
        headers={"User-Agent": "datamule-hub", "Accept-Encoding": "identity"},
    ) as session:
        async with aioboto3.Session().client(
            "s3", aws_access_key_id=credentials["aws_access_key_id"],
            aws_secret_access_key=credentials["aws_secret_access_key"],
            region_name=credentials["region_name"],
        ) as client:
            semaphore = asyncio.Semaphore(max_workers)
            tasks = [
                _transfer_object(session, client, semaphore, item, credentials["bucket_name"],
                                 retry_errors, prefix, api_key)
                for item in items
            ]
            results = []
            with tqdm(total=len(tasks), desc="Transferring archive files", unit="file") as progress:
                for task in asyncio.as_completed(tasks):
                    results.append(await task)
                    progress.update(1)
            return results


def _transfer_batch(*args):
    return asyncio.run(_transfer_objects(*args))


def transfer_sgml_archive(s3_credentials, **kwargs):
    """Copy stored .sgml.zst filings; accepts filing lookup filters and transfer options."""
    return _transfer_archive("sgml", _transfer_batch, s3_credentials, **kwargs)


def transfer_tar_archive(s3_credentials, **kwargs):
    """Copy whole submission TARs; document filters select filings, not TAR members."""
    return _transfer_archive("tar", _transfer_batch, s3_credentials, **kwargs)


def transfer_xbrl_archive(s3_credentials, **kwargs):
    """Copy stored XBRL .json.zst files; accepts download_xbrl filing filters."""
    return _transfer_archive("xbrl", _transfer_batch, s3_credentials, **kwargs)

