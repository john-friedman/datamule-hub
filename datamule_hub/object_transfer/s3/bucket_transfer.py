import asyncio
import aiohttp
import aioboto3
import ssl
import json
from urllib.parse import urlparse
from tqdm import tqdm
from datetime import datetime, timezone

from ..utils import _generate_dates, _get_urls

async def _transfer_file(session, s3_client, semaphore, url, bucket, prefix=None, retry_errors=3):
    async with semaphore:
        key = urlparse(url).path.split('/')[-1]
        if prefix:
            key = f"{prefix.rstrip('/')}/{key}"
        last_error = None
        for attempt in range(retry_errors + 1):
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
                    content = await response.read()
                    await s3_client.put_object(
                        Bucket=bucket, Key=key, Body=content,
                        ContentType=response.headers.get('Content-Type', 'application/octet-stream'),
                        Metadata={'source-url': url, 'transfer-date': datetime.now(timezone.utc).isoformat()}
                    )
                    return {'success': True, 'url': url, 'size_bytes': len(content)}
            except Exception as e:
                if attempt < retry_errors:
                    await asyncio.sleep(2 ** attempt)
                last_error = e
        return {'success': False, 'url': url, 'error': str(last_error)}


async def _transfer_urls(urls, s3_credentials, max_workers, retry_errors, prefix=None):
    connector = aiohttp.TCPConnector(limit=max_workers, ssl=ssl.create_default_context(), ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=600)) as session:
        async with aioboto3.Session().client(
            's3',
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key'],
            region_name=s3_credentials['region_name']
        ) as s3_client:
            semaphore = asyncio.Semaphore(max_workers)
            tasks = [_transfer_file(session, s3_client, semaphore, url, s3_credentials['bucket_name'], prefix, retry_errors) for url in urls]

            failed, total_bytes = [], 0
            with tqdm(total=len(urls), desc="Transferring files", unit="file") as pbar:
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    if result['success']:
                        total_bytes += result['size_bytes']
                    else:
                        failed.append(result)
                    pbar.set_postfix({'Total': f"{total_bytes / (1024**3):.2f} GB"})
                    pbar.update(1)

            return failed


def bucket_transfer(datamule_bucket, s3_credentials, max_workers=4, errors_json_filename='errors_bucket_transfer.json',
                retry_errors=3, force_daily=True, cik=None, submission_type=None, filing_date=None, accession_number=None, prefix=None):

    if datamule_bucket not in ['filings_sgml_r2', 'sec_filings_sgml_r2']:
        raise ValueError('Datamule S3 bucket not found.')

    if accession_number is not None and any(p is not None for p in [cik, submission_type, filing_date]):
        raise ValueError('If accession_number is provided, cik, submission_type, and filing_date must be None.')

    dates = _generate_dates(filing_date) if force_daily and filing_date else [filing_date]

    for date in dates:
        if len(dates) > 1:
            print(f"Transferring {date}")
        urls = _get_urls(submission_type=submission_type, cik=cik, filing_date=date, accession_number=accession_number)
        failed = asyncio.run(_transfer_urls(urls, s3_credentials, max_workers, retry_errors, prefix=prefix))

        if failed and errors_json_filename:
            with open(errors_json_filename, 'w') as f:
                json.dump(failed, f, indent=2)
            print(f"Saved {len(failed)} errors to {errors_json_filename}")

        print(f"Transfer complete: {len(urls) - len(failed)}/{len(urls)} files successful")