import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from ...v3.datasets import get_link
from ..archives import _destination_key, _transfer_archive
from .utils import _get_gcs_client, _upload_response


def _transfer_object(bucket, item, retry_errors, prefix, api_key):
    key = _destination_key(item, prefix)
    for attempt in range(retry_errors + 1):
        try:
            url = get_link(item["path"], api_key=api_key)["download_url"] if "path" in item else item["url"]
            request = urllib.request.Request(url, headers={"User-Agent": "datamule-hub"})
            with urllib.request.urlopen(request, timeout=7200) as response:
                size = _upload_response(bucket, key, response, url)
            return {"success": True, "key": key, "size_bytes": size}
        except Exception as exc:
            if attempt == retry_errors:
                return {"success": False, "key": key, "source": item, "error": str(exc)}
            time.sleep(2 ** attempt)


def _transfer_batch(items, credentials, max_workers, retry_errors, prefix, api_key):
    with _get_gcs_client(credentials) as client:
        bucket = client.bucket(credentials["bucket_name"])
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_transfer_object, bucket, item, retry_errors, prefix, api_key)
                for item in items
            ]
            results = []
            with tqdm(total=len(futures), desc="Transferring archive files", unit="file") as progress:
                for future in as_completed(futures):
                    results.append(future.result())
                    progress.update(1)
            return results


def transfer_sgml_archive(gcs_credentials, **kwargs):
    """Copy stored .sgml.zst filings; accepts filing lookup filters and transfer options."""
    return _transfer_archive("sgml", _transfer_batch, gcs_credentials, **kwargs)


def transfer_tar_archive(gcs_credentials, **kwargs):
    """Copy whole submission TARs; document filters select filings, not TAR members."""
    return _transfer_archive("tar", _transfer_batch, gcs_credentials, **kwargs)


def transfer_xbrl_archive(gcs_credentials, **kwargs):
    """Copy stored XBRL .json.zst files; accepts download_xbrl filing filters."""
    return _transfer_archive("xbrl", _transfer_batch, gcs_credentials, **kwargs)

