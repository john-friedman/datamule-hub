import json
import logging
import math
import shutil
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import UUID

from tqdm import tqdm

from ...api_key import _format_api_key_credits, get_api_key


API_BASE_URL = "https://api.datamule.xyz"
logger = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, status, message, job_id=None, details=None):
        self.status = status
        self.job_id = job_id
        self.details = details
        super().__init__(f"API request failed ({status}): {message}")


class JobNotReadyError(ApiError):
    def __init__(self, job_id, retry_after=5, details=None):
        self.retry_after = retry_after
        super().__init__(202, f"Job {job_id} is not ready to download", job_id, details)


def _api_error(exc, job_id=None):
    body = exc.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(body)
        message = payload.get("error", body)
        details = payload.get("details")
    except json.JSONDecodeError:
        message, details = body, None
    finally:
        exc.close()
    return ApiError(exc.code, message, job_id, details)


def _request(path, api_key, accept="application/json", body=None):
    return urllib.request.Request(
        f"{API_BASE_URL}{path}",
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={
            "Accept": accept,
            "Authorization": f"Bearer {get_api_key(api_key)}",
            "Content-Type": "application/json",
            "User-Agent": "datamule-hub",
        },
        method="POST" if body is not None else "GET",
    )


def _request_json(path, api_key=None, timeout=30, body=None, job_id=None):
    request = _request(path, api_key, body=body)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise _api_error(exc, job_id) from exc
    if not payload.get("success"):
        raise ApiError(200, payload.get("error"), job_id, payload.get("details"))
    data = payload["data"]
    billing = (payload.get("metadata") or {}).get("billing")
    if isinstance(data, dict) and billing is not None:
        data = {**data, "request_billing": billing}
    return data


def _job_path(job_id):
    try:
        value = str(UUID(str(job_id)))
    except (ValueError, AttributeError) as exc:
        raise ValueError("job_id must be a UUID") from exc
    return f"/v3/cloud/athena/{value}"


def list(service="athena", limit=50, cursor=None, api_key=None, timeout=30):
    """Return job IDs and next_cursor in S3 key order, with request_billing."""
    if service != "athena":
        raise ValueError("service must be 'athena'")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    params = {"limit": limit}
    if cursor is not None:
        params["cursor"] = cursor
    return _request_json(
        f"/v3/cloud/{service}?{urllib.parse.urlencode(params)}", api_key, timeout
    )


def get(job_id, api_key=None, timeout=30):
    """Read a saved job. This call's charge is in request_billing."""
    return _request_json(_job_path(job_id), api_key, timeout, job_id=job_id)


def _float_header(headers, name):
    value = headers.get(name)
    return None if value is None else float(value)


def _extract_result_files(download_path, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []
    if tarfile.is_tarfile(download_path):
        with tarfile.open(download_path, "r") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                with archive.extractfile(member) as source:
                    target = output_dir / f"part-{len(files):05d}.parquet"
                    with open(target, "wb") as file:
                        shutil.copyfileobj(source, file)
                files.append(str(target))
        return files

    target = output_dir / "part-00000.parquet"
    shutil.move(str(download_path), target)
    return [str(target)]


def download(job_id, output_dir=None, api_key=None, chunk_size=1024 * 1024, file=None, timeout=30):
    """Download a saved job. Pending jobs raise JobNotReadyError; only egress is billed.

    file selects one manifest entry. Otherwise all result parts are downloaded.
    Transfers are not retried automatically because another transfer can be billable.
    """
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    path = f"{_job_path(job_id)}/download"
    if file is not None:
        path += f"?{urllib.parse.urlencode({'file': file})}"
    request = _request(path, api_key, accept="application/octet-stream")
    destination_dir = Path(output_dir or "datamule-query-result")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status == 202:
                details = json.loads(response.read().decode("utf-8")).get("data")
                try:
                    retry_after = float(response.headers.get("Retry-After", 5))
                except ValueError:
                    retry_after = 5
                if not math.isfinite(retry_after) or retry_after < 0:
                    retry_after = 5
                raise JobNotReadyError(job_id, retry_after, details)
            with tempfile.TemporaryDirectory() as tmp_dir:
                download_path = Path(tmp_dir) / "result"
                total_size = int(response.headers.get("Content-Length", 0))
                with open(download_path, "wb") as target, tqdm(
                    total=total_size, unit="B", unit_scale=True, desc="Query result",
                    disable=not logger.isEnabledFor(logging.INFO),
                ) as progress:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        target.write(chunk)
                        progress.update(len(chunk))
                files = _extract_result_files(download_path, destination_dir)
            headers = response.headers
    except urllib.error.HTTPError as exc:
        raise _api_error(exc, job_id) from exc

    scan_charge = _float_header(headers, "x-datamule-athena-scan-charge")
    download_charge = _float_header(headers, "x-datamule-s3-download-charge")
    total_charge = _float_header(headers, "x-datamule-total-charge")
    remaining_balance = _float_header(headers, "x-datamule-remaining-balance")
    api_key_credits = headers.get("x-datamule-remaining-api-key-credits")
    remaining_api_key_credits = (
        None if api_key_credits in (None, "unlimited") else float(api_key_credits)
    )
    billing = {
        "athena_scan": scan_charge, "s3_download": download_charge,
        "total_charge": total_charge, "remaining_balance": remaining_balance,
    }
    if api_key_credits is not None:
        billing["remaining_api_key_credits"] = remaining_api_key_credits
    logger.info("Downloaded job %s to %s", job_id, destination_dir)
    if total_charge is not None and remaining_balance is not None:
        logger.info(
            "Download cost: $%.4f | Remaining balance: $%.2f%s",
            total_charge, remaining_balance, _format_api_key_credits(billing),
        )
    return {
        "output_dir": str(destination_dir), "files": files,
        "query_id": headers.get("x-datamule-query-id", job_id),
        "cost": total_charge, "remaining_balance": remaining_balance,
        "remaining_api_key_credits": remaining_api_key_credits, "billing": billing,
    }
