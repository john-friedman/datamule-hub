import logging
import math
import shutil
import tarfile
import tempfile
import time
import urllib.error
from pathlib import Path

from ...api_key import get_api_key
from .. import cloud
from ..cloud.cloud import _request_json
from .fastest import (
    download_fastest_metadata_query,
    is_fastest_metadata_query,
    read_fastest_metadata_query,
)


logger = logging.getLogger(__name__)


class QueryTimeoutError(TimeoutError):
    def __init__(self, job_id):
        self.job_id = job_id
        super().__init__(
            f"Stopped waiting for Athena job {job_id}. The job continues running. "
            "Use cloud.get(job_id) and cloud.download(job_id) to retrieve it later."
        )


def query(sql, output_dir=None, api_key=None, wait_seconds=600, chunk_size=1024 * 1024,
          from_storage="both", wait=True, poll_interval_ms=1000):
    """Submit SQL and download the result, or return the job ID with wait=False.

    wait_seconds limits local polling; None waits indefinitely. A timeout never
    cancels the server job. poll_interval_ms defaults to 1000 milliseconds.
    The fastest metadata table uses its existing service.
    """
    if from_storage not in ("both", "daily", "delta"):
        raise ValueError("from_storage must be 'both', 'daily', or 'delta'")
    if not isinstance(wait, bool):
        raise ValueError("wait must be True or False")
    if wait_seconds is not None and (
        isinstance(wait_seconds, bool) or not isinstance(wait_seconds, (int, float))
        or not math.isfinite(wait_seconds) or wait_seconds < 0
    ):
        raise ValueError("wait_seconds must be a nonnegative number or None")
    if (
        isinstance(poll_interval_ms, bool) or not isinstance(poll_interval_ms, (int, float))
        or not math.isfinite(poll_interval_ms) or poll_interval_ms <= 0
    ):
        raise ValueError("poll_interval_ms must be a positive number of milliseconds")
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    if is_fastest_metadata_query(sql):
        if not wait:
            raise ValueError("wait=False is only available for Athena queries")
        return download_fastest_metadata_query(sql, output_dir=output_dir, api_key=api_key)

    key = get_api_key(api_key)
    submitted = _request_json(
        "/v3/athena/query", api_key=key,
        body={"query": sql, "from_storage": from_storage, "wait_seconds": 0},
    )
    job_id = submitted["query_id"]
    logger.info("Submitted Athena job %s", job_id)
    if not wait:
        return {**submitted, "job_id": job_id}

    poll_charge = 0
    deadline = None if wait_seconds is None else time.monotonic() + wait_seconds
    while True:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise QueryTimeoutError(job_id)
        try:
            job = cloud.get(job_id, api_key=key, timeout=30 if remaining is None else min(30, remaining))
        except (TimeoutError, urllib.error.URLError) as exc:
            if deadline is not None and time.monotonic() >= deadline:
                raise QueryTimeoutError(job_id) from exc
            raise
        poll_charge = round(poll_charge + job.get("request_billing", {}).get("total_charge", 0), 12)
        if job["status"] in ("FAILED", "CANCELLED"):
            raise cloud.ApiError(
                409, f"Athena query {job['status'].lower()}: {job.get('state_change_reason') or 'No reason provided'}",
                job_id=job_id, details=job,
            )
        if job["status"] == "SUCCEEDED" and job["billing_status"] == "SETTLED":
            result = cloud.download(job_id, output_dir=output_dir, api_key=key, chunk_size=chunk_size)
            scan_charge = job["billing"]["total_charge"]
            result["cost"] = round(scan_charge + result["cost"] + poll_charge, 12)
            result["billing"] = {
                **result["billing"], "athena_scan": scan_charge,
                "cloud_get": poll_charge, "total_charge": result["cost"],
            }
            logger.info("Query %s total cost: $%.4f", job_id, result["cost"])
            return result
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise QueryTimeoutError(job_id)
        time.sleep(poll_interval_ms / 1000 if remaining is None else min(poll_interval_ms / 1000, remaining))


def _read_parquet(path):
    import pyarrow.parquet as pq

    return pq.read_table(path)


def _concat_tables(tables):
    import pyarrow as pa

    if not tables:
        return pa.table({})

    try:
        return pa.concat_tables(tables, promote_options="default")
    except TypeError:
        return pa.concat_tables(tables, promote=True)


def _read_result_table(path):
    path = Path(path)
    if path.is_dir():
        return _concat_tables([_read_parquet(file) for file in sorted(path.glob("*.parquet"))])

    if tarfile.is_tarfile(path):
        tables = []
        with tempfile.TemporaryDirectory() as extract_dir:
            extract_dir = Path(extract_dir)
            with tarfile.open(path, "r") as archive:
                for member in archive.getmembers():
                    if not member.isfile():
                        continue

                    source = archive.extractfile(member)
                    if source is None:
                        continue

                    target = extract_dir / Path(member.name).name
                    with open(target, "wb") as file:
                        file.write(source.read())
                    tables.append(_read_parquet(target))

        return _concat_tables(tables)

    return _read_parquet(path)


def read_query(sql, api_key=None, wait_seconds=600, from_storage="both", poll_interval_ms=1000):
    if from_storage not in ("both", "daily", "delta"):
        raise ValueError("from_storage must be 'both', 'daily', or 'delta'")
    if is_fastest_metadata_query(sql):
        return read_fastest_metadata_query(sql, api_key=api_key)

    with tempfile.TemporaryDirectory() as tmp_dir:
        result = query(
            sql,
            output_dir=tmp_dir,
            api_key=api_key,
            wait_seconds=wait_seconds,
            from_storage=from_storage,
            poll_interval_ms=poll_interval_ms,
        )
        table = _read_result_table(result["output_dir"])
        return table.to_pylist()
