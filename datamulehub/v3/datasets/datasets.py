import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.message import Message
from pathlib import Path

from tqdm import tqdm

from ...api_key import get_api_key
from ...utils.format_accession import format_accession


API_BASE_URL = "https://api.datamule.xyz"
LIVE_RETRY_SECONDS = 3
LOOKUP_OVERLAP_SECONDS = 300
LIVE_DOWNLOAD_WORKERS = 32

DATASET_PATH_MAP = {
    "simple_xbrl": "datasets/simple_xbrl/data.parquet",
    "simple_xbrl_table": "datasets/simple_xbrl/data.parquet",
    "submissions_metadata": "metadata/submissions_metadata/data.parquet",
    "sec_master_submissions": "metadata/submissions_metadata/data.parquet",
    "sec_master_submissions_table": "metadata/submissions_metadata/data.parquet",
    "sec_submission_details": "metadata/sec_submission_details_table/data.parquet",
    "sec_submission_details_table": "metadata/sec_submission_details_table/data.parquet",
    "sec_accession_cik": "metadata/sec_accession_cik_table/data.parquet",
    "sec_accession_cik_table": "metadata/sec_accession_cik_table/data.parquet",
    "sec_documents": "metadata/sec_documents_table/data.parquet",
    "sec_documents_table": "metadata/sec_documents_table/data.parquet",
}


def resolve_path(dataset):
    value = str(dataset or "").strip().strip("/")
    if not value:
        raise ValueError("dataset is required.")

    key = value.lower().replace("-", "_")
    if key in DATASET_PATH_MAP:
        return DATASET_PATH_MAP[key]

    if value.startswith(("datasets/", "metadata/", "monitor-dumps/", "sec-filings/")):
        if value.endswith("/"):
            return f"{value}data.parquet"
        return value

    if value.startswith("xml2tables/"):
        table = value.split("/", 1)[1].strip("/")
        return f"datasets/xml2tables/{table}/data.parquet"

    if value.startswith("simple_xbrl/"):
        return f"datasets/{value.rstrip('/')}/data.parquet"

    return f"datasets/xml2tables/{value}/data.parquet"


class ApiError(Exception):
    def __init__(self, status, message):
        self.status = status
        super().__init__(f"API request failed ({status}): {message}")


def _api_error(exc):
    body = exc.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(body)
        message = payload.get("error", body)
    except json.JSONDecodeError:
        message = body
    return ApiError(exc.code, message)


def _content_disposition_filename(value):
    if not value:
        return None

    message = Message()
    message["content-disposition"] = value
    filename = message.get_param("filename", header="content-disposition")
    return filename.strip("\"") if filename else None


def _filename_from_path(path):
    name = path.rstrip("/").split("/")[-1]
    return name or "dataset.parquet"


def get_link(dataset, api_key=None):
    key = get_api_key(api_key)
    object_key = resolve_path(dataset)
    body = json.dumps({"path": object_key}).encode("utf-8")
    request = urllib.request.Request(
        f"{API_BASE_URL}/v3/get-s3-link",
        data=body,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "User-Agent": "datamule-hub",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise _api_error(exc) from exc

    if not payload.get("success"):
        raise Exception(f"API request failed: {payload.get('error')}")

    data = payload.get("data", {})
    billing = payload.get("metadata", {}).get("billing", {})
    return {
        "download_url": data["download_url"],
        "object_key": data.get("object_key", object_key),
        "size_bytes": data.get("size_bytes"),
        "size_gb": data.get("size_gb"),
        "expires_at": data.get("expires_at"),
        "last_modified": data.get("last_modified"),
        "billing": billing,
    }


def _download_link(link, output, chunk_size, accept_filename=False, quiet=False):
    request = urllib.request.Request(
        link["download_url"],
        headers={"User-Agent": "datamule-hub"},
        method="GET",
    )

    try:
        with urllib.request.urlopen(request) as response:
            total_size = int(response.headers.get("Content-Length", 0))
            if accept_filename:
                header_name = _content_disposition_filename(response.headers.get("Content-Disposition"))
                if header_name:
                    output = header_name
            with open(output, "wb") as file, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=os.path.basename(output),
                disable=quiet,
            ) as progress:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    file.write(chunk)
                    progress.update(len(chunk))
    except urllib.error.HTTPError as exc:
        raise _api_error(exc) from exc
    return output


def _xml_table_name(object_key):
    parts = object_key.split("/")
    if len(parts) == 4 and parts[:2] == ["datasets", "xml2tables"] and parts[3] == "data.parquet":
        return parts[2]
    return None


def _snapshot_keys(path, table, candidates):
    import pyarrow.parquet as pq

    if not candidates:
        return candidates
    parquet = pq.ParquetFile(path)
    fields = parquet.schema_arrow.names
    if "_sourceKey" in fields:
        for batch in parquet.iter_batches(columns=["_sourceKey"], batch_size=65536):
            candidates.difference_update(value for value in batch.column(0).to_pylist() if value)
    elif "filingDate" in fields and "accessionNumber" in fields:
        for batch in parquet.iter_batches(columns=["filingDate", "accessionNumber"], batch_size=65536):
            dates = batch.column(0).to_pylist()
            accessions = batch.column(1).to_pylist()
            for filing_date, accession in zip(dates, accessions):
                key = f"sec-filings/xml2tables/{filing_date}/{table}/{format_accession(accession, 'no-dash')}.parquet"
                candidates.discard(key)
    return candidates


def _recent_xml_keys(table, snapshot_last_modified, api_key):
    if snapshot_last_modified is None:
        raise ValueError("Dataset link did not include last_modified; deploy the updated download link service")

    start = datetime.fromtimestamp(int(snapshot_last_modified) - LOOKUP_OVERLAP_SECONDS, timezone.utc)
    end = datetime.now(timezone.utc) + timedelta(minutes=1)
    keys = set()
    page = 1
    while True:
        params = urllib.parse.urlencode({
            "table": table,
            "detectedTime_START": start.strftime("%Y-%m-%d %H:%M:%S"),
            "detectedTime_END": end.strftime("%Y-%m-%d %H:%M:%S"),
            "page": page,
        })
        request = urllib.request.Request(
            f"{API_BASE_URL}/v3/xml2tables/candidates?{params}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {get_api_key(api_key)}",
                "User-Agent": "datamule-hub",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise _api_error(exc) from exc
        if not payload.get("success"):
            raise Exception(f"API request failed: {payload.get('error')}")

        data = payload.get("data", {})
        for filing_date, accession in zip(data.get("filingDate", []), data.get("accession", [])):
            accession = format_accession(accession, "no-dash")
            keys.add(f"sec-filings/xml2tables/{filing_date}/{table}/{accession}.parquet")
        if not payload.get("metadata", {}).get("pagination", {}).get("hasMore"):
            break
        page += 1
    return keys


def _live_link(key, api_key):
    try:
        return get_link(key, api_key=api_key)
    except ApiError as exc:
        if exc.status != 404:
            raise
    time.sleep(LIVE_RETRY_SECONDS)
    try:
        return get_link(key, api_key=api_key)
    except ApiError as exc:
        if exc.status != 404:
            raise
        return None


def _download_live_file(index, key, directory, api_key, chunk_size):
    link = _live_link(key, api_key)
    if link is None:
        return None
    path = Path(directory) / f"live-{index}.parquet"
    _download_link(link, path, chunk_size, quiet=True)
    return path, key, link


def _merge_parquet(snapshot_path, additions, output_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    fields = {}
    for path, _key in [(snapshot_path, None), *additions]:
        for field in pq.read_schema(path):
            existing = fields.get(field.name)
            if existing is not None and existing.type != field.type:
                raise ValueError(f"Column {field.name} has conflicting Parquet types")
            fields.setdefault(field.name, field)
    fields.setdefault("_sourceKey", pa.field("_sourceKey", pa.string()))
    schema = pa.schema([field.with_nullable(True) for field in fields.values()])

    with pq.ParquetWriter(output_path, schema, compression="zstd") as writer:
        for path, key in [(snapshot_path, None), *additions]:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=65536):
                columns = {}
                for name in batch.schema.names:
                    columns[name] = batch.column(batch.schema.get_field_index(name))
                if key is not None:
                    columns["_sourceKey"] = pa.array([key] * batch.num_rows, type=pa.string())
                arrays = [
                    columns[field.name] if field.name in columns else pa.nulls(batch.num_rows, type=field.type)
                    for field in schema
                ]
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))


def download(dataset, filename=None, api_key=None, chunk_size=1024 * 1024, include_current_day=True):
    link = get_link(dataset, api_key=api_key)
    output = filename or _filename_from_path(link["object_key"])
    table = _xml_table_name(link["object_key"]) if include_current_day else None

    if table is None:
        output = _download_link(link, output, chunk_size, accept_filename=filename is None)
        links = [link]
    else:
        output_path = Path(output).resolve()
        with tempfile.TemporaryDirectory(dir=output_path.parent) as directory:
            snapshot_path = Path(directory) / "snapshot.parquet"
            _download_link(link, snapshot_path, chunk_size)
            candidates = _recent_xml_keys(table, link.get("last_modified"), api_key)
            _snapshot_keys(snapshot_path, table, candidates)
            additions = []
            links = [link]
            with ThreadPoolExecutor(max_workers=LIVE_DOWNLOAD_WORKERS) as executor:
                futures = [
                    executor.submit(_download_live_file, index, key, directory, api_key, chunk_size)
                    for index, key in enumerate(sorted(candidates))
                ]
                for future in as_completed(futures):
                    result = future.result()
                    if result is None:
                        continue
                    path, key, live_link = result
                    additions.append((path, key))
                    links.append(live_link)
            if additions:
                additions.sort(key=lambda item: item[1])
                merged_path = Path(directory) / "merged.parquet"
                _merge_parquet(snapshot_path, additions, merged_path)
                os.replace(merged_path, output_path)
            else:
                os.replace(snapshot_path, output_path)

    cost = sum(item.get("billing", {}).get("total_charge", 0) or 0 for item in links)
    balances = [
        item.get("billing", {}).get("remaining_balance")
        for item in links
        if item.get("billing", {}).get("remaining_balance") is not None
    ]
    remaining = min(balances) if balances else None
    billing = link.get("billing", {})
    billing = {**billing, "total_charge": cost, "remaining_balance": remaining}
    print(f"Downloaded to {output}")
    if remaining is not None:
        print(f"- Cost: ${cost:.4f} | Remaining balance: ${remaining:.2f}")

    return {
        "filename": output,
        "object_key": link["object_key"],
        "size_bytes": os.path.getsize(output),
        "size_gb": os.path.getsize(output) / 1000000000,
        "cost": cost,
        "remaining_balance": remaining,
        "billing": billing,
    }
