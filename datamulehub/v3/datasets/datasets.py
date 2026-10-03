import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from email.message import Message
from pathlib import Path
from zoneinfo import ZoneInfo

from tqdm import tqdm

from ...api_key import get_api_key
from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_sgml
from .document_types import DOCUMENT_TYPES_BY_TABLE


API_BASE_URL = "https://api.datamule.xyz"
DELTA_RETRY_SECONDS = 3
DELTA_DOWNLOAD_WORKERS = 32

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


def _source_key(table, filing_date, accession):
    accession = format_accession(accession, "no-dash")
    if table is None:
        return f"sec-filings/simple_xbrl/{filing_date}/{accession}.parquet"
    return f"sec-filings/xml2tables/{filing_date}/{table}/{accession}.parquet"


def _snapshot_keys(path, table, candidates):
    import pyarrow.parquet as pq

    if not candidates:
        return candidates
    parquet = pq.ParquetFile(path)
    fields = parquet.schema_arrow.names
    if "_sourceKey" in fields:
        for batch in parquet.iter_batches(columns=["_sourceKey"], batch_size=65536):
            candidates.difference_update(value for value in batch.column(0).to_pylist() if value)
    if candidates and "filingDate" in fields and "accessionNumber" in fields:
        for batch in parquet.iter_batches(columns=["filingDate", "accessionNumber"], batch_size=65536):
            dates = batch.column(0).to_pylist()
            accessions = batch.column(1).to_pylist()
            for filing_date, accession in zip(dates, accessions):
                if filing_date is None or accession is None:
                    continue
                candidates.discard(_source_key(table, filing_date, accession))
    return candidates


def _delta_keys_for_date(table, filing_date, api_key):
    if table is None:
        filters = {"contains_xbrl": True}
    else:
        document_types = DOCUMENT_TYPES_BY_TABLE.get(table)
        if document_types is None:
            raise ValueError(f"Unknown XML table: {table}")
        filters = {"document_type": document_types}

    keys = set()
    for data in stream_sgml(filing_date=filing_date, api_key=api_key, **filters):
        for accession in data.get("accession", []):
            keys.add(_source_key(table, filing_date, accession))
    return keys


def _delta_filing_date(et_now):
    minutes = et_now.hour * 60 + et_now.minute
    if minutes < 150:
        return (et_now.date() - timedelta(days=1)).isoformat()
    if minutes < 390:
        return None
    return et_now.date().isoformat()


def _delta_link(key, api_key):
    try:
        return get_link(key, api_key=api_key)
    except ApiError as exc:
        if exc.status != 404:
            raise
    time.sleep(DELTA_RETRY_SECONDS)
    try:
        return get_link(key, api_key=api_key)
    except ApiError as exc:
        if exc.status != 404:
            raise
        return None


def _download_delta_file(index, key, directory, api_key, chunk_size):
    link = _delta_link(key, api_key)
    if link is None:
        return None
    path = Path(directory) / f"delta-{index}.parquet"
    _download_link(link, path, chunk_size, quiet=True)
    return path, key, link


def _download_delta_files(candidates, directory, api_key, chunk_size):
    additions = []
    links = []
    if not candidates:
        return additions, links

    keys = iter(enumerate(sorted(candidates)))
    worker_count = min(DELTA_DOWNLOAD_WORKERS, len(candidates))
    in_flight = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        def submit_next():
            try:
                index, key = next(keys)
            except StopIteration:
                return False
            future = executor.submit(_download_delta_file, index, key, directory, api_key, chunk_size)
            in_flight[future] = key
            return True

        for _ in range(worker_count):
            submit_next()

        missing = 0
        failed = 0
        first_error = None
        with tqdm(total=len(candidates), desc="Delta filings", unit="filing") as progress:
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    key = in_flight.pop(future)
                    try:
                        result = future.result()
                        if result is None:
                            missing += 1
                        else:
                            path, _, link = result
                            additions.append((path, key))
                            links.append(link)
                    except Exception as exc:
                        failed += 1
                        if first_error is None:
                            first_error = exc
                    finally:
                        progress.set_postfix_str(
                            f"{len(additions)} downloaded, {missing} missing, {failed} failed",
                            refresh=False,
                        )
                        progress.update(1)

                if first_error is None:
                    while len(in_flight) < worker_count and submit_next():
                        pass

            if first_error is not None:
                progress.refresh()
                raise first_error

    additions.sort(key=lambda item: item[1])
    return additions, links


def _merge_parquet(sources, output_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    fields = {}
    for path, _key in sources:
        for field in pq.read_schema(path):
            existing = fields.get(field.name)
            if existing is not None and existing.type != field.type:
                raise ValueError(f"Column {field.name} has conflicting Parquet types")
            fields.setdefault(field.name, field)
    if any(key and key.startswith("sec-filings/simple_xbrl/") for _, key in sources):
        fields.setdefault("filingDate", pa.field("filingDate", pa.string()))
    fields.setdefault("_sourceKey", pa.field("_sourceKey", pa.string()))
    schema = pa.schema([field.with_nullable(True) for field in fields.values()])

    with pq.ParquetWriter(output_path, schema, compression="zstd") as writer:
        for path, key in sources:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=65536):
                columns = {}
                for name in batch.schema.names:
                    columns[name] = batch.column(batch.schema.get_field_index(name))
                if key is not None:
                    columns["_sourceKey"] = pa.array([key] * batch.num_rows, type=pa.string())
                    if "filingDate" in fields and "filingDate" not in columns:
                        columns["filingDate"] = pa.array(
                            [key.split("/")[2]] * batch.num_rows, type=pa.string()
                        )
                arrays = [
                    columns[field.name] if field.name in columns else pa.nulls(batch.num_rows, type=field.type)
                    for field in schema
                ]
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))


def download(dataset, filename=None, api_key=None, chunk_size=1024 * 1024, from_storage="both"):
    if from_storage not in ("both", "daily", "delta"):
        raise ValueError("from_storage must be 'both', 'daily', or 'delta'")

    object_key = resolve_path(dataset)
    table = _xml_table_name(object_key)
    has_delta = table is not None or object_key == DATASET_PATH_MAP["simple_xbrl"]
    if from_storage == "delta" and not has_delta:
        raise ValueError("from_storage='delta' requires an XML table or simple_xbrl dataset")

    et_now = datetime.now().astimezone(ZoneInfo("America/New_York")) if has_delta else None
    filing_date = _delta_filing_date(et_now) if et_now and from_storage != "daily" else None
    include_daily = from_storage != "delta"
    lookup_executor = ThreadPoolExecutor(max_workers=1) if include_daily and filing_date else None
    try:
        lookup_future = (
            lookup_executor.submit(_delta_keys_for_date, table, filing_date, api_key)
            if lookup_executor else None
        )
        link = get_link(object_key, api_key=api_key) if include_daily else None
        output = filename or _filename_from_path(link["object_key"] if link else object_key)
        links = [link] if link else []

        if include_daily and filing_date is None:
            output = _download_link(link, output, chunk_size, accept_filename=filename is None)
        else:
            output_path = Path(output).resolve()
            with tempfile.TemporaryDirectory(dir=output_path.parent) as directory:
                snapshot_path = None
                if link:
                    snapshot_path = Path(directory) / "snapshot.parquet"
                    _download_link(link, snapshot_path, chunk_size)
                if lookup_future:
                    candidates = lookup_future.result()
                elif filing_date:
                    candidates = _delta_keys_for_date(table, filing_date, api_key)
                else:
                    candidates = set()
                if snapshot_path:
                    _snapshot_keys(snapshot_path, table, candidates)
                additions, delta_links = _download_delta_files(candidates, directory, api_key, chunk_size)
                links.extend(delta_links)
                if snapshot_path and not additions:
                    os.replace(snapshot_path, output_path)
                else:
                    sources = ([(snapshot_path, None)] if snapshot_path else []) + additions
                    merged_path = Path(directory) / "merged.parquet"
                    _merge_parquet(sources, merged_path)
                    os.replace(merged_path, output_path)
    finally:
        if lookup_executor:
            lookup_executor.shutdown(wait=True)

    cost = sum(item.get("billing", {}).get("total_charge", 0) or 0 for item in links)
    balances = [
        item.get("billing", {}).get("remaining_balance")
        for item in links
        if item.get("billing", {}).get("remaining_balance") is not None
    ]
    remaining = min(balances) if balances else None
    billing = links[0].get("billing", {}) if links else {}
    billing = {**billing, "total_charge": cost, "remaining_balance": remaining}
    print(f"Downloaded to {output}")
    if remaining is not None:
        print(f"- Cost: ${cost:.4f} | Remaining balance: ${remaining:.2f}")

    return {
        "filename": output,
        "object_key": link["object_key"] if link else object_key,
        "size_bytes": os.path.getsize(output),
        "size_gb": os.path.getsize(output) / 1000000000,
        "cost": cost,
        "remaining_balance": remaining,
        "billing": billing,
    }
