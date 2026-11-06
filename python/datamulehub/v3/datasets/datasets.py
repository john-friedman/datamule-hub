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

from ...api_key import _format_api_key_credits, get_api_key
from ...utils.format_accession import format_accession
from ...utils.xbrl import ACCESSION_BATCH_SIZE, iter_xbrl_filings, normalize_accessions
from ..sec_filings_lookup import stream_sgml
from .document_types import DOCUMENT_TYPES_BY_TABLE


API_BASE_URL = "https://api.datamule.xyz"
EASTERN = ZoneInfo("America/New_York")
DELTA_RETRY_SECONDS = 3
DELTA_DOWNLOAD_WORKERS = 32
DELTA_REQUEST_TIMEOUT_SECONDS = 30

METADATA_TABLES = tuple(f"filing_metadata_{name}" for name in ("filings", "entities", "former_companies", "series", "classes"))

DATASET_PATH_MAP = {
    "filer_metadata": "metadata/filer_metadata/data.parquet",
    "filer_names": "metadata/filer_names/data.parquet",
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
DATASET_PATH_MAP.update({table: f"datasets/metadata/{table}/data.parquet" for table in METADATA_TABLES})


STANDARDIZED_TEXT_PREFIX = "datasets/standardized_text/doc2struct/"
STANDARDIZED_TEXT_SOURCE_PREFIX = "sec-filings/standardized_text/doc2struct/"


def _standardized_text_year(year):
    if isinstance(year, bool) or str(year) != str(int(year)):
        raise ValueError("year must be a four-digit filing year")
    year = int(year)
    if not 1993 <= year <= datetime.now(EASTERN).year:
        raise ValueError("year must be between 1993 and the current filing year")
    return year


def resolve_path(dataset, year=None):
    value = str(dataset or "").strip().strip("/")
    if not value:
        raise ValueError("dataset is required.")

    key = value.lower().replace("-", "_")
    if key == "standardized_text":
        if year is None:
            raise ValueError("year is required for a single standardized_text download link")
        return f"{STANDARDIZED_TEXT_PREFIX}{_standardized_text_year(year)}.parquet"
    if year is not None:
        raise ValueError("year is only supported for standardized_text")
    if key in DATASET_PATH_MAP:
        return DATASET_PATH_MAP[key]

    if value.startswith(("datasets/", "metadata/", "monitor-dumps/", "sec-filings/")):
        if value.endswith("/"):
            return f"{value}data.parquet"
        return value

    if value.startswith(("xml2tables/", "xmltables/")):
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


def _urlopen(request, timeout=None):
    if timeout is None:
        return urllib.request.urlopen(request)
    return urllib.request.urlopen(request, timeout=timeout)


def get_link(dataset, api_key=None, timeout=None, year=None):
    key = get_api_key(api_key)
    object_key = resolve_path(dataset, year=year)
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
        with _urlopen(request, timeout=timeout) as response:
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


def _download_link(link, output, chunk_size, accept_filename=False, quiet=False, timeout=None):
    request = urllib.request.Request(
        link["download_url"],
        headers={"User-Agent": "datamule-hub"},
        method="GET",
    )

    try:
        with _urlopen(request, timeout=timeout) as response:
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


def _metadata_table_name(object_key):
    parts = object_key.split("/")
    if len(parts) == 4 and parts[:2] == ["datasets", "metadata"] and parts[3] == "data.parquet" and parts[2] in METADATA_TABLES:
        return parts[2]
    return None


def _source_key(table, filing_date, accession):
    accession = format_accession(accession, "no-dash")
    if table == "standardized_text":
        return f"{STANDARDIZED_TEXT_SOURCE_PREFIX}{filing_date}/{accession}.parquet"
    if table is None:
        return f"sec-filings/simple_xbrl/{filing_date}/{accession}.parquet"
    prefix = "metadata" if table in METADATA_TABLES else "xml2tables"
    return f"sec-filings/{prefix}/{filing_date}/{table}/{accession}.parquet"


def _dataset_filing_filters(table):
    if table == "standardized_text":
        return {}
    if table is None:
        return {"contains_xbrl": True}
    if table in METADATA_TABLES:
        return {}
    document_types = DOCUMENT_TYPES_BY_TABLE.get(table)
    if document_types is None:
        raise ValueError(f"Unknown XML table: {table}")
    return {"document_type": document_types}


def _delta_keys_for_dates(table, filing_dates, api_key):
    keys = set()
    for filing_date in filing_dates:
        for data in stream_sgml(filing_date=filing_date, api_key=api_key, **_dataset_filing_filters(table)):
            for accession in data.get("accession", []):
                keys.add(_source_key(table, filing_date, accession))
    return keys


def _accession_keys(table, accession, api_key):
    if table is None:
        return {
            _source_key(None, filing_date, number)
            for filing_date, number in iter_xbrl_filings(accession=accession, api_key=api_key)
        }

    filters = _dataset_filing_filters(table)
    keys = set()
    for index in range(0, len(accession), ACCESSION_BATCH_SIZE):
        batch = accession[index:index + ACCESSION_BATCH_SIZE]
        for data in stream_sgml(accession=batch, api_key=api_key, **filters):
            dates = data.get("filingDate", [])
            numbers = data.get("accession", [])
            if len(dates) != len(numbers):
                raise ValueError("Filing lookup returned mismatched accession and filingDate columns.")
            for filing_date, number in zip(dates, numbers):
                number = format_accession(number, "no-dash")
                if number not in batch:
                    raise ValueError("Filing lookup returned an accession outside the requested batch.")
                keys.add(_source_key(table, filing_date, number))
    return keys


def _last_delta_filing_date(et_now):
    if et_now.hour < 6:
        return et_now.date() - timedelta(days=1)
    return et_now.date()


def _delta_filing_dates(last_modified, et_now):
    if last_modified is None:
        raise ValueError("Daily download link is missing last_modified.")
    # Each overnight snapshot contains all filing days before its Eastern publication date.
    first = datetime.fromtimestamp(last_modified, EASTERN).date()
    last = _last_delta_filing_date(et_now)
    return [
        (first + timedelta(days=offset)).isoformat()
        for offset in range((last - first).days + 1)
    ]


def _delta_link(key, api_key):
    try:
        return get_link(key, api_key=api_key, timeout=DELTA_REQUEST_TIMEOUT_SECONDS)
    except ApiError as exc:
        if exc.status != 404:
            raise
    time.sleep(DELTA_RETRY_SECONDS)
    try:
        return get_link(key, api_key=api_key, timeout=DELTA_REQUEST_TIMEOUT_SECONDS)
    except ApiError as exc:
        if exc.status != 404:
            raise
        return None


def _download_delta_file(index, key, directory, api_key, chunk_size):
    link = _delta_link(key, api_key)
    if link is None:
        return None
    path = Path(directory) / f"delta-{index}.parquet"
    _download_link(link, path, chunk_size, quiet=True, timeout=DELTA_REQUEST_TIMEOUT_SECONDS)
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


def _download_accessions(table, accession, filename, api_key, chunk_size, year=None):
    import pyarrow as pa
    import pyarrow.parquet as pq

    candidates = _accession_keys(table, accession, api_key)
    if year is not None:
        candidates = {key for key in candidates if key.startswith(f"{STANDARDIZED_TEXT_SOURCE_PREFIX}{year}-")}
    output = filename or f"{table or 'simple_xbrl'}.parquet"
    output_path = Path(output).resolve()
    with tempfile.TemporaryDirectory(dir=output_path.parent) as directory:
        additions, links = _download_delta_files(candidates, directory, api_key, chunk_size)
        merged_path = Path(directory) / "merged.parquet"
        if additions:
            _merge_parquet([(path, key) for path, key in additions], merged_path)
        elif table == "standardized_text":
            pq.write_table(pa.Table.from_batches([], schema=_standardized_text_schema()), merged_path, compression="zstd")
        elif table is None:
            schema = pa.schema([
                pa.field("accessionNumber", pa.uint64()),
                pa.field("context_id", pa.uint64()),
                *[pa.field(name, pa.string()) for name in [
                    "taxonomy", "name", "value", "period_start_date", "period_end_date", "members",
                    "filingDate", "_sourceKey",
                ]],
            ])
            pq.write_table(pa.Table.from_batches([], schema=schema), merged_path, compression="zstd")
        else:
            schema = pa.schema([pa.field("_sourceKey", pa.string())])
            pq.write_table(pa.Table.from_batches([], schema=schema), merged_path, compression="zstd")
        os.replace(merged_path, output_path)

    downloaded = {key.rsplit("/", 1)[1].removesuffix(".parquet") for _, key in additions}
    result = _download_result(output, None, links)
    result["object_keys"] = [key for _, key in additions]
    result["accession"] = accession
    result["missing_accessions"] = [number for number in accession if number not in downloaded]
    return result


def _standardized_text_schema():
    import pyarrow as pa
    numbers = {"accessionNumber", "sequence", "id", "level"}
    matrix = pa.list_(pa.list_(pa.string()))
    names = ["accessionNumber", "sequence", "filingDate", "filename", "documentType", "id", "level", "type", "text", "matrix", "cleaned", "preamble", "footnotes", "postamble", "class", "standardizedTitle", "_sourceKey"]
    return pa.schema([
        pa.field(name, pa.uint64() if name in numbers else matrix if name in ("matrix", "footnotes") else pa.list_(pa.string()) if name == "cleaned" else pa.string())
        for name in names
    ])


def _download_standardized_text_year(year, filename, api_key, chunk_size, from_storage):
    object_key = resolve_path("standardized_text", year=year)
    output = filename or f"standardized_text-{year}.parquet"
    output_path = Path(output).resolve()
    link = get_link(object_key, api_key=api_key) if from_storage != "delta" else None
    links = [link] if link else []
    if from_storage == "daily":
        _download_link(link, output, chunk_size)
    else:
        now = datetime.now(EASTERN)
        dates = (_delta_filing_dates(link["last_modified"], now) if link else [_last_delta_filing_date(now).isoformat()])
        dates = [date for date in dates if date.startswith(f"{year}-")]
        with tempfile.TemporaryDirectory(dir=output_path.parent) as directory:
            snapshot = None
            if link:
                snapshot = Path(directory) / "snapshot.parquet"
                _download_link(link, snapshot, chunk_size)
            keys = _delta_keys_for_dates("standardized_text", dates, api_key)
            additions, delta_links = _download_delta_files(keys, directory, api_key, chunk_size)
            links.extend(delta_links)
            if snapshot and not additions:
                os.replace(snapshot, output_path)
            elif snapshot or additions:
                merged = Path(directory) / "merged.parquet"
                _merge_parquet(([(snapshot, None)] if snapshot else []) + additions, merged)
                os.replace(merged, output_path)
            else:
                import pyarrow as pa
                import pyarrow.parquet as pq
                pq.write_table(pa.Table.from_batches([], schema=_standardized_text_schema()), output_path, compression="zstd")
    return _download_result(output, object_key, links)


def _download_standardized_text(year, filename, api_key, chunk_size, from_storage, accession):
    if accession is not None:
        if from_storage == "daily":
            raise ValueError("accession downloads use per-filing objects; from_storage='daily' is unavailable.")
        return _download_accessions("standardized_text", normalize_accessions(accession), filename, api_key, chunk_size, year=year)
    if year is not None:
        return _download_standardized_text_year(year, filename, api_key, chunk_size, from_storage)
    if from_storage == "delta":
        year = _last_delta_filing_date(datetime.now(EASTERN)).year
        return _download_standardized_text_year(year, filename, api_key, chunk_size, from_storage)
    # A corpus download remains a directory of yearly files, never a giant merged file.
    directory = Path(filename or "standardized_text")
    if directory.suffix == ".parquet":
        raise ValueError("Select year for one Parquet file, or use filename as a directory for all years")
    directory.mkdir(parents=True, exist_ok=True)
    results = []
    for filing_year in range(1993, datetime.now(EASTERN).year + 1):
        try:
            results.append(_download_standardized_text_year(filing_year, str(directory / f"{filing_year}.parquet"), api_key, chunk_size, from_storage))
        except ApiError as error:
            if error.status != 404:
                raise
    if not results:
        raise ValueError("No yearly standardized_text snapshots are available yet")
    return {"files": [result["filename"] for result in results], "results": results, "cost": sum(result["cost"] for result in results)}


def download(dataset, filename=None, api_key=None, chunk_size=1024 * 1024, from_storage="both", accession=None, year=None):
    if from_storage not in ("both", "daily", "delta"):
        raise ValueError("from_storage must be 'both', 'daily', or 'delta'")

    if str(dataset).strip().strip("/").lower().replace("-", "_") == "standardized_text":
        if year is not None:
            year = _standardized_text_year(year)
        return _download_standardized_text(year, filename, api_key, chunk_size, from_storage, accession)
    if year is not None:
        raise ValueError("year is only supported for standardized_text")
    object_key = resolve_path(dataset)
    table = _xml_table_name(object_key) or _metadata_table_name(object_key)
    if accession is not None:
        accession = normalize_accessions(accession)
        if table is None and object_key != DATASET_PATH_MAP["simple_xbrl"]:
            raise ValueError("accession requires a metadata table, XML table or simple_xbrl dataset.")
        if from_storage == "daily":
            raise ValueError("accession downloads use per-filing objects; from_storage='daily' is unavailable.")
        return _download_accessions(table, accession, filename, api_key, chunk_size)
    has_delta = table is not None or object_key == DATASET_PATH_MAP["simple_xbrl"]
    if from_storage == "delta" and not has_delta:
        raise ValueError("from_storage='delta' requires a metadata table, XML table or simple_xbrl dataset")

    include_daily = from_storage != "delta"
    link = get_link(object_key, api_key=api_key) if include_daily else None
    filing_dates = []
    if has_delta and from_storage != "daily":
        et_now = datetime.now(EASTERN)
        filing_dates = (
            _delta_filing_dates(link["last_modified"], et_now)
            if include_daily else [_last_delta_filing_date(et_now).isoformat()]
        )
    lookup_executor = ThreadPoolExecutor(max_workers=1) if include_daily and filing_dates else None
    try:
        lookup_future = (
            lookup_executor.submit(_delta_keys_for_dates, table, filing_dates, api_key)
            if lookup_executor else None
        )
        output = filename or _filename_from_path(link["object_key"] if link else object_key)
        links = [link] if link else []

        if include_daily and not filing_dates:
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
                elif filing_dates:
                    candidates = _delta_keys_for_dates(table, filing_dates, api_key)
                else:
                    candidates = set()
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

    return _download_result(output, link["object_key"] if link else object_key, links)


def _download_result(output, object_key, links):
    cost = sum(item.get("billing", {}).get("total_charge", 0) or 0 for item in links)
    balances = [
        item.get("billing", {}).get("remaining_balance")
        for item in links
        if item.get("billing", {}).get("remaining_balance") is not None
    ]
    remaining = min(balances) if balances else None
    billing = links[0].get("billing", {}) if links else {}
    billing = {**billing, "total_charge": cost, "remaining_balance": remaining}
    if any("remaining_api_key_credits" in item.get("billing", {}) for item in links):
        credits = [
            item["billing"]["remaining_api_key_credits"]
            for item in links
            if item.get("billing", {}).get("remaining_api_key_credits") is not None
        ]
        billing["remaining_api_key_credits"] = min(credits) if credits else None
    print(f"Downloaded to {output}")
    if remaining is not None:
        print(
            f"- Cost: ${cost:.4f} | Remaining balance: ${remaining:.2f}"
            + _format_api_key_credits(billing)
        )

    return {
        "filename": output,
        "object_key": object_key,
        "size_bytes": os.path.getsize(output),
        "size_gb": os.path.getsize(output) / 1000000000,
        "cost": cost,
        "remaining_balance": remaining,
        "remaining_api_key_credits": billing.get("remaining_api_key_credits"),
        "billing": billing,
    }
