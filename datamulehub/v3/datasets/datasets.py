import json
import os
import urllib.error
import urllib.request
from email.message import Message

from tqdm import tqdm

from ...api_key import get_api_key


API_BASE_URL = "https://api.datamule.xyz"

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


def _api_error(exc):
    body = exc.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(body)
        message = payload.get("error", body)
    except json.JSONDecodeError:
        message = body
    return Exception(f"API request failed ({exc.code}): {message}")


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
        "billing": billing,
    }


def download(dataset, filename=None, api_key=None, chunk_size=1024 * 1024):
    link = get_link(dataset, api_key=api_key)
    output = filename or _filename_from_path(link["object_key"])
    request = urllib.request.Request(
        link["download_url"],
        headers={"User-Agent": "datamule-hub"},
        method="GET",
    )

    try:
        with urllib.request.urlopen(request) as response:
            total_size = int(response.headers.get("Content-Length", 0))
            header_name = _content_disposition_filename(response.headers.get("Content-Disposition"))
            if filename is None and header_name:
                output = header_name

            with open(output, "wb") as file, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=os.path.basename(output),
            ) as progress:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    file.write(chunk)
                    progress.update(len(chunk))
    except urllib.error.HTTPError as exc:
        raise _api_error(exc) from exc

    billing = link.get("billing", {})
    cost = billing.get("total_charge")
    remaining = billing.get("remaining_balance")
    print(f"Downloaded to {output}")
    if cost is not None:
        print(f"- Cost: ${cost:.4f} | Remaining balance: ${remaining:.2f}")

    return {
        "filename": output,
        "object_key": link["object_key"],
        "size_bytes": link.get("size_bytes"),
        "size_gb": link.get("size_gb"),
        "cost": cost,
        "remaining_balance": remaining,
        "billing": billing,
    }
