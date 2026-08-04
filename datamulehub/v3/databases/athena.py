import json
import shutil
import tarfile
import tempfile
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path

from tqdm import tqdm

from ...api_key import get_api_key


API_BASE_URL = "https://api.datamule.xyz"


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


def _float_header(headers, name):
    value = headers.get(name)
    if value is None:
        return None
    return float(value)


def _transport_filename(headers):
    filename = _content_disposition_filename(headers.get("Content-Disposition"))
    if filename:
        return filename

    content_type = headers.get("Content-Type", "")
    if "parquet" in content_type:
        return "athena-result.parquet"
    return "athena-results.tar"


def _part_name(index):
    return f"part-{index:05d}.parquet"


def _extract_result_files(download_path, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []

    if tarfile.is_tarfile(download_path):
        with tarfile.open(download_path, "r") as archive:
            index = 0
            for member in archive.getmembers():
                if not member.isfile():
                    continue

                source = archive.extractfile(member)
                if source is None:
                    continue

                target = output_dir / _part_name(index)
                with open(target, "wb") as file:
                    shutil.copyfileobj(source, file)
                files.append(str(target))
                index += 1
        return files

    target = output_dir / _part_name(0)
    shutil.move(str(download_path), target)
    files.append(str(target))
    return files


def query(sql, output_dir=None, api_key=None, wait_seconds=None, chunk_size=1024 * 1024, quiet=False):
    key = get_api_key(api_key)
    body = {"query": sql}
    if wait_seconds is not None:
        body["wait_seconds"] = wait_seconds

    request = urllib.request.Request(
        f"{API_BASE_URL}/v3/athena/download",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Accept": "application/octet-stream",
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "User-Agent": "datamule-hub",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request) as response:
            destination_dir = Path(output_dir or "datamule-query-result")
            with tempfile.TemporaryDirectory() as tmp_dir:
                download_path = Path(tmp_dir) / _transport_filename(response.headers)
                total_size = int(response.headers.get("Content-Length", 0))

                with open(download_path, "wb") as file, tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    desc=download_path.name,
                    disable=quiet,
                ) as progress:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        file.write(chunk)
                        progress.update(len(chunk))

                files = _extract_result_files(download_path, destination_dir)
            headers = response.headers
    except urllib.error.HTTPError as exc:
        raise _api_error(exc) from exc

    scan_charge = _float_header(headers, "x-datamule-athena-scan-charge")
    download_charge = _float_header(headers, "x-datamule-s3-download-charge")
    total_charge = _float_header(headers, "x-datamule-total-charge")
    remaining_balance = _float_header(headers, "x-datamule-remaining-balance")
    query_id = headers.get("x-datamule-query-id")

    if not quiet:
        print(f"Downloaded query result to {destination_dir}")
        if total_charge is not None:
            print(f"- Cost: ${total_charge:.4f} | Remaining balance: ${remaining_balance:.2f}")

    return {
        "output_dir": str(destination_dir),
        "files": files,
        "query_id": query_id,
        "cost": total_charge,
        "remaining_balance": remaining_balance,
        "billing": {
            "athena_scan": scan_charge,
            "s3_download": download_charge,
            "total_charge": total_charge,
            "remaining_balance": remaining_balance,
        },
    }


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


def read_query(sql, api_key=None, wait_seconds=None):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = query(
            sql,
            output_dir=tmp_dir,
            api_key=api_key,
            wait_seconds=wait_seconds,
            quiet=False,
        )
        table = _read_result_table(result["output_dir"])
        return table.to_pylist()
