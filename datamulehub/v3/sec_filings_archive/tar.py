import asyncio
import io
import logging
import re
import tarfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Union

from tqdm.asyncio import tqdm

from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_sgml, stream_tar
from .utils import (
    DownloadItem,
    TarBatchWriter,
    decompress_zstd,
    prepare_output_dir,
    store_item,
    validate_tar_max_size_mb,
)


BASE_URL = "https://sec-filings-archive.tar.datamule.xyz"
DEFAULT_MAX_WORKERS = 100
DEFAULT_DECOMP_WORKERS = 8
CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
logger = logging.getLogger(__name__)


def _is_document_mode(document_type, filename, sequence) -> bool:
    return document_type is not None or filename is not None or sequence is not None


def _parse_content_range(value: str) -> tuple[int, int]:
    match = CONTENT_RANGE_RE.match(value.strip())
    if match is None:
        raise RuntimeError(f"Invalid Content-Range header: {value}")

    start = int(match.group(1))
    end_inclusive = int(match.group(2))
    return start, end_inclusive + 1


def _extract_boundary(content_type: str) -> bytes:
    parts = [part.strip() for part in content_type.split(";")]
    if not parts or parts[0].lower() != "multipart/byteranges":
        raise RuntimeError(f"Expected multipart/byteranges response, got: {content_type}")

    for part in parts[1:]:
        name, separator, value = part.partition("=")
        if separator and name.strip().lower() == "boundary":
            boundary = value.strip()
            if len(boundary) >= 2 and boundary[0] == boundary[-1] == '"':
                boundary = boundary[1:-1]
            if not boundary:
                break
            return boundary.encode("latin-1")

    raise RuntimeError(f"Missing multipart boundary in Content-Type: {content_type}")


def _parse_headers(header_blob: bytes) -> dict[str, str]:
    headers = {}
    for line in header_blob.decode("latin-1").split("\r\n"):
        name, separator, value = line.partition(":")
        if separator:
            headers[name.strip().lower()] = value.strip()
    return headers


def _parse_multipart_byteranges(body: bytes, content_type: str) -> dict[tuple[int, int], bytes]:
    boundary = _extract_boundary(content_type)
    first_boundary = b"--" + boundary
    next_boundary = b"\r\n--" + boundary

    position = body.find(first_boundary)
    if position < 0:
        raise RuntimeError("Multipart response did not contain the declared boundary.")

    position += len(first_boundary)
    ranges = {}

    while True:
        if body[position:position + 2] == b"--":
            return ranges
        if body[position:position + 2] != b"\r\n":
            raise RuntimeError("Malformed multipart response after boundary.")
        position += 2

        header_end = body.find(b"\r\n\r\n", position)
        if header_end < 0:
            raise RuntimeError("Multipart part is missing a header terminator.")

        headers = _parse_headers(body[position:header_end])
        content_range = headers.get("content-range")
        if content_range is None:
            raise RuntimeError("Multipart part is missing Content-Range.")

        data_start = header_end + 4
        data_end = body.find(next_boundary, data_start)
        if data_end < 0:
            raise RuntimeError("Multipart part is missing a closing boundary.")

        range_key = _parse_content_range(content_range)
        if range_key in ranges:
            raise RuntimeError(f"Duplicate multipart range returned: {range_key}")
        ranges[range_key] = body[data_start:data_end]

        position = data_end + len(next_boundary)


def _parse_range_response(
    status: int,
    url,
    headers,
    content: bytes,
    expected_count: int,
) -> dict[tuple[int, int], bytes]:
    if status != 206:
        raise RuntimeError(
            f"Expected HTTP 206 for range request, got {status}: {url}"
        )

    if expected_count == 1:
        content_range = headers.get("Content-Range")
        if content_range is None:
            raise RuntimeError("Single-range response is missing Content-Range.")
        return {_parse_content_range(content_range): content}

    content_type = headers.get("Content-Type", "")
    return _parse_multipart_byteranges(content, content_type)


def _decompress_document(compressed: bytes, logical_path: Path) -> DownloadItem:
    return DownloadItem(logical_path=logical_path, data=decompress_zstd(compressed))


def _logical_document_path(filing_date, accession_nd: str, filename: str, decompress: bool) -> Path:
    name = Path(filename).name
    if not decompress:
        name = f"{name}.zst"
    return Path(str(filing_date)) / accession_nd / name


def _extract_submission_items(
    filing_date,
    accession_nd: str,
    tar_bytes: bytes,
    decompress: bool,
) -> list[DownloadItem]:
    items = []
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as archive:
        for member in archive:
            if not member.isfile():
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                continue

            data = extracted.read()
            member_name = Path(member.name).name
            if not member_name:
                continue

            if member_name == "metadata.json":
                logical_path = Path(str(filing_date)) / accession_nd / member_name
            elif decompress:
                data = decompress_zstd(data)
                logical_path = Path(str(filing_date)) / accession_nd / member_name
            else:
                logical_path = Path(str(filing_date)) / accession_nd / f"{member_name}.zst"

            items.append(DownloadItem(logical_path=logical_path, data=data))

    return items


async def _download_submission_tar(
    session,
    tar_url: str,
    filing_date,
    accession_nd: str,
    loop,
    decomp_pool,
    semaphore: asyncio.Semaphore,
    decompress: bool,
) -> list[DownloadItem]:
    async with semaphore:
        async with session.get(tar_url) as response:
            response.raise_for_status()
            if response.status != 200:
                raise RuntimeError(
                    f"Expected HTTP 200 for submission tar request, got {response.status}: {tar_url}"
                )
            tar_bytes = await response.read()

    return await loop.run_in_executor(
        decomp_pool,
        _extract_submission_items,
        filing_date,
        accession_nd,
        tar_bytes,
        decompress,
    )


async def _download_accession_tar(
    session,
    tar_url: str,
    filing_date,
    accession_nd: str,
    files: list[dict],
    loop,
    decomp_pool,
    semaphore: asyncio.Semaphore,
    decompress: bool,
) -> list[DownloadItem]:
    expected = {}
    range_parts = []
    for file_info in files:
        start = int(file_info["start"])
        end = int(file_info["end"])
        range_key = (start, end)
        if range_key in expected:
            raise RuntimeError(f"Duplicate requested byte range for {tar_url}: {range_key}")

        expected[range_key] = file_info
        range_parts.append(f"{start}-{end - 1}")

    headers = {"Range": f"bytes={','.join(range_parts)}"}

    async with semaphore:
        async with session.get(tar_url, headers=headers) as response:
            response.raise_for_status()
            content = await response.read()
            parts = _parse_range_response(
                response.status,
                response.url,
                response.headers,
                content,
                len(expected),
            )

    expected_ranges = set(expected)
    returned_ranges = set(parts)
    if returned_ranges != expected_ranges:
        missing = sorted(expected_ranges - returned_ranges)
        unexpected = sorted(returned_ranges - expected_ranges)
        raise RuntimeError(
            f"Multipart response ranges did not match request for {tar_url}: "
            f"missing={missing[:5]} unexpected={unexpected[:5]}"
        )

    items: list[DownloadItem] = []
    futures = []
    for range_key, file_info in expected.items():
        logical_path = _logical_document_path(
            filing_date,
            accession_nd,
            file_info["filename"],
            decompress,
        )
        data = parts[range_key]

        if not decompress:
            items.append(DownloadItem(logical_path=logical_path, data=data))
            continue

        futures.append(
            loop.run_in_executor(
                decomp_pool,
                _decompress_document,
                data,
                logical_path,
            )
        )

    if futures:
        items.extend(await asyncio.gather(*futures))
    return items


async def _process_rows(
    rows: list[tuple],
    session_factory,
    loop,
    decomp_pool,
    semaphore: asyncio.Semaphore,
    pbar,
    decompress: bool,
    tar_writer: Optional[TarBatchWriter],
    output_dir: Path,
) -> list[Path]:
    by_tar: dict[tuple, list[dict]] = defaultdict(list)
    for filing_date, accession, filename, start, end in rows:
        accession_nd = format_accession(accession, "no-dash")
        by_tar[(filing_date, accession_nd)].append({
            "filename": filename,
            "start": int(start),
            "end": int(end),
        })

    grouped_requests = list(by_tar.items())
    paths: list[Path] = []

    async with session_factory() as session:
        tasks = []
        for (filing_date, accession_nd), files in grouped_requests:
            tasks.append(
                _download_accession_tar(
                    session=session,
                    tar_url=f"{BASE_URL}/{filing_date}/{accession_nd}.tar",
                    filing_date=filing_date,
                    accession_nd=accession_nd,
                    files=files,
                    loop=loop,
                    decomp_pool=decomp_pool,
                    semaphore=semaphore,
                    decompress=decompress,
                )
            )

        for coro in asyncio.as_completed(tasks):
            result = await coro
            paths.extend(store_item(output_dir, item, tar_writer) for item in result)
            pbar.update(len(result))

    return paths


async def _process_submissions(
    rows: list[tuple],
    session_factory,
    loop,
    decomp_pool,
    semaphore: asyncio.Semaphore,
    pbar,
    decompress: bool,
    tar_writer: Optional[TarBatchWriter],
    output_dir: Path,
) -> list[Path]:
    seen = set()
    submissions = []
    for filing_date, accession in rows:
        accession_nd = format_accession(accession, "no-dash")
        key = (filing_date, accession_nd)
        if key in seen:
            continue
        seen.add(key)
        submissions.append(key)

    paths: list[Path] = []
    async with session_factory() as session:
        tasks = [
            _download_submission_tar(
                session=session,
                tar_url=f"{BASE_URL}/{filing_date}/{accession_nd}.tar",
                filing_date=filing_date,
                accession_nd=accession_nd,
                loop=loop,
                decomp_pool=decomp_pool,
                semaphore=semaphore,
                decompress=decompress,
            )
            for filing_date, accession_nd in submissions
        ]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            paths.extend(store_item(output_dir, item, tar_writer) for item in result)
            pbar.update(1)

    return paths


def _make_http_client(aiohttp, max_workers: int):
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=max_workers, ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=300),
        headers={
            "Accept": "application/octet-stream",
            "Connection": "keep-alive",
            "User-Agent": "datamule-hub",
        },
    )


def download_tar(
    cik=None,
    accession=None,
    submission_type=None,
    filing_date=None,
    report_date=None,
    detected_time=None,
    contains_xbrl=None,
    document_type=None,
    filename=None,
    sequence=None,
    api_key=None,
    page=None,
    page_size=25_000,
    output_dir="downloads",
    max_workers=DEFAULT_MAX_WORKERS,
    decomp_workers=DEFAULT_DECOMP_WORKERS,
    decompress=True,
    tar_max_size_mb=512,
    overwrite=False,
):
    """
    Download TAR document ranges.

    By default, decompressed documents are written into batch_*.tar shards under
    output_dir. Set tar_max_size_mb=None to write individual files to
    output_dir/filingDate/accession/filename.

    Queries without document_type, filename, or sequence download whole matching
    submission TARs. Document-level filters use exact byte-range downloads.
    """
    return asyncio.run(
        _download_tar_async(
            cik=cik,
            accession=accession,
            submission_type=submission_type,
            filing_date=filing_date,
            report_date=report_date,
            detected_time=detected_time,
            contains_xbrl=contains_xbrl,
            document_type=document_type,
            filename=filename,
            sequence=sequence,
            api_key=api_key,
            page=page,
            page_size=page_size,
            output_dir=Path(output_dir),
            max_workers=max_workers,
            decomp_workers=decomp_workers,
            decompress=decompress,
            tar_max_size_mb=tar_max_size_mb,
            overwrite=overwrite,
        )
    )


async def _download_tar_async(
    output_dir: Path,
    max_workers: int,
    decomp_workers: int,
    decompress: bool,
    tar_max_size_mb: Optional[Union[int, float]],
    overwrite: bool,
    **stream_kwargs,
):
    validate_tar_max_size_mb(tar_max_size_mb)
    if max_workers <= 0:
        raise ValueError("max_workers must be a positive integer.")
    prepare_output_dir(output_dir, overwrite=overwrite)
    document_mode = _is_document_mode(
        stream_kwargs.get("document_type"),
        stream_kwargs.get("filename"),
        stream_kwargs.get("sequence"),
    )

    try:
        import aiohttp
    except ImportError as exc:
        raise ImportError("Install aiohttp to use the async downloader.") from exc

    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)
    downloaded: list[Path] = []
    tar_writer = TarBatchWriter(output_dir, tar_max_size_mb) if tar_max_size_mb is not None else None

    try:
        session_factory = lambda: _make_http_client(aiohttp, max_workers)

        with ThreadPoolExecutor(max_workers=decomp_workers,
                                thread_name_prefix="decomp") as decomp_pool:

            with tqdm(total=0, desc="Downloading TAR filings",
                      unit="file") as pbar:

                if document_mode:
                    lookup_pages = stream_tar(**stream_kwargs)
                else:
                    lookup_pages = stream_sgml(**stream_kwargs)

                for lookup_page in lookup_pages:
                    if document_mode:
                        rows = list(zip(
                            lookup_page["filingDate"],
                            lookup_page["accession"],
                            lookup_page["filename"],
                            lookup_page["start"],
                            lookup_page["end"],
                        ))
                        pbar.total += len(rows)
                        pbar.refresh()

                        batch_paths = await _process_rows(
                            rows=rows,
                            session_factory=session_factory,
                            loop=loop,
                            decomp_pool=decomp_pool,
                            semaphore=semaphore,
                            pbar=pbar,
                            decompress=decompress,
                            tar_writer=tar_writer,
                            output_dir=output_dir,
                        )
                    else:
                        rows = list(zip(lookup_page["filingDate"], lookup_page["accession"]))
                        pbar.total += len(rows)
                        pbar.refresh()

                        batch_paths = await _process_submissions(
                            rows=rows,
                            session_factory=session_factory,
                            loop=loop,
                            decomp_pool=decomp_pool,
                            semaphore=semaphore,
                            pbar=pbar,
                            decompress=decompress,
                            tar_writer=tar_writer,
                            output_dir=output_dir,
                        )

                    downloaded.extend(batch_paths)
    finally:
        if tar_writer is not None:
            tar_writer.close()

    logger.info(
        "TAR archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded
