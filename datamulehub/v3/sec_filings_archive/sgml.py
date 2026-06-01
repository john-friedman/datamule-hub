import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Union

from tqdm.asyncio import tqdm

from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_sgml
from .utils import (
    DownloadItem,
    TarBatchWriter,
    decompress_zstd,
    prepare_output_dir,
    store_item,
    validate_tar_max_size_mb,
)


BASE_URL = "https://sec-filings-archive.sgml.datamule.xyz"
DEFAULT_MAX_WORKERS = 100
DEFAULT_DECOMP_WORKERS = 8
MAX_REQUESTS_PER_CLIENT = 2000
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Decompression - runs in a thread so it never blocks the async event loop
# ---------------------------------------------------------------------------

def _decompress_item(compressed: bytes, logical_path: Path) -> DownloadItem:
    return DownloadItem(logical_path=logical_path, data=decompress_zstd(compressed))


def _make_http_client(httpx):
    return httpx.AsyncClient(
        http2=True,
        timeout=60.0,
        headers={
            "Accept": "application/octet-stream",
            "Connection": "keep-alive",
            "User-Agent": "datamule-hub",
        },
        limits=httpx.Limits(
            max_connections=1,
            max_keepalive_connections=1,
        ),
    )


# ---------------------------------------------------------------------------
# Single file download
# ---------------------------------------------------------------------------

async def _download_one(
    session,
    filing_date,
    accession,
    loop,
    decomp_pool,
    semaphore: asyncio.Semaphore,
    decompress: bool,
) -> DownloadItem:
    accession_nd = format_accession(accession, "no-dash")
    url = f"{BASE_URL}/{filing_date}/{accession_nd}.sgml.zst"
    filename = f"{accession_nd}.sgml" if decompress else f"{accession_nd}.sgml.zst"
    logical_path = Path(str(filing_date)) / filename

    async with semaphore:
        response = await session.get(url)
        response.raise_for_status()
        compressed = response.content

    if not decompress:
        return DownloadItem(logical_path=logical_path, data=compressed)

    return await loop.run_in_executor(
        decomp_pool,
        _decompress_item,
        compressed,
        logical_path,
    )


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def download_sgml(
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
    quiet=False,
):
    """
    Download SGML archive objects.

    By default, decompressed filings are written into batch_*.tar shards under
    output_dir. Set tar_max_size_mb=None to write individual files to
    output_dir/filingDate/accession.sgml.

    Improvements over the original implementation
    ---------------------------------------------
    1. A single persistent httpx.AsyncClient with HTTP/2 multiplexing and
       connection keep-alive.
    2. asyncio replaces ThreadPoolExecutor for I/O, supporting hundreds of
       concurrent requests with negligible memory overhead.
    3. zstd decompression is offloaded to a separate ThreadPoolExecutor so
       CPU work runs in parallel with ongoing downloads.
    """
    return asyncio.run(
        _download_sgml_async(
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
            quiet=quiet,
        )
    )


async def _download_sgml_async(
    output_dir: Path,
    max_workers: int,
    decomp_workers: int,
    decompress: bool,
    tar_max_size_mb: Optional[Union[int, float]],
    overwrite: bool,
    quiet: bool,
    **stream_kwargs,
):
    validate_tar_max_size_mb(tar_max_size_mb)
    if max_workers <= 0:
        raise ValueError("max_workers must be a positive integer.")
    prepare_output_dir(output_dir, overwrite=overwrite)

    try:
        import httpx
    except ImportError as exc:
        raise ImportError("Install httpx to use the async downloader.") from exc

    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)
    downloaded: list[Path] = []
    tar_writer = TarBatchWriter(output_dir, tar_max_size_mb) if tar_max_size_mb is not None else None

    try:
        with ThreadPoolExecutor(max_workers=decomp_workers,
                                thread_name_prefix="decomp") as decomp_pool:

            with tqdm(total=0, desc="Downloading SGML filings",
                      unit="filing", disable=quiet) as pbar:

                for lookup_page in stream_sgml(**stream_kwargs):
                    rows = list(zip(lookup_page["filingDate"], lookup_page["accession"]))
                    pbar.total += len(rows)
                    pbar.refresh()

                    for offset in range(0, len(rows), MAX_REQUESTS_PER_CLIENT):
                        row_chunk = rows[offset:offset + MAX_REQUESTS_PER_CLIENT]
                        async with _make_http_client(httpx) as session:
                            tasks = [
                                _download_one(
                                    session=session,
                                    filing_date=fd,
                                    accession=acc,
                                    loop=loop,
                                    decomp_pool=decomp_pool,
                                    semaphore=semaphore,
                                    decompress=decompress,
                                )
                                for fd, acc in row_chunk
                            ]

                            for coro in asyncio.as_completed(tasks):
                                try:
                                    item = await coro
                                    downloaded.append(store_item(output_dir, item, tar_writer))
                                except Exception as exc:
                                    logger.error(
                                        "Download failed: %s: %r",
                                        type(exc).__name__,
                                        exc,
                                    )
                                finally:
                                    pbar.update(1)
    finally:
        if tar_writer is not None:
            tar_writer.close()

    logger.info(
        "SGML archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded
