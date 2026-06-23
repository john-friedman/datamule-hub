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
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Decompression - runs in a thread so it never blocks the async event loop
# ---------------------------------------------------------------------------

def _decompress_item(compressed: bytes, logical_path: Path) -> DownloadItem:
    if compressed.startswith(ZSTD_MAGIC):
        return DownloadItem(logical_path=logical_path, data=decompress_zstd(compressed))
    return DownloadItem(logical_path=logical_path, data=compressed)


def _make_http_client(aiohttp, max_workers: int):
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=max_workers, ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=60),
        headers={
            "Accept": "application/octet-stream",
            "Connection": "keep-alive",
            "User-Agent": "datamule-hub",
        },
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

    try:
        async with semaphore:
            async with session.get(url) as response:
                response.raise_for_status()
                compressed = await response.read()

        if not decompress:
            return DownloadItem(logical_path=logical_path, data=compressed)

        return await loop.run_in_executor(
            decomp_pool,
            _decompress_item,
            compressed,
            logical_path,
        )
    except Exception as exc:
        raise RuntimeError(
            f"filing_date={filing_date} accession={accession_nd}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


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
):
    """
    Download SGML archive objects.

    By default, decompressed filings are written into batch_*.tar shards under
    output_dir. Set tar_max_size_mb=None to write individual files to
    output_dir/filingDate/accession.sgml.

    Uses async HTTP, optional zstd decompression, and optional local tar batching.
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
        )
    )


async def _download_sgml_async(
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

    try:
        import aiohttp
    except ImportError as exc:
        raise ImportError("Install aiohttp to use the async downloader.") from exc

    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)
    downloaded: list[Path] = []
    tar_writer = TarBatchWriter(output_dir, tar_max_size_mb) if tar_max_size_mb is not None else None

    try:
        async with _make_http_client(aiohttp, max_workers) as session:
            with ThreadPoolExecutor(max_workers=decomp_workers,
                                    thread_name_prefix="decomp") as decomp_pool:

                with tqdm(total=0, desc="Downloading SGML filings",
                          unit="filing") as pbar:

                    for lookup_page in stream_sgml(**stream_kwargs):
                        rows = list(zip(lookup_page["filingDate"], lookup_page["accession"]))
                        pbar.total += len(rows)
                        pbar.refresh()

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
                            for fd, acc in rows
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
