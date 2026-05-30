import asyncio
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tqdm.asyncio import tqdm

from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_sgml


BASE_URL = "https://sec-filings-archive.sgml.datamule.xyz"
DEFAULT_MAX_WORKERS = 64
DEFAULT_DECOMP_WORKERS = 8
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Decompression — runs in a thread so it never blocks the async event loop
# ---------------------------------------------------------------------------

def _decompress_and_write(compressed: bytes, out_path: Path) -> Path:
    try:
        import zstandard as zstd
    except ImportError as exc:
        raise ImportError("Install zstandard to download SEC filings archives.") from exc

    dctx = zstd.ZstdDecompressor()
    raw = dctx.decompress(compressed)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(raw)
    return out_path


# ---------------------------------------------------------------------------
# Single file download
# ---------------------------------------------------------------------------

async def _download_one(
    session,
    filing_date,
    accession,
    output_dir: Path,
    loop,
    decomp_pool,
    semaphore: asyncio.Semaphore,
) -> Path:
    accession_nd = format_accession(accession, "no-dash")
    url = f"{BASE_URL}/{filing_date}/{accession_nd}.sgml.zst"
    out_path = output_dir / str(filing_date) / f"{accession_nd}.sgml"

    async with semaphore:
        response = await session.get(url)
        response.raise_for_status()
        compressed = response.content

    return await loop.run_in_executor(
        decomp_pool,
        _decompress_and_write,
        compressed,
        out_path,
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
    quiet=False,
):
    """
    Download and decompress SGML archive objects to output_dir/filingDate/accession.sgml.

    Improvements over the original implementation
    ---------------------------------------------
    1. Single persistent httpx.AsyncClient with HTTP/2 multiplexing and
       connection keep-alive, shared across all requests.
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
            quiet=quiet,
        )
    )


async def _download_sgml_async(
    output_dir: Path,
    max_workers: int,
    decomp_workers: int,
    quiet: bool,
    **stream_kwargs,
):
    try:
        import httpx
    except ImportError as exc:
        raise ImportError("Install httpx to use the async downloader.") from exc

    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)
    downloaded: list[Path] = []

    async with httpx.AsyncClient(
        http2=True,
        timeout=60.0,
        headers={
            "Accept": "application/octet-stream",
            "Connection": "keep-alive",
            "User-Agent": "datamule-hub",
        },
        limits=httpx.Limits(
            max_connections=max_workers,
            max_keepalive_connections=max_workers,
        ),
    ) as session:
        with ThreadPoolExecutor(max_workers=decomp_workers,
                                thread_name_prefix="decomp") as decomp_pool:

            with tqdm(total=0, desc="Downloading SGML filings",
                      unit="filing", disable=quiet) as pbar:

                for lookup_page in stream_sgml(**stream_kwargs):
                    rows = list(zip(lookup_page["filingDate"], lookup_page["accession"]))
                    pbar.total += len(rows)
                    pbar.refresh()

                    tasks = [
                        _download_one(
                            session=session,
                            filing_date=fd,
                            accession=acc,
                            output_dir=output_dir,
                            loop=loop,
                            decomp_pool=decomp_pool,
                            semaphore=semaphore,
                        )
                        for fd, acc in rows
                    ]

                    for coro in asyncio.as_completed(tasks):
                        try:
                            downloaded.append(await coro)
                        except Exception as exc:
                            logger.error("Download failed: %s", exc)
                        finally:
                            pbar.update(1)

    logger.info(
        "SGML archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded