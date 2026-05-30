import asyncio
import io
import logging
import shutil
from collections import defaultdict
from pathlib import Path

from tqdm.asyncio import tqdm

from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_tar


BASE_URL = "https://sec-filings-archive.tar.datamule.xyz"
DEFAULT_MAX_WORKERS = 64          # higher is fine with async
DEFAULT_DECOMP_WORKERS = 8        # threadpool for CPU-bound zstd
RANGE_MERGE_GAP = 64 * 1024       # merge ranges within 64 KB of each other
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Range merging (improvement #2)
# ---------------------------------------------------------------------------

def _merge_ranges(files: list[dict], gap: int = RANGE_MERGE_GAP) -> list[dict]:
    """
    Given a list of file dicts (each with keys start, end, and anything else),
    merge those whose byte ranges are within `gap` bytes of each other into a
    single HTTP request. Returns a list of "batch" dicts:

        {
            "start":   int,          # merged range start
            "end":     int,          # merged range end
            "files":   [file_dict],  # originals covered by this range
        }
    """
    if not files:
        return []

    sorted_files = sorted(files, key=lambda f: f["start"])
    batches = []
    current_batch = {"start": sorted_files[0]["start"],
                     "end":   sorted_files[0]["end"],
                     "files": [sorted_files[0]]}

    for f in sorted_files[1:]:
        if f["start"] <= current_batch["end"] + gap:
            # Extend the current batch to cover this file too
            current_batch["end"] = max(current_batch["end"], f["end"])
            current_batch["files"].append(f)
        else:
            batches.append(current_batch)
            current_batch = {"start": f["start"], "end": f["end"], "files": [f]}

    batches.append(current_batch)
    return batches


# ---------------------------------------------------------------------------
# Decompression (improvement #5) — runs in a thread so it never blocks the
# async event loop.
# ---------------------------------------------------------------------------

def _decompress_slice(compressed_blob: bytes, file_start: int,
                      batch_start: int, file_end: int,
                      batch_end: int, out_path: Path) -> Path:
    """Extract the bytes belonging to one file from a (possibly merged) blob."""
    try:
        import zstandard as zstd
    except ImportError as exc:
        raise ImportError("Install zstandard to download SEC filings archives.") from exc

    # Slice the portion of the decompressed blob that belongs to this file.
    # We decompress the whole merged blob once and slice in memory.
    dctx = zstd.ZstdDecompressor()
    raw = dctx.decompress(compressed_blob)

    # Offsets within the blob
    rel_start = file_start - batch_start
    rel_end   = file_end   - batch_start

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(raw[rel_start:rel_end])
    return out_path


# ---------------------------------------------------------------------------
# Core async download (improvements #3 & #4)
# ---------------------------------------------------------------------------

async def _download_batch(
    session,           # httpx.AsyncClient — shared, persistent (improvement #3)
    tar_url: str,
    batch: dict,
    output_dir: Path,
    filing_date,
    loop,
    decomp_pool,       # concurrent.futures.ThreadPoolExecutor (improvement #5)
    semaphore: asyncio.Semaphore,
) -> list[Path]:
    """Fetch one (possibly merged) byte range and fan out decompression."""
    async with semaphore:
        start, end = batch["start"], batch["end"]
        headers = {"Range": f"bytes={start}-{end - 1}"}

        response = await session.get(tar_url, headers=headers)
        response.raise_for_status()
        if response.status_code != 206:
            raise RuntimeError(
                f"Expected HTTP 206 for range request, got {response.status_code}: {tar_url}"
            )
        compressed_blob = response.content

    # Fan out decompression for every file in this batch — all in parallel
    # inside the thread pool so the event loop stays free (improvement #5).
    decomp_futures = []
    for f in batch["files"]:
        accession_nd = format_accession(f["accession"], "no-dash")
        out_path = output_dir / str(filing_date) / accession_nd / Path(f["filename"]).name
        decomp_futures.append(
            loop.run_in_executor(
                decomp_pool,
                _decompress_slice,
                compressed_blob,
                int(f["start"]),
                start,
                int(f["end"]),
                end,
                out_path,
            )
        )

    return list(await asyncio.gather(*decomp_futures))


# ---------------------------------------------------------------------------
# Per-TAR grouping & dispatch (improvement #6)
# ---------------------------------------------------------------------------

async def _process_rows(
    rows: list[tuple],
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    session,
    loop,
    decomp_pool,
    pbar,
) -> list[Path]:
    """
    Group rows by (filing_date, accession) so all files from the same TAR
    share a merged fetch plan, then dispatch batches concurrently.
    """
    # Group by TAR (improvement #6)
    by_tar: dict[tuple, list[dict]] = defaultdict(list)
    for filing_date, accession, filename, start, end in rows:
        key = (filing_date, format_accession(accession, "no-dash"))
        by_tar[key].append({
            "accession": accession,
            "filename":  filename,
            "start":     int(start),
            "end":       int(end),
        })

    tasks = []
    for (filing_date, accession_nd), files in by_tar.items():
        tar_url = f"{BASE_URL}/{filing_date}/{accession_nd}.tar"
        merged_batches = _merge_ranges(files)   # improvement #2
        for batch in merged_batches:
            tasks.append(
                _download_batch(
                    session=session,
                    tar_url=tar_url,
                    batch=batch,
                    output_dir=output_dir,
                    filing_date=filing_date,
                    loop=loop,
                    decomp_pool=decomp_pool,
                    semaphore=semaphore,
                )
            )

    paths: list[Path] = []
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            paths.extend(result)
        except Exception as exc:
            logger.error("Batch download failed: %s", exc)
        finally:
            pbar.update(1)

    return paths


# ---------------------------------------------------------------------------
# Public entry-point — drop-in replacement for the original download_tar
# ---------------------------------------------------------------------------

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
    quiet=False,
):
    """
    Download and decompress TAR document ranges to output_dir/filingDate/accession/filename.

    Improvements over the original implementation
    ---------------------------------------------
    2. Contiguous / near-contiguous byte ranges for the same TAR are merged
       into a single HTTP request (RANGE_MERGE_GAP controls the tolerance).
    3. A single persistent httpx.AsyncClient is reused across all requests,
       enabling HTTP/2 multiplexing and connection keep-alive.
    4. asyncio replaces ThreadPoolExecutor for I/O, supporting hundreds of
       concurrent requests with negligible memory overhead.
    5. zstd decompression is offloaded to a separate ThreadPoolExecutor so
       CPU work runs in parallel with ongoing downloads.
    6. Rows are grouped by (filing_date, accession) before dispatch so all
       files from the same TAR share a merged fetch plan.
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
            quiet=quiet,
        )
    )


async def _download_tar_async(
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

    from concurrent.futures import ThreadPoolExecutor

    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)  # cap concurrent in-flight requests
    downloaded: list[Path] = []

    # Improvements #3 & #4: single persistent async client with HTTP/2
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
        # Improvement #5: dedicated thread pool for decompression
        with ThreadPoolExecutor(max_workers=decomp_workers,
                                thread_name_prefix="decomp") as decomp_pool:

            with tqdm(total=0, desc="Downloading TAR documents",
                      unit="batch", disable=quiet) as pbar:

                for lookup_page in stream_tar(**stream_kwargs):
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
                        output_dir=output_dir,
                        semaphore=semaphore,
                        session=session,
                        loop=loop,
                        decomp_pool=decomp_pool,
                        pbar=pbar,
                    )
                    downloaded.extend(batch_paths)

    logger.info(
        "TAR archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded