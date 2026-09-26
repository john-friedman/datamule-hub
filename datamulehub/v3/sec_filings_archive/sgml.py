import logging
from pathlib import Path
from typing import Optional, Union

from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_sgml
from . import _rust
from .utils import prepare_output_dir, validate_tar_max_size_mb


DEFAULT_MAX_WORKERS = 100
DEFAULT_DECOMP_WORKERS = 8
logger = logging.getLogger(__name__)


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

    Uses the Rust archive downloader for HTTP, decompression, and tar batching.
    """
    return _download_sgml_rust(
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


def _download_sgml_rust(
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
    if decomp_workers <= 0:
        raise ValueError("decomp_workers must be a positive integer.")
    _rust.require_binary()
    prepare_output_dir(output_dir, overwrite=overwrite)

    def jobs():
        for lookup_page in stream_sgml(**stream_kwargs):
            for filing_date, accession in zip(
                lookup_page["filingDate"],
                lookup_page["accession"],
            ):
                yield {
                    "filingDate": str(filing_date),
                    "accession": format_accession(accession, "no-dash"),
                }

    downloaded = _rust.run(
        mode="sgml",
        jobs=jobs(),
        output_dir=output_dir,
        max_workers=max_workers,
        decomp_workers=decomp_workers,
        decompress=decompress,
        tar_max_size_mb=tar_max_size_mb,
        description="Downloading SGML filings",
        logger=logger,
    )
    logger.info(
        "SGML archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded
