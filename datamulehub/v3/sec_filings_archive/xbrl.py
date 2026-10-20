import logging
from pathlib import Path

from ...api_key import get_api_key
from ...utils.xbrl import iter_xbrl_filings, normalize_accessions
from . import _rust
from .utils import prepare_output_dir, validate_tar_max_size_mb


DEFAULT_MAX_WORKERS = 100
DEFAULT_DECOMP_WORKERS = 8
logger = logging.getLogger(__name__)


def download_xbrl(
    cik=None,
    accession=None,
    submission_type=None,
    filing_date=None,
    report_date=None,
    detected_time=None,
    api_key=None,
    page=None,
    page_size=25_000,
    output_dir="downloads",
    max_workers=DEFAULT_MAX_WORKERS,
    decomp_workers=DEFAULT_DECOMP_WORKERS,
    decompress=True,
    tar_max_size_mb=512,
    overwrite=False,
    ticker=None,
):
    """Download full XBRL fact JSON using the existing XBRL subscription.

    accession must be a non-empty list when supplied. By default JSON files are
    packed into batch_*.tar shards. Set tar_max_size_mb=None for individual files
    at output_dir/filingDate/accession.json; decompress=False retains .json.zst.
    """
    accession = normalize_accessions(accession)
    validate_tar_max_size_mb(tar_max_size_mb)
    if max_workers <= 0 or decomp_workers <= 0:
        raise ValueError("Worker counts must be positive integers.")
    api_key = get_api_key(api_key)
    _rust.require_binary()
    output_dir = Path(output_dir)
    prepare_output_dir(output_dir, overwrite=overwrite)

    def jobs():
        for date, number in iter_xbrl_filings(
            accession=accession,
            cik=cik,
            ticker=ticker,
            submission_type=submission_type,
            filing_date=filing_date,
            report_date=report_date,
            detected_time=detected_time,
            api_key=api_key,
            page=page,
            page_size=page_size,
        ):
            yield {
                "filingDate": date,
                "accession": number,
            }

    downloaded = _rust.run(
        mode="xbrl",
        jobs=jobs(),
        output_dir=output_dir,
        max_workers=max_workers,
        decomp_workers=decomp_workers,
        decompress=decompress,
        tar_max_size_mb=tar_max_size_mb,
        description="Downloading XBRL filings",
        logger=logger,
        api_key=api_key,
    )
    logger.info(
        "XBRL archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded
