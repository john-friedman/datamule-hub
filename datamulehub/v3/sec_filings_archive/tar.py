import logging
from pathlib import Path
from typing import Optional, Union

from ...utils.format_accession import format_accession
from ..sec_filings_lookup import stream_sgml, stream_tar
from . import _rust
from .utils import prepare_output_dir, validate_tar_max_size_mb


DEFAULT_MAX_WORKERS = 100
DEFAULT_DECOMP_WORKERS = 8
logger = logging.getLogger(__name__)


def _is_document_mode(document_type, filename, sequence) -> bool:
    return not _is_metadata_mode(document_type) and (
        document_type is not None or filename is not None or sequence is not None
    )


def _is_metadata_mode(document_type) -> bool:
    return isinstance(document_type, str) and document_type.lower() == "metadata"


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

    Set document_type="metadata" to download only each matching filing's
    metadata.json using exact byte-range requests.
    """
    return _download_tar_rust(
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


def _download_tar_rust(
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

    metadata_mode = _is_metadata_mode(stream_kwargs.get("document_type"))
    document_mode = _is_document_mode(
        stream_kwargs.get("document_type"),
        stream_kwargs.get("filename"),
        stream_kwargs.get("sequence"),
    )

    def jobs():
        if metadata_mode:
            lookup_kwargs = {**stream_kwargs, "document_type": None}
            lookup_pages = stream_sgml(**lookup_kwargs)
        elif document_mode:
            lookup_pages = stream_tar(**stream_kwargs)
        else:
            lookup_pages = stream_sgml(**stream_kwargs)

        for lookup_page in lookup_pages:
            if document_mode:
                rows = zip(
                    lookup_page["filingDate"],
                    lookup_page["accession"],
                    lookup_page["filename"],
                    lookup_page["start"],
                    lookup_page["end"],
                )
                for filing_date, accession, filename, start, end in rows:
                    yield {
                        "filingDate": str(filing_date),
                        "accession": format_accession(accession, "no-dash"),
                        "filename": filename,
                        "start": int(start),
                        "end": int(end),
                    }
            else:
                seen = set()
                rows = zip(lookup_page["filingDate"], lookup_page["accession"])
                for filing_date, accession in rows:
                    accession_nd = format_accession(accession, "no-dash")
                    key = (str(filing_date), accession_nd)
                    if key in seen:
                        continue
                    seen.add(key)
                    yield {
                        "filingDate": key[0],
                        "accession": key[1],
                    }

    mode = "metadata" if metadata_mode else "range" if document_mode else "submission"

    downloaded = _rust.run(
        mode=mode,
        jobs=jobs(),
        output_dir=output_dir,
        max_workers=max_workers,
        decomp_workers=decomp_workers,
        decompress=decompress,
        tar_max_size_mb=tar_max_size_mb,
        description="Downloading TAR filings",
        logger=logger,
    )
    logger.info(
        "TAR archive download complete: files=%s output_dir=%s",
        len(downloaded), output_dir,
    )
    return downloaded
