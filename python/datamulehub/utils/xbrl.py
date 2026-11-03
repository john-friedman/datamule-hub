import logging
import re

from ..v3.sec_filings_lookup import stream_sgml
from .format_accession import format_accession


ACCESSION_BATCH_SIZE = 200
logger = logging.getLogger(__name__)


def normalize_accessions(accession):
    if accession is None:
        return None
    if not isinstance(accession, list):
        raise TypeError("accession must be a list, including for a single accession.")
    if not accession:
        raise ValueError("accession must be a non-empty list.")

    normalized = []
    seen = set()
    for value in accession:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError("Each accession must be a string or integer accession number.")
        value = str(value).strip()
        if not re.fullmatch(r"(?:[0-9]{1,18}|[0-9]{10}-[0-9]{2}-[0-9]{6})", value):
            raise ValueError(f"Invalid accession: {value!r}")
        if int(value.replace("-", "")) == 0:
            raise ValueError("Accession numbers must be positive.")
        value = format_accession(value, "no-dash")
        if value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def iter_xbrl_filings(accession=None, api_key=None, **filters):
    accession = normalize_accessions(accession)
    batches = (
        [accession[index:index + ACCESSION_BATCH_SIZE] for index in range(0, len(accession), ACCESSION_BATCH_SIZE)]
        if accession is not None else [None]
    )
    seen = set()
    found = set()
    for batch in batches:
        for data in stream_sgml(accession=batch, contains_xbrl=True, api_key=api_key, **filters):
            dates = data.get("filingDate", [])
            numbers = data.get("accession", [])
            if len(dates) != len(numbers):
                raise ValueError("Filing lookup returned mismatched accession and filingDate columns.")
            for filing_date, number in zip(dates, numbers):
                number = format_accession(number, "no-dash")
                if batch is not None and number not in batch:
                    raise ValueError("Filing lookup returned an accession outside the requested batch.")
                key = (str(filing_date), number)
                if accession is not None:
                    found.add(number)
                if key not in seen:
                    seen.add(key)
                    yield key

    if accession is not None:
        for number in accession:
            if number not in found:
                logger.warning("No XBRL filing found for accession %s", number)
