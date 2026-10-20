"""Select stored filing archive objects for transfer to a cloud bucket."""

import json

from ..api_key import get_api_key
from ..utils.format_accession import format_accession
from ..utils.xbrl import iter_xbrl_filings
from ..v3.sec_filings_lookup import stream_sgml


SGML_BASE_URL = "https://sec-filings-archive.sgml.datamule.xyz"
TAR_BASE_URL = "https://sec-filings-archive.tar.datamule.xyz"
BATCH_SIZE = 1000


def _iter_filings(api_key, **filters):
    for page in stream_sgml(api_key=api_key, **filters):
        dates = page.get("filingDate", [])
        accessions = page.get("accession", [])
        if len(dates) != len(accessions):
            raise ValueError("Filing lookup returned mismatched accession and filingDate columns.")
        yield from zip(dates, accessions)


def _iter_archive_objects(archive, api_key, **filters):
    if archive == "xbrl":
        filings = iter_xbrl_filings(api_key=api_key, **filters)
    else:
        filings = _iter_filings(api_key, **filters)

    seen = set()
    for filing_date, accession in filings:
        accession = format_accession(accession, "no-dash")
        filing_date = str(filing_date)
        identity = (filing_date, accession)
        if identity in seen:
            continue
        seen.add(identity)

        if archive == "xbrl":
            key = f"{filing_date}/{accession}.json.zst"
            yield {"key": key, "path": f"sec-filings/xbrl/{key}"}
        elif archive == "sgml":
            key = f"{filing_date}/{accession}.sgml.zst"
            yield {"key": key, "url": f"{SGML_BASE_URL}/{key}"}
        else:
            key = f"{filing_date}/{accession}.tar"
            yield {"key": key, "url": f"{TAR_BASE_URL}/{key}"}


def _destination_key(item, prefix):
    return f"{prefix.rstrip('/')}/{item['key']}" if prefix else item["key"]


def _transfer_archive(archive, transfer_batch, credentials, max_workers=4,
                      errors_json_filename="errors_archive_transfer.json", retry_errors=3,
                      prefix=None, api_key=None, **filters):
    if not isinstance(max_workers, int) or max_workers <= 0:
        raise ValueError("max_workers must be a positive integer.")
    if not isinstance(retry_errors, int) or retry_errors < 0:
        raise ValueError("retry_errors must be a non-negative integer.")
    if not credentials or not credentials.get("bucket_name"):
        raise ValueError("Destination credentials must include bucket_name.")
    api_key = get_api_key(api_key)
    failed = []
    total = 0

    def transfer(batch):
        nonlocal total
        results = transfer_batch(batch, credentials, max_workers, retry_errors, prefix, api_key)
        total += len(batch)
        failed.extend(result for result in results if not result["success"])

    batch = []
    for item in _iter_archive_objects(archive, api_key, **filters):
        batch.append(item)
        if len(batch) == BATCH_SIZE:
            transfer(batch)
            batch = []
    if batch:
        transfer(batch)

    if failed and errors_json_filename:
        with open(errors_json_filename, "w") as file:
            json.dump(failed, file, indent=2)
        print(f"Saved {len(failed)} errors to {errors_json_filename}")
    print(f"Transfer complete: {total - len(failed)}/{total} files successful")

