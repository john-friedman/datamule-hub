import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request


API_BASE_URL = "https://api.datamule.xyz/v3/sec-filings-lookup"
DEFAULT_PAGE_SIZE = 25000
logger = logging.getLogger(__name__)

_PARAM_MAP = {
    "accession": "accessionNumber",
    "submission_type": "submissionType",
    "filing_date": "filingDate",
    "report_date": "reportDate",
    "detected_time": "detectedTime",
    "contains_xbrl": "containsXBRL",
    "document_type": "documentType",
}
_RANGE_PARAMS = {"filingDate", "reportDate", "detectedTime"}


def _get_api_key(api_key):
    key = api_key or os.environ.get("DATAMULE_API_KEY")
    if not key:
        raise EnvironmentError("DATAMULE_API_KEY environment variable is not set.")
    return key


def _stringify(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _format_accession(value):
    return str(int(str(value).replace("-", "")))


def _add_param(params, name, value):
    if value is None:
        return

    if name == "accessionNumber":
        if isinstance(value, (list, tuple, set)):
            params[name] = ",".join(_format_accession(item) for item in value)
        else:
            params[name] = _format_accession(value)
        return

    if name in _RANGE_PARAMS and isinstance(value, tuple):
        if len(value) != 2:
            raise ValueError(f"{name} range must be a 2-item tuple.")
        params[f"{name}_START"] = _stringify(value[0])
        params[f"{name}_END"] = _stringify(value[1])
        return

    if isinstance(value, (list, tuple, set)):
        params[name] = ",".join(_stringify(item) for item in value)
        return

    params[name] = _stringify(value)


def _build_params(filters):
    params = {}
    for key, value in filters.items():
        if value is None:
            continue

        name = _PARAM_MAP.get(key, key)
        _add_param(params, name, value)

    return params


def _request_page(endpoint, params, api_key):
    query = urllib.parse.urlencode(params)
    url = f"{API_BASE_URL}/{endpoint}"
    if query:
        url = f"{url}?{query}"

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "datamule-hub",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8")
        try:
            error_payload = json.loads(error_body)
            message = error_payload.get("error", error_body)
        except json.JSONDecodeError:
            message = error_body
        raise Exception(f"API request failed ({exc.code}): {message}") from exc

    if not payload.get("success"):
        raise Exception(f"API request failed: {payload.get('error')}")

    return payload


def _validate_columnar_results(page_data):
    if not isinstance(page_data, dict):
        raise ValueError("Expected columnar lookup response data.")

    for column, values in page_data.items():
        if not isinstance(values, list):
            raise ValueError(f"Expected columnar lookup values for {column}.")


def _merge_columnar_results(target, page_data):
    _validate_columnar_results(page_data)

    for column, values in page_data.items():
        target.setdefault(column, []).extend(values)


def _rows_returned(page_data):
    if not page_data:
        return 0
    return max(len(values) for values in page_data.values())


def _stream_lookup(endpoint, api_key=None, page=None, page_size=DEFAULT_PAGE_SIZE, **filters):
    key = _get_api_key(api_key)
    base_params = _build_params(filters)
    base_params["pageSize"] = page_size

    current_page = page or 1
    single_page = page is not None
    pages = 0
    rows = 0
    total_charge = 0
    remaining_balance = None
    start_time = time.time()

    logger.info(
        "SEC filings lookup started: endpoint=%s page=%s page_size=%s",
        endpoint,
        current_page,
        page_size,
    )

    while True:
        params = base_params.copy()
        params["page"] = current_page

        payload = _request_page(endpoint, params, key)
        page_data = payload.get("data", {})
        _validate_columnar_results(page_data)

        metadata = payload.get("metadata", {})
        billing = metadata.get("billing", {})
        pagination = metadata.get("pagination", {})
        page_rows = _rows_returned(page_data)
        page_charge = billing.get("total_charge", 0) or 0
        pages += 1
        rows += page_rows
        total_charge += page_charge
        remaining_balance = billing.get("remaining_balance", remaining_balance)
        has_more = pagination.get("hasMore", False)

        logger.info(
            "SEC filings lookup page fetched: endpoint=%s page=%s rows=%s has_more=%s page_charge=%s elapsed=%.1fs",
            endpoint,
            current_page,
            page_rows,
            has_more,
            page_charge,
            time.time() - start_time,
        )

        yield page_data

        if single_page or not has_more:
            logger.info(
                "SEC filings lookup complete: endpoint=%s pages=%s rows=%s total_charge=%s remaining_balance=%s elapsed=%.1fs",
                endpoint,
                pages,
                rows,
                total_charge,
                remaining_balance,
                time.time() - start_time,
            )
            return

        current_page += 1


def _lookup(endpoint, api_key=None, page=None, page_size=DEFAULT_PAGE_SIZE, **filters):
    results = {}
    for page_data in _stream_lookup(
        endpoint,
        api_key=api_key,
        page=page,
        page_size=page_size,
        **filters,
    ):
        _merge_columnar_results(results, page_data)
    return results


def lookup_sgml(
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
    page_size=DEFAULT_PAGE_SIZE,
):
    """
    Return SGML filing lookup columns: accession and filingDate.
    """
    return _lookup(
        "sgml-lookup",
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
    )


def stream_sgml(
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
    page_size=DEFAULT_PAGE_SIZE,
):
    """
    Yield SGML filing lookup columns one page at a time.
    """
    yield from _stream_lookup(
        "sgml-lookup",
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
    )


def lookup_tar(
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
    page_size=DEFAULT_PAGE_SIZE,
):
    """
    Return TAR byte-range lookup columns: accession, filingDate, start, and end.
    """
    return _lookup(
        "tar-lookup",
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
    )


def stream_tar(
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
    page_size=DEFAULT_PAGE_SIZE,
):
    """
    Yield TAR byte-range lookup columns one page at a time.
    """
    yield from _stream_lookup(
        "tar-lookup",
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
    )
