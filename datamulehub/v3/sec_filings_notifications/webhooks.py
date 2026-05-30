import json
import logging
import urllib.error
import urllib.request

from ...api_key import api_key


API_BASE_URL = "https://api.datamule.xyz/v3/sec-filings-webhooks"
DEFAULT_SOURCES = ("Rss", "Efts", "anticipate")
logger = logging.getLogger(__name__)


def _normalize_sources(sources):

    if sources is None:
        return list(DEFAULT_SOURCES)

    allowed = ", ".join(DEFAULT_SOURCES)

    if not sources:
        raise ValueError(f"Provide at least one source. Allowed sources: {allowed}.")

    result = []
    for source in sources:
        source = str(source).strip()
        if source not in DEFAULT_SOURCES:
            raise ValueError(f"Invalid source: {source}. Allowed sources: {allowed}.")
        result.append(source)
    return result


def _request(method, path="", body=None):
    url = API_BASE_URL + path
    data = None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "datamule-hub",
    }

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8")
        raise Exception(f"API request failed ({exc.code}): {error_body}") from exc


def add_endpoint(endpoint, submission_types=None, ciks=None, sources=None):
    """
    Register one HTTPS endpoint for SEC filing webhook notifications.

    The endpoint must handle AWS SNS subscription confirmation before it will
    receive filing notifications.
    """
    body = {
        "endpoint": endpoint,
        "mode": "filter" if submission_types or ciks else "all",
        "sources": _normalize_sources(sources),
    }

    if submission_types:
        body["submission_types"] = submission_types
    if ciks:
        body["ciks"] = ciks

    result = _request("POST", body=body)
    logger.info("Webhook endpoint added: %s", endpoint)
    return result


def list_endpoints():
    """List the user's registered SEC filing webhook endpoints."""
    result = _request("GET")
    logger.info("Webhook endpoints listed")
    return result


def remove_endpoint(id=None, endpoint=None):
    """Remove one SEC filing webhook endpoint by Datamule webhook ID or URL."""
    if (id is None) == (endpoint is None):
        raise ValueError("Provide exactly one of id or endpoint.")

    if id is not None:
        result = _request("DELETE", path=f"/{id}")
        logger.info("Webhook endpoint removed: id=%s", id)
        return result

    result = _request("DELETE", body={"endpoint": endpoint})
    logger.info("Webhook endpoint removed: %s", endpoint)
    return result
