import json
import urllib.error
import urllib.request

from .api_key import api_key


API_BASE_URL = "https://api.datamule.xyz/sec-filings-webhooks"


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


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


def add_endpoints(endpoints, submission_types=None, ciks=None, all_filings=False):
    """
    Register one or more HTTPS endpoints for SEC filing webhook notifications.

    The endpoint must handle AWS SNS subscription confirmation before it will
    receive filing notifications.
    """
    body = {
        "endpoints": _as_list(endpoints),
        "mode": "all" if all_filings else "filter",
    }

    if not all_filings:
        body["submission_types"] = _as_list(submission_types)
        body["ciks"] = _as_list(ciks)

    return _request("POST", body=body)


def list_endpoints():
    """List the user's registered SEC filing webhook endpoints."""
    return _request("GET")


def remove_endpoints(ids=None, endpoints=None):
    """Remove SEC filing webhook endpoints by Datamule webhook ID or URL."""
    ids = _as_list(ids)
    endpoints = _as_list(endpoints)

    if len(ids) == 1 and not endpoints:
        return _request("DELETE", path=f"/{ids[0]}")

    body = {}
    if ids:
        body["ids"] = ids
    if endpoints:
        body["endpoints"] = endpoints

    return _request("DELETE", body=body)

