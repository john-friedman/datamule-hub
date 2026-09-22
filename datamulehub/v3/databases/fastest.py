import json
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from ...api_key import get_api_key


API_URL = "https://api.datamule.xyz/v3/fastest-sec-filings-metadata"
TABLE_NAME = "fastest_sec_filings_metadata"
DEFAULT_LIMIT = 1000
MAX_LIMIT = 1000
FIELDS = (
    "accession",
    "ciks",
    "submission_type",
    "detected_time",
    "detection_method",
)

_TABLE_PATTERN = re.compile(
    rf"\bfrom\s+(?:[\"`]?{TABLE_NAME}[\"`]?)(?![a-z0-9_])",
    re.IGNORECASE,
)
_QUERY_PATTERN = re.compile(
    rf"""
    ^\s*select\s+(?P<columns>.+?)
    \s+from\s+(?:[\"`]?{TABLE_NAME}[\"`]?)
    (?:\s+where\s+(?P<where>.*?))?
    (?:\s+order\s+by\s+(?P<order_field>[a-z_]+)(?:\s+(?P<order>asc|desc))?)?
    (?:\s+limit\s+(?P<limit>\d+))?
    \s*;?\s*$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)
_CONDITION_PATTERN = re.compile(
    r"^(accession|cik|submission_type|detection_method|detected_time)\s*"
    r"(=|>=|<=|>|<)\s*(.+?)\s*$",
    re.IGNORECASE,
)


def is_fastest_metadata_query(sql):
    return isinstance(sql, str) and _TABLE_PATTERN.search(sql) is not None


def read_fastest_metadata_query(sql, api_key=None):
    columns, params = _parse_query(sql)
    payload = _request(params, api_key)
    rows = [_project(row, columns) for row in payload["results"]]
    _print_billing(payload["billing"])
    return rows


def download_fastest_metadata_query(sql, output_dir=None, api_key=None, quiet=False):
    columns, params = _parse_query(sql)
    payload = _request(params, api_key)
    rows = [_project(row, columns) for row in payload["results"]]

    destination_dir = Path(output_dir or "datamule-query-result")
    destination_dir.mkdir(parents=True, exist_ok=True)
    result_path = destination_dir / "part-00000.parquet"
    _write_parquet(result_path, rows, columns)

    billing = payload["billing"]
    if not quiet:
        print(f"Downloaded query result to {destination_dir}")
        _print_billing(billing)

    return {
        "output_dir": str(destination_dir),
        "files": [str(result_path)],
        "query_id": None,
        "cost": billing.get("total_charge"),
        "remaining_balance": billing.get("remaining_balance"),
        "billing": billing,
    }


def _parse_query(sql):
    if not isinstance(sql, str):
        raise TypeError("sql must be a string.")

    match = _QUERY_PATTERN.fullmatch(sql)
    if match is None:
        raise ValueError(
            f"Unsupported {TABLE_NAME} query. Use SELECT, optional WHERE conditions "
            "joined by AND, optional ORDER BY detected_time, and optional LIMIT."
        )

    columns = _parse_columns(match.group("columns"))
    params = {"limit": _parse_limit(match.group("limit"))}
    order = _parse_order(match.group("order_field"), match.group("order"))
    if order is not None:
        params["order"] = order

    where = match.group("where")
    if where:
        for condition in re.split(r"\s+and\s+", where.strip(), flags=re.IGNORECASE):
            _parse_condition(condition, params)

    categorical = ("cik", "submission_type", "detection_method")
    categorical_count = sum(name in params for name in categorical)
    if categorical_count > 1:
        raise ValueError(
            "Use at most one of cik, submission_type, or detection_method."
        )
    time_filters = ("detected_after", "detected_before")
    if categorical_count and any(name in params for name in time_filters):
        raise ValueError(
            "A categorical filter cannot be combined with a detected_time filter."
        )
    if categorical_count and "order" in params:
        raise ValueError(
            "A categorical filter cannot be combined with ORDER BY."
        )
    if "accession" in params and any(
        name in params for name in (*categorical, *time_filters, "order")
    ):
        raise ValueError(
            "accession cannot be combined with other filters or ORDER BY."
        )

    return columns, params


def _parse_columns(value):
    value = value.strip()
    if value == "*":
        return FIELDS

    columns = tuple(_identifier(column) for column in value.split(","))
    if not columns or any(column not in FIELDS for column in columns):
        raise ValueError(
            f"Columns for {TABLE_NAME} must be selected from: {', '.join(FIELDS)}."
        )
    if len(set(columns)) != len(columns):
        raise ValueError("Duplicate columns are not supported.")
    return columns


def _parse_limit(value):
    if value is None:
        return DEFAULT_LIMIT
    limit = int(value)
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"LIMIT must be between 1 and {MAX_LIMIT}.")
    return limit


def _parse_order(field, order):
    if field is None:
        return None
    if _identifier(field) != "detected_time":
        raise ValueError("Only ORDER BY detected_time is supported.")
    return (order or "asc").lower()


def _parse_condition(condition, params):
    match = _CONDITION_PATTERN.fullmatch(condition.strip())
    if match is None:
        raise ValueError(f"Unsupported WHERE condition: {condition.strip()}")

    field = match.group(1).lower()
    operator = match.group(2)
    value = _literal(match.group(3))

    if field == "detected_time":
        timestamp = _positive_integer(value, "detected_time")
        _add_time_filter(params, operator, timestamp)
        return

    if operator != "=":
        raise ValueError(f"{field} only supports the = operator.")
    if field in params:
        raise ValueError(f"Duplicate {field} filter.")

    if field == "accession":
        params[field] = _format_accession(value)
    elif field == "cik":
        params[field] = _positive_integer(value, field)
    else:
        text = str(value).strip()
        if not text:
            raise ValueError(f"{field} cannot be empty.")
        params[field] = text


def _add_time_filter(params, operator, timestamp):
    if operator in (">", ">="):
        name = "detected_after"
        timestamp -= operator == ">="
    elif operator in ("<", "<="):
        name = "detected_before"
        timestamp += operator == "<="
    else:
        _set_once(params, "detected_after", timestamp - 1)
        _set_once(params, "detected_before", timestamp + 1)
        return
    _set_once(params, name, timestamp)


def _set_once(params, name, value):
    if name in params:
        raise ValueError(f"Duplicate {name} filter.")
    params[name] = value


def _literal(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        quote = value[0]
        return value[1:-1].replace(quote * 2, quote)
    if re.fullmatch(r"\d+", value):
        return int(value)
    raise ValueError(f"Unsupported SQL literal: {value}")


def _identifier(value):
    return value.strip().strip('"`').lower()


def _positive_integer(value, field):
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer.")
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a positive integer.") from error
    if result <= 0:
        raise ValueError(f"{field} must be a positive integer.")
    return result


def _format_accession(value):
    digits = str(value).strip().replace("-", "")
    if not digits.isdigit() or len(digits) > 18:
        raise ValueError("accession must contain at most 18 digits, with optional dashes.")
    accession = int(digits)
    if accession <= 0:
        raise ValueError("accession must be positive.")
    return str(accession)


def _request(params, api_key):
    key = get_api_key(api_key)
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {key}",
            "User-Agent": "datamule-hub",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise _api_error(error) from error
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        raise Exception(f"Fastest filings metadata request failed: {error}") from error

    if not payload.get("success") or not isinstance(payload.get("data"), list):
        raise Exception(
            f"Fastest filings metadata request failed: {payload.get('error')}"
        )

    metadata = payload.get("metadata") or {}
    return {
        "results": payload["data"],
        "billing": metadata.get("billing") or {},
    }


def _api_error(error):
    body = error.read().decode("utf-8", errors="replace")
    try:
        message = json.loads(body).get("error", body)
    except json.JSONDecodeError:
        message = body
    return Exception(f"API request failed ({error.code}): {message}")


def _project(row, columns):
    return {column: row.get(column) for column in columns}


def _write_parquet(path, rows, columns):
    import pyarrow as pa
    import pyarrow.parquet as pq

    types = {
        "accession": pa.string(),
        "ciks": pa.list_(pa.int64()),
        "submission_type": pa.string(),
        "detected_time": pa.int64(),
        "detection_method": pa.string(),
    }
    schema = pa.schema([pa.field(column, types[column]) for column in columns])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _print_billing(billing):
    cost = billing.get("total_charge")
    remaining = billing.get("remaining_balance")
    if cost is not None and remaining is not None:
        print(f"Cost: ${float(cost):.4f} | Remaining balance: ${float(remaining):.2f}")
