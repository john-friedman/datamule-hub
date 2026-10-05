"""Resolve listed-company tickers to CIKs for filing archive requests."""

import ast
import csv
import gzip
import io
import logging
import os
import tempfile
import threading
import time
import urllib.request
from pathlib import Path


_METADATA_URL = (
    "https://github.com/john-friedman/datamule-data/raw/master/"
    "data/filer_metadata/listed_filer_metadata.csv.gz"
)
_CACHE_AGE_SECONDS = 24 * 60 * 60
_CACHE_NAME = "listed_filer_metadata.csv"
_LOCK = threading.Lock()
_loaded_path = None
_loaded_mtime_ns = None
_loaded_tickers = None
logger = logging.getLogger(__name__)


def _parse_tickers(data):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig")))
    if not reader.fieldnames or not {"cik", "tickers"}.issubset(reader.fieldnames):
        raise ValueError("Listed-filer metadata has no CIK or tickers column.")

    ticker_map = {}
    for row in reader:
        cik = int(row["cik"])
        tickers = ast.literal_eval(row["tickers"])
        if not isinstance(tickers, list):
            raise ValueError("Listed-filer metadata has an invalid tickers value.")
        for ticker in tickers:
            if not isinstance(ticker, str):
                raise ValueError("Listed-filer metadata has a non-string ticker.")
            symbol = ticker.strip().upper()
            if symbol:
                matches = ticker_map.setdefault(symbol, [])
                if cik not in matches:
                    matches.append(cik)

    if not ticker_map:
        raise ValueError("Listed-filer metadata contains no tickers.")
    return ticker_map


def _cache_path():
    return Path.home() / ".datamulehub" / _CACHE_NAME


def _ticker_map():
    global _loaded_path, _loaded_mtime_ns, _loaded_tickers

    path = _cache_path()
    with _LOCK:
        cached = path.is_file()
        if not cached or time.time() - path.stat().st_mtime >= _CACHE_AGE_SECONDS:
            try:
                with urllib.request.urlopen(_METADATA_URL, timeout=30) as response:
                    data = gzip.decompress(response.read())
                parsed = _parse_tickers(data)
                path.parent.mkdir(parents=True, exist_ok=True)
                fd, temporary_path = tempfile.mkstemp(
                    prefix="listed_filer_metadata-", suffix=".tmp", dir=path.parent
                )
                try:
                    with os.fdopen(fd, "wb") as cache_file:
                        cache_file.write(data)
                    os.replace(temporary_path, path)
                finally:
                    if os.path.exists(temporary_path):
                        os.unlink(temporary_path)
                _loaded_path = path
                _loaded_mtime_ns = path.stat().st_mtime_ns
                _loaded_tickers = parsed
                return parsed
            except (OSError, ValueError, EOFError, SyntaxError) as exc:
                if not cached:
                    raise RuntimeError("Unable to load listed-filer ticker metadata.") from exc
                logger.warning("Unable to refresh ticker metadata; using cached copy: %s", exc)

        mtime_ns = path.stat().st_mtime_ns
        if _loaded_tickers is None or _loaded_path != path or _loaded_mtime_ns != mtime_ns:
            _loaded_tickers = _parse_tickers(path.read_bytes())
            _loaded_path = path
            _loaded_mtime_ns = mtime_ns
        return _loaded_tickers


def resolve_cik_filter(cik, ticker):
    """Return CIKs for a ticker filter, or leave an existing CIK filter intact."""
    if ticker is None:
        return cik
    if cik is not None:
        raise ValueError("Provide either cik or ticker, not both.")

    if isinstance(ticker, str):
        tickers = [ticker]
    elif isinstance(ticker, (list, tuple, set, frozenset)):
        tickers = list(ticker)
    else:
        raise TypeError("ticker must be a string or a collection of strings.")

    if not tickers or any(not isinstance(item, str) or not item.strip() for item in tickers):
        raise ValueError("ticker must contain at least one non-empty string.")

    ticker_map = _ticker_map()
    ciks = []
    seen_ciks = set()
    unknown = []
    seen_unknown = set()
    for item in tickers:
        symbol = item.strip().upper()
        matches = ticker_map.get(symbol)
        if not matches:
            if symbol not in seen_unknown:
                seen_unknown.add(symbol)
                unknown.append(symbol)
        else:
            for match in matches:
                if match not in seen_ciks:
                    seen_ciks.add(match)
                    ciks.append(match)

    if unknown:
        raise ValueError(f"No CIK found for ticker(s): {', '.join(unknown)}")
    return ciks
