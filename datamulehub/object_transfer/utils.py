from datetime import datetime, timedelta
from ..v3.databases import read_query
from ..utils.format_accession import format_accession

def _generate_dates(filing_date):
    if isinstance(filing_date, str):
        return [filing_date]
    elif isinstance(filing_date, list):
        return filing_date
    elif isinstance(filing_date, tuple):
        start = datetime.strptime(filing_date[0], '%Y-%m-%d')
        end = datetime.strptime(filing_date[1], '%Y-%m-%d')
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        return dates
    raise ValueError('filing_date must be a string, list, or (start, end) tuple')

def _sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"

def _normalize_accession(value):
    return str(value).replace("-", "")

def _sql_value(value, quote=True):
    return _sql_literal(value) if quote else str(value)

def _sql_filter(column, value, transform=None, quote=True):
    if value is None:
        return None

    if isinstance(value, tuple):
        start = transform(value[0]) if transform else value[0]
        end = transform(value[1]) if transform else value[1]
        return f"{column} BETWEEN {_sql_value(start, quote)} AND {_sql_value(end, quote)}"

    if isinstance(value, (list, set)):
        values = [transform(item) if transform else item for item in value]
        return f"{column} IN ({', '.join(_sql_value(item, quote) for item in values)})"

    value = transform(value) if transform else value
    return f"{column} = {_sql_value(value, quote)}"

def _get_urls(submission_type=None, cik=None, filing_date=None, accession_number=None):
    filters = [
        _sql_filter("cik", cik, quote=False),
        _sql_filter("form", submission_type),
        _sql_filter("filingdate", filing_date),
        _sql_filter("accessionnumber", accession_number, _normalize_accession, quote=False),
    ]
    where = " AND ".join(item for item in filters if item)
    if where:
        where = f"WHERE {where}"

    rows = read_query(f"""
        SELECT accessionnumber
        FROM submissions_metadata
        {where}
        ORDER BY filingdate, accessionnumber
    """)
    return [f"https://sec-library.datamule.xyz/{format_accession(row['accessionnumber'], 'no-dash')}.sgml" for row in rows]
