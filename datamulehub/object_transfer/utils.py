from datetime import datetime, timedelta
from ..query_database import query_database
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

def _get_urls(submission_type=None, cik=None, filing_date=None, accession_number=None):
    results = query_database('sec-filings-lookup',
        cik=cik,
        submissionType=submission_type,
        filingDate=filing_date,
        accessionNumber=accession_number
    )
    return [f"https://sec-library.datamule.xyz/{format_accession(result['accessionNumber'], 'no-dash')}.sgml" for result in results]