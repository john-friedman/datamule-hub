# SEC Filings Lookup

Lookup returns filing locations from the v3 SEC filings archive.

> Note: You probably don't need to read this. Only used for internals.

SGML lookup returns filings as:

```
{
  "accession": [123456789012345678],
  "filingDate": ["2026-05-28"]
}
```

TAR lookup returns filing document byte ranges as:

```
{
  "accession": [123456789012345678],
  "filingDate": ["2026-05-28"],
  "filename": ["primary-document.htm"],
  "start": ["1024"],
  "end": ["4096"]
}
```

Filters can be passed as one item, a list of items, or a tuple for date ranges.

Tuple ranges are supported for:

- `filing_date`
- `report_date`
- `detected_time`

Common filters are:

- `cik`
- `accession`
- `submission_type`
- `filing_date`
- `report_date`
- `detected_time`
- `contains_xbrl`
- `document_type`
- `filename`
- `sequence`

## Usage 
```python
from datamulehub import sec_filings_lookup

# lookup sgml archive filings and return all pages merged
sgml = sec_filings_lookup.lookup_sgml(cik=320193, submission_type="10-K")
print(sgml)

# lookup sgml archive filings by date range
sgml = sec_filings_lookup.lookup_sgml(filing_date=("2026-01-01", "2026-05-28"))
print(sgml)

# lookup tar archive document byte ranges
tar = sec_filings_lookup.lookup_tar(cik=[320193, 789019], document_type=["10-K", "10-Q"])
print(tar)

# lookup one accession, dashed or undashed
tar = sec_filings_lookup.lookup_tar(accession="0000320193-24-000123")
print(tar)

# stream
for page in sec_filings_lookup.stream_sgml(submission_type="10-K"):
    print(page)
```
