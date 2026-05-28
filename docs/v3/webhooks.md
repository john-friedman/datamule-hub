# Webhooks

SNS posts filings as:

```
{
  "version": 1,
  "event_type": "sec_submission_detected",
  "accession": 123456789012345678,
  "submission_type": "10-K",
  "ciks": [320193],
  "filing_date": "2026-05-28",
  "source": "Rss",
  "detected_time": 1779999999
}
```

Where message attributes are `submission_type`, `source`, and `cik`.

Current source values are:

- `Rss`
- `Efts`
- `anticipate`

The Python client defaults to all sources. To exclude anticipated filings, pass `sources=["Rss", "Efts"]`. Note that if `anticipate` is enabled, filings will emit twice. First if anticipated, second when RSS/EFTS detects the filing. If only RSS and EFTS is enabled, filings will emit once.

## Usage 
```python
from datamulehub import sec_filings_notifications

# add an endpoint, filter by submission types, cik codes, or source of filing detection
print(sec_filings_notifications.add_endpoint(endpoint="https://example.com/sec-filings", submission_types=None, ciks=None, sources=["Rss","Efts"]))

# list endpoints
endpoints = sec_filings_notifications.list_endpoints()
print(endpoints)

# remove an endpoint by endpoint url or id
for endpoint in endpoints["data"]:
    print(sec_filings_notifications.remove_endpoint(id=endpoint["id"]))
```
