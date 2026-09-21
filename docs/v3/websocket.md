# Websocket

Websocket emits filing objects as

```
{
    "accession": "000032019326000001",
    "submission_type": "8-K",
    "ciks": ["320193"],
    "filing_date": "2026-05-28",
    "source": "Rss",
    "detected_time": 1779999999,
    "anticipated_time": None,
    "event_type": "sec_submission_detected",
    "version": 1,
    "created_at": 1779999999000,
}
```

## Usage

```python
from datamulehub import sec_filings_notifications

for filing in sec_filings_notifications.stream_filings():
    if filing["submission_type"] == "10-K":
        print("10-K:", filing)
```

`stream_filings()` reconnects automatically if the socket closes, the
connection stops answering pings, or no filing/heartbeat messages are received
for the idle timeout.

```python
for filing in sec_filings_notifications.stream_filings(
    idle_timeout=300,
    ping_interval=60,
    ping_timeout=10,
    recv_timeout=30,
):
    print(filing)
```

## Fastest filings

The fastest filings stream charges $0.25 when it issues a service token. The
package reuses that token for transport reconnects and requests another only
after the token expires at the 05:00 UTC reset.

```python
from datamulehub import sec_filings_notifications

for filing in sec_filings_notifications.stream_fastest_filings():
    print(filing)
```

Events contain `accession`, `cik`, `detected_time`, and `detection_method`.
