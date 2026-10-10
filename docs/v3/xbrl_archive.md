# XBRL Archive

`xbrl_archive.download_xbrl` downloads full XBRL fact JSON from the existing S3
archive using an XBRL subscription. Wheels include the Rust downloader. When
using the source checkout, rebuild `rust/archive-downloader` after updating it.

```python
from datamulehub import xbrl_archive

xbrl_archive.download_xbrl(cik=320193, submission_type="10-K", output_dir="apple_xbrl")
xbrl_archive.download_xbrl(
    accession=["0000320193-24-000123"],
    output_dir="filing_xbrl",
    tar_max_size_mb=None,
)
```

`accession` must be a non-empty list when supplied, even for one filing. Each item
may be a dashed string, undashed string, or integer accession number. Filing
filters support cik, ticker, submission_type, filing_date, report_date, and
detected_time, with the same date-range and pagination options as filing lookups.
Only filings marked as containing XBRL are selected.

The source objects are `sec-filings/xbrl/filingDate/accession.json.zst`, containing
parsed facts, attributes, and contexts. They use the existing S3 download API and
normal S3 download billing. Each job obtains its download link immediately before
fetching the object. Explicit api_key overrides are passed to the Rust process.

Output defaults match the filing archive: concurrent downloads, decompression,
and TAR shards up to 512 MB. `tar_max_size_mb=None` writes individual
`filingDate/accession.json` files. `decompress=False` retains `.json.zst` files.
The output directory must be empty unless overwrite=True is supplied. The return
value is a list of output paths. Missing lookups or S3 objects are logged;
authorization, transport, and decompression errors raise an exception.
