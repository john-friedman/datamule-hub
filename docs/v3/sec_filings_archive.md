# SEC Filings Archive

All three download functions require the Rust archive downloader. Wheels include its
executable. When importing this repository directly, build it first with
`cargo build --release --manifest-path rust/archive-downloader/Cargo.toml`.


## Usage 
```python
from datamulehub import sec_filings_archive

# download decompressed sgml filings into batch_*.tar shards
sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="outfolder")

# download decompressed tar document ranges into batch_*.tar shards
sec_filings_archive.download_tar(cik=320193, document_type=["10-K", "10-Q"], output_dir="outfolder")

# download full XBRL fact JSON as individual files
sec_filings_archive.download_xbrl(cik=320193, submission_type="10-K", output_dir="outfolder", tar_max_size_mb=None)

# download only metadata.json for each matching filing
sec_filings_archive.download_tar(
    cik=320193,
    document_type="metadata",
    output_dir="outfolder",
)

# write individual files instead of tar shards
sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="outfolder", tar_max_size_mb=None)

# keep downloaded archive data compressed
sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="outfolder", decompress=False)

# clear an existing output directory before downloading
sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="outfolder", overwrite=True)

```

By default, archive downloads batch logical output files into tar shards up to
512 MB each. The member paths inside each shard keep the normal layout:

```text
filingDate/accession.sgml
filingDate/accession/filename
filingDate/accession.json
```

Set `tar_max_size_mb=None` to preserve the file-per-document layout:

With `document_type="metadata"`, the downloader uses the filing lookup and two
exact range requests per filing: one for its 512-byte TAR header and one for the
declared `metadata.json` payload.

```text
outfolder/filingDate/accession.sgml
outfolder/filingDate/accession/filename
outfolder/filingDate/accession.json
```

`output_dir` must be empty unless `overwrite=True` is passed.

## XBRL

`sec_filings_archive.download_xbrl` downloads full XBRL fact JSON from the existing
S3 archive using an XBRL subscription.

```python
from datamulehub import sec_filings_archive

sec_filings_archive.download_xbrl(
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

Output defaults match the other filing archive downloads: concurrent downloads,
decompression, and TAR shards up to 512 MB. `tar_max_size_mb=None` writes individual
`filingDate/accession.json` files. `decompress=False` retains `.json.zst` files.
The return value is a list of output paths. Missing lookups or S3 objects are
logged; authorization, transport, and decompression errors raise an exception.
