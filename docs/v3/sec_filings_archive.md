# SEC Filings Archive


## Usage 
```python
from datamulehub import sec_filings_archive

# download decompressed sgml filings into batch_*.tar shards
sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="outfolder")

# download decompressed tar document ranges into batch_*.tar shards
sec_filings_archive.download_tar(cik=320193, document_type=["10-K", "10-Q"], output_dir="outfolder")

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
```

Set `tar_max_size_mb=None` to preserve the file-per-document layout:

With `document_type="metadata"`, the downloader uses the filing lookup and two
exact range requests per filing: one for its 512-byte TAR header and one for the
declared `metadata.json` payload.

```text
outfolder/filingDate/accession.sgml
outfolder/filingDate/accession/filename
```

`output_dir` must be empty unless `overwrite=True` is passed.
