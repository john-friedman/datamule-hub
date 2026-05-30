# SEC Filings Archive


## Usage 
```python
from datamulehub import sec_filings_archive

# download decompressed sgml filings to outfolder/filingDate/accession.sgml
sec_filings_archive.download_sgml(cik=320193, submission_type="10-K", output_dir="outfolder")

# download tar document ranges to outfolder/filingDate/accession/filename
sec_filings_archive.download_tar(cik=320193, document_type=["10-K", "10-Q"], output_dir="outfolder")
```
