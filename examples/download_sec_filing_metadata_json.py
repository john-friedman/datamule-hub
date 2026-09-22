from datamulehub import sec_filings_archive


for year in range(1999, 2000):
    sec_filings_archive.download_tar(
        filing_date=(f"{year}-01-01", f"{year}-12-31"),
        document_type="metadata",
        output_dir=f"metadata/{year}",
    )
