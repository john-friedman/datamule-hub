import logging

from datamulehub import sec_filings_archive


# Hub hides logs by default.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Download every MSFT 8-K and 8-K/A in batch TAR files for better speed.
# $0.00281 as of 10/5/2026
sec_filings_archive.download_tar(
    ticker="MSFT",
    submission_type=["8-K", "8-K/A"],
    output_dir="msft_8k_batch",
)

input("Proceed?")

# Download every MSFT 8-K and 8-K/A as individual files.
# $0.00281 as of 10/5/2026
sec_filings_archive.download_tar(
    ticker="MSFT",
    submission_type=["8-K", "8-K/A"],
    output_dir="msft_8k_individual",
    tar_max_size_mb=None,  # Write individual filing documents.
)

input("Proceed?")

# Download only the 8-K document within MSFT's 8-K filings. An 8-K filing has an 8-K root form as well as other attachments.
# $0.00281 as of 10/5/2026
sec_filings_archive.download_tar(
    ticker="MSFT",
    submission_type=["8-K", "8-K/A"],
    document_type=["8-K", "8-K/A"],
    output_dir="msft_8k_documents_only",
)

input("Proceed?")

# Filter by date range.
sec_filings_archive.download_tar(
    ticker="MSFT",
    submission_type=["8-K", "8-K/A"],
    document_type=["8-K", "8-K/A"],
    filing_date=("2026-01-01", "2026-10-05"),
    report_date=("2026-01-01", "2026-10-05"),
    output_dir="msft_8k_date_range",
)

input("Proceed?")


# Other optional filters: accession, detected_time, contains_xbrl, cik, filename, and sequence.
# detected_time : when Datamule detected the filing
# filename: filename within filing
# sequence: sequence of file within filing.
sec_filings_archive.download_tar(
    cik=789019, # MSFT
    submission_type=["8-K", "8-K/A"],
    # accession="0000789019-26-000123", 
    # detected_time=(START_TIMESTAMP, END_TIMESTAMP),
    # contains_xbrl=True,
    # filename="primary-document.htm", 
    # sequence=1,
    output_dir="msft_8k_filtered",
)

input("Proceed?")


# Download every MSFT 8-K and 8-K/A in batch TAR files as original SGML.
# $0.00281 as of 10/5/2026
sec_filings_archive.download_sgml(
    ticker="MSFT",
    submission_type=["8-K", "8-K/A"],
    output_dir="msft_8k_sgml_batch",
)

input("Proceed?")

# Download every MSFT 8-K and 8-K/A as individual SGML files.
# $0.00281 as of 10/5/2026
sec_filings_archive.download_sgml(
    ticker="MSFT",
    submission_type=["8-K", "8-K/A"],
    output_dir="msft_8k_sgml_individual",
    tar_max_size_mb=None,  # Write individual .sgml files.
)
