from datamulehub import databases

# submission_metadata is an endpoint that reports metadata generated from filings
# filing_metadata_filings is extracted from filings themselves, by datamule
# one quirk: Filings in filing_metadata_filings before Jan 2025 are from the SEC's bulk archive, which sometimes is slightly different.

# $0.01 as of 10/12/2026
databases.query(
    """
    SELECT DISTINCT
        'https://www.sec.gov/Archives/edgar/data/'
        || CAST(s.cik AS varchar) || '/'
        || REPLACE(s.accessionnumber, '-', '') || '/'
        || s.accessionnumber || '-index.html' AS sec_index_url,
        f."type" AS filing_reported_type,
        s."form" AS sec_endpoint_type
    FROM submissions_metadata s
    JOIN filing_metadata_filings f
      ON CAST(REPLACE(s.accessionnumber, '-', '') AS bigint)
       = CAST(f.accessionnumber AS bigint)
    WHERE s."form" <> f."type"
      AND s.filingdate >= '2025-01-01'
      AND f.filingdate >= '2025-01-01'
    """,
    output_dir="submission_type_mismatches",
)