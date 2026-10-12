from datamulehub import databases

# $0.0099 as of 10/12/2026
databases.query(
    """
    SELECT DISTINCT
        'https://www.sec.gov/Archives/edgar/data/'
        || CAST(s.cik AS varchar) || '/'
        || REPLACE(s.accessionnumber, '-', '') || '/'
        || s.accessionnumber || '-index.html' AS sec_index_url,
        d.submissiontype AS filing_reported_type,
        s."form" AS sec_endpoint_type
    FROM submissions_metadata s
    JOIN sec_submission_details_table d
      ON CAST(REPLACE(s.accessionnumber, '-', '') AS bigint)
       = CAST(d.accessionnumber AS bigint)
    WHERE s."form" <> d.submissiontype
      AND s.filingdate >= '2025-01-01'
      AND d.filingdate >= '2025-01-01'
    """,
    output_dir="submission_type_mismatches",
)