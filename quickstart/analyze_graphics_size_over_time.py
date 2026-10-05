from datamulehub import databases

databases.query(
    """
    SELECT
    substr(CAST(doc.filingdate AS varchar), 1, 4) AS filing_year,
    approx_percentile(
        TRY_CAST(doc.tarendbyte AS bigint) - TRY_CAST(doc.tarstartbyte AS bigint),
        0.5
    ) AS median_doc_size_bytes,
    COUNT(*) AS n_documents
FROM sec_documents_table doc
WHERE doc.documenttype = 'GRAPHIC'
GROUP BY substr(CAST(doc.filingdate AS varchar), 1, 4)
ORDER BY filing_year
    """,
    output_dir="graphics_size",
)