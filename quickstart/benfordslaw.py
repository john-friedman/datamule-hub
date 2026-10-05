from datamulehub import databases

# $1.317 as of 10/5/2026
databases.query(
    """
    SELECT acc AS accessionnumber, c.cik, d.submissiontype, taxonomy, digit, n
    FROM (
        SELECT accessionnumber AS acc, taxonomy,
               CAST(regexp_extract(value, '[1-9]') AS integer) AS digit,
               count(*) AS n
        FROM simple_xbrl
        WHERE regexp_like(value, '^-?[0-9]+([.][0-9]+)?$')
          AND regexp_like(value, '[1-9]')
        GROUP BY 1, 2, 3
    )
    JOIN sec_accession_cik_table c ON acc = c.accessionnumber
    JOIN sec_submission_details_table d ON acc = d.accessionnumber
    ORDER BY 1, 2, 3, 4, 5
    """,
    output_dir="benford_by_accession_cik_taxonomy_type",
)
