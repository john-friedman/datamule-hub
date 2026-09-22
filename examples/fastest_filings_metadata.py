import logging

from datamulehub import databases


logging.basicConfig(level=logging.INFO)


rows =databases.read_query(
    """
    SELECT accession, ciks, submission_type, detected_time, detection_method
    FROM fastest_sec_filings_metadata
    WHERE detection_method = 'index_url'
    LIMIT 100
    """
)

print(rows)
