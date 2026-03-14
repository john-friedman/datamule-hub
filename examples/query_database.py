from datamule_hub import query_database

result = query_database('sec-filings-lookup',
    cik=320193,
    submissionType='10-K',
    filingDate=('2024-01-01', '2024-12-31'),
    containsXBRL=1)

print(result)