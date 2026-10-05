from datamulehub import sec_filings_notifications

# add an endpoint, filter by submission types, cik codes, or source of filing detection
print(sec_filings_notifications.add_endpoint(endpoint="https://example.com/sec-filings", submission_types=None, ciks=None, sources=["Rss","Efts"]))

# list endpoints
endpoints = sec_filings_notifications.list_endpoints()
print(endpoints)

# remove an endpoint by endpoint url or id
for endpoint in endpoints["data"]:
    print(sec_filings_notifications.remove_endpoint(id=endpoint["id"]))