from datamulehub import sec_filings_notifications

import logging
logging.basicConfig(level=logging.INFO)


# Connect to the sec filing websocket. One connection is free.
for filing in sec_filings_notifications.stream_filings():
    print(filing)


# Connect to the fastest sec filing websocket. This websocket uses url prediction to get filings quicker. It costs $0.25/connection/day.
for filing in sec_filings_notifications.stream_fastest_filings():
    print(filing)