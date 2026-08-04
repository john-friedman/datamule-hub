from datamulehub import sec_filings_notifications


for filing in sec_filings_notifications.stream_filings(
    idle_timeout=300,
    ping_interval=60,
    ping_timeout=10,
    recv_timeout=30,
):
    print(
        filing.get("detected_time"),
        filing.get("submission_type"),
        filing.get("accession"),
        filing.get("ciks"),
    )
