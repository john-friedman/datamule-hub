import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from datamulehub import sec_filings_notifications
from datamulehub.utils.format_accession import format_accession


logging.basicConfig(level=logging.INFO)

eastern = ZoneInfo("America/New_York")

for filing in sec_filings_notifications.stream_fastest_filings():

    ciks = filing.get("ciks", [])
    submission_type = filing.get("submission_type")

    accession = filing.get("accession")
    accno_wo_dash = format_accession(accession, "no-dash")
    accno_w_dash = format_accession(accession, "dash")
    timestamp = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " ET"
    print("------------")
    print(f"[{timestamp}] {submission_type} ciks={ciks}")
    print(f"https://www.sec.gov/Archives/edgar/data/{accno_wo_dash}/{accno_w_dash}-index.html")