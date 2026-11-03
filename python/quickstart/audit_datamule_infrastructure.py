from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from datamulehub import databases

eastern = ZoneInfo("America/New_York")
end_date = datetime.now(eastern).date()
start_date = end_date - timedelta(days=30)
start_seconds = int(datetime.combine(start_date, time(), eastern).timestamp())
end_seconds = int(datetime.combine(end_date, time(), eastern).timestamp())


# Download Fastest SEC Filing Websocket's metrics
# Estimated $0.0301 at current rates.
databases.query(
    f"""
    SELECT accession, detected_time, detection_method
    FROM fastest_sec_filings_metadata
    WHERE detected_time >= {start_seconds * 1000}
      AND detected_time < {end_seconds * 1000}
    """,
    output_dir="fastest",
)

# Download Websocket's metrics
# Estimated $0.00117 at current rates.
databases.query(
    f"""
    SELECT accession, source, detected_time
    FROM monitor_dumps
    WHERE detected_time >= {start_seconds}
      AND detected_time < {end_seconds}
      AND lower(source) IN ('rss', 'efts', 'anticipate')
    """,
    output_dir="websocket",
)

# Download acceptancedatetime according to the SEC.
# Estimated $0.00286 at current rates.
databases.query(
    f"""
    SELECT accessionnumber, acceptancedatetime, filingdate
    FROM submissions_metadata
    WHERE filingdate >= '{start_date}'
      AND filingdate < '{end_date}'
    """,
    output_dir="linked_filings",
)
