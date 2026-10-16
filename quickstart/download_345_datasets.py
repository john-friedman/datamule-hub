from datamulehub import datasets

import logging
logging.basicConfig(level=logging.INFO)

tables = [
    "345",
    "345_derivative_holding",
    "345_derivative_security",
    "345_derivative_transaction",
    "345_footnote",
    "345_non_derivative_holding",
    "345_non_derivative_security",
    "345_non_derivative_transaction",
    "345_owner_signature",
    "345_reporting_owner",
]


# Download every 345 dataset (updated daily)
# Estimated $0.763 at current rates.
for table in tables:
    datasets.download(
        f"{table}",
        filename=f"{table}.parquet",
        from_storage="daily"
    )
input("Proceed?")

# Download a dataset updated in realtime.
# Estimated $0.169 at current rates.
datasets.download(
    "345",
    filename=f"345_full.parquet",
    from_storage="both"
)
input("Proceed?")
# Download just the delta.

datasets.download(
    "345",
    filename=f"345_delta.parquet",
    from_storage="delta"
)