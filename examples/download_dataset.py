import logging

from datamulehub import datasets


logging.basicConfig(level=logging.INFO)


# download simple xbrl dataset, including instantly updated delta
datasets.download("simple_xbrl", filename="simple_xbrl.parquet")

# only download delta
datasets.download("simple_xbrl", filename="simple_xbrl_delta.parquet", from_storage="delta")

# only download daily updated dataset
datasets.download("xml2tables/dos", filename="dos_daily.parquet", from_storage="daily")
