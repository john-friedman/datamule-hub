import logging

from datamulehub import datasets


logging.basicConfig(level=logging.INFO)


datasets.download("simple_xbrl", filename="simple_xbrl.parquet")
