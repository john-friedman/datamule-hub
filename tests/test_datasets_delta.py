import importlib
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


datasets = importlib.import_module("datamulehub.v3.datasets.datasets")


class DatasetDeltaTest(unittest.TestCase):
    def test_simple_xbrl_modes_merge_recent_filings(self):
        filing_date = "2026-09-30"
        daily_key = "datasets/simple_xbrl/data.parquet"
        old_key = datasets._source_key(None, filing_date, 1)
        new_key = datasets._source_key(None, filing_date, 2)

        with TemporaryDirectory() as directory:
            directory = Path(directory)
            snapshot = directory / "snapshot.parquet"
            old_file = directory / "old.parquet"
            new_file = directory / "new.parquet"
            pq.write_table(pa.table({
                "accessionNumber": pa.array([1], type=pa.uint64()),
                "filingDate": [filing_date],
                "_sourceKey": [old_key],
                "name": ["old"],
            }), snapshot)
            for accession, path, name in [(1, old_file, "old"), (2, new_file, "new")]:
                pq.write_table(pa.table({
                    "accessionNumber": pa.array([accession], type=pa.uint64()),
                    "name": [name],
                }), path)
            files = {daily_key: snapshot, old_key: old_file, new_key: new_file}
            requested = []

            def get_link(key, api_key=None):
                requested.append(key)
                return {"download_url": key, "object_key": key, "billing": {}}

            def download_link(link, output, chunk_size, **kwargs):
                shutil.copyfile(files[link["object_key"]], output)
                return output

            def stream_sgml(**kwargs):
                self.assertEqual(kwargs["contains_xbrl"], True)
                self.assertEqual(kwargs["filing_date"], filing_date)
                yield {"accession": [1, 2]}

            with patch.object(datasets, "_delta_filing_date", return_value=filing_date), \
                 patch.object(datasets, "get_link", side_effect=get_link), \
                 patch.object(datasets, "_download_link", side_effect=download_link), \
                 patch.object(datasets, "stream_sgml", side_effect=stream_sgml):
                both_path = directory / "both.parquet"
                datasets.download("simple_xbrl", filename=both_path)
                rows = sorted(pq.read_table(both_path).to_pylist(), key=lambda row: row["accessionNumber"])
                self.assertEqual([row["accessionNumber"] for row in rows], [1, 2])
                self.assertEqual([row["_sourceKey"] for row in rows], [old_key, new_key])
                self.assertEqual([row["filingDate"] for row in rows], [filing_date, filing_date])
                self.assertEqual(set(requested), {daily_key, new_key})

                requested.clear()
                daily_path = directory / "daily.parquet"
                datasets.download("simple_xbrl", filename=daily_path, from_storage="daily")
                self.assertEqual(pq.read_table(daily_path).num_rows, 1)
                self.assertEqual(requested, [daily_key])

                requested.clear()
                delta_path = directory / "delta.parquet"
                datasets.download("simple_xbrl", filename=delta_path, from_storage="delta")
                delta_rows = pq.read_table(delta_path).to_pylist()
                self.assertEqual(len(delta_rows), 2)
                self.assertEqual({row["filingDate"] for row in delta_rows}, {filing_date})
                self.assertEqual(set(requested), {old_key, new_key})

    def test_xml_table_lookup_and_mode_validation(self):
        with patch.object(datasets, "stream_sgml", return_value=[{"accession": [1]}]) as lookup:
            keys = datasets._delta_keys_for_date("dos", "2026-09-30", None)
        self.assertEqual(keys, {datasets._source_key("dos", "2026-09-30", 1)})
        self.assertIn("document_type", lookup.call_args.kwargs)
        self.assertNotIn("contains_xbrl", lookup.call_args.kwargs)

        for mode in ("express", "standard"):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "from_storage must be"):
                datasets.download("simple_xbrl", from_storage=mode)
        with self.assertRaisesRegex(ValueError, "requires an XML table or simple_xbrl"):
            datasets.download("submissions_metadata", from_storage="delta")

    def test_empty_delta_writes_parquet(self):
        with TemporaryDirectory() as directory, patch.object(datasets, "_delta_filing_date", return_value=None):
            output = Path(directory) / "empty.parquet"
            datasets.download("simple_xbrl", filename=output, from_storage="delta")
            table = pq.read_table(output)
            self.assertEqual(table.num_rows, 0)
            self.assertEqual(table.schema.names, ["_sourceKey"])

    def test_delta_stops_after_error_and_counts_in_flight_files(self):
        from threading import Event
        from time import sleep

        second_started = Event()
        requested = []
        progress_bars = []

        class Progress:
            def __init__(self, total, **kwargs):
                self.total = total
                self.n = 0
                progress_bars.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def set_postfix_str(self, *args, **kwargs):
                pass

            def update(self, count):
                self.n += count

            def refresh(self):
                pass

        def download_file(index, key, directory, api_key, chunk_size):
            requested.append(key)
            if index == 0:
                self.assertTrue(second_started.wait(2))
                raise datasets.ApiError(500, "usage tracking failed")
            second_started.set()
            sleep(0.05)
            return None

        candidates = {"a", "b", "c", "d"}
        with patch.object(datasets, "DELTA_DOWNLOAD_WORKERS", 2), \
             patch.object(datasets, "_download_delta_file", side_effect=download_file), \
             patch.object(datasets, "tqdm", Progress):
            with self.assertRaises(datasets.ApiError):
                datasets._download_delta_files(candidates, "unused", None, 1024)

        self.assertEqual(set(requested), {"a", "b"})
        self.assertEqual(progress_bars[0].n, 2)
        self.assertEqual(progress_bars[0].total, 4)


if __name__ == "__main__":
    unittest.main()
