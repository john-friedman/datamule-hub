import io
import json
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from datamulehub.v3.datasets import datasets


class LiveXmlDatasetTest(unittest.TestCase):
    def test_missing_object_gets_one_retry_after_three_seconds(self):
        missing = datasets.ApiError(404, "Object not found")
        with patch.object(datasets, "get_link", side_effect=[missing, {"download_url": "ready"}]) as get_link:
            with patch.object(datasets.time, "sleep") as sleep:
                link = datasets._live_link("sec-filings/xml2tables/2026-09-28/dos/000000000000000001.parquet", "key")

        self.assertEqual(link["download_url"], "ready")
        self.assertEqual(get_link.call_count, 2)
        sleep.assert_called_once_with(3)

    def test_missing_object_after_retry_is_skipped(self):
        missing = datasets.ApiError(404, "Object not found")
        with patch.object(datasets, "get_link", side_effect=[missing, missing]) as get_link:
            with patch.object(datasets.time, "sleep") as sleep:
                self.assertIsNone(datasets._live_link("missing", "key"))

        self.assertEqual(get_link.call_count, 2)
        sleep.assert_called_once_with(3)

    def test_merge_preserves_snapshot_rows_and_adds_new_columns(self):
        key = "sec-filings/xml2tables/2026-09-28/dos/000000000000000002.parquet"
        with TemporaryDirectory() as directory:
            snapshot = Path(directory) / "snapshot.parquet"
            live = Path(directory) / "live.parquet"
            merged = Path(directory) / "merged.parquet"
            pq.write_table(pa.table({
                "accessionNumber": pa.array([1], type=pa.uint64()),
                "filingDate": ["2026-09-27"],
                "_sourceKey": ["sec-filings/xml2tables/2026-09-27/dos/000000000000000001.parquet"],
                "old": ["first"],
            }), snapshot)
            pq.write_table(pa.table({
                "accessionNumber": pa.array([2], type=pa.uint64()),
                "filingDate": ["2026-09-28"],
                "old": ["second"],
                "new": ["value"],
            }), live)

            datasets._merge_parquet(snapshot, [(live, key)], merged)
            rows = pq.read_table(merged).to_pylist()

        self.assertEqual([row["accessionNumber"] for row in rows], [1, 2])
        self.assertIsNone(rows[0]["new"])
        self.assertEqual(rows[1]["new"], "value")
        self.assertEqual(rows[1]["_sourceKey"], key)

    def test_snapshot_keys_remove_published_filing(self):
        existing = "sec-filings/xml2tables/2026-09-28/dos/000000000000000001.parquet"
        new = "sec-filings/xml2tables/2026-09-28/dos/000000000000000002.parquet"
        with TemporaryDirectory() as directory:
            snapshot = Path(directory) / "snapshot.parquet"
            pq.write_table(pa.table({"_sourceKey": [existing]}), snapshot)
            candidates = datasets._snapshot_keys(snapshot, "dos", {existing, new})

        self.assertEqual(candidates, {new})

    def test_candidate_lookup_uses_database_pages_and_pads_accessions(self):
        first = {
            "success": True,
            "data": {"filingDate": ["2026-09-28"], "accession": ["1"]},
            "metadata": {"pagination": {"hasMore": True}},
        }
        second = {
            "success": True,
            "data": {"filingDate": ["2026-09-28"], "accession": ["2"]},
            "metadata": {"pagination": {"hasMore": False}},
        }
        responses = [io.BytesIO(json.dumps(item).encode()) for item in (first, second)]
        with patch.object(datasets.urllib.request, "urlopen", side_effect=responses) as urlopen:
            keys = datasets._recent_xml_keys("dos", 1790632800, "key")

        self.assertEqual(keys, {
            "sec-filings/xml2tables/2026-09-28/dos/000000000000000001.parquet",
            "sec-filings/xml2tables/2026-09-28/dos/000000000000000002.parquet",
        })
        self.assertIn("page=2", urlopen.call_args.args[0].full_url)

    def test_download_merges_ready_file_and_adds_charges(self):
        key = "sec-filings/xml2tables/2026-09-28/dos/000000000000000002.parquet"
        snapshot_link = {
            "object_key": "datasets/xml2tables/dos/data.parquet",
            "last_modified": 1790632800,
            "billing": {"total_charge": 0.10, "remaining_balance": 9.90},
        }
        live_link = {
            "object_key": key,
            "billing": {"total_charge": 0.01, "remaining_balance": 9.89},
        }
        with TemporaryDirectory() as directory:
            snapshot = Path(directory) / "source-snapshot.parquet"
            live = Path(directory) / "source-live.parquet"
            output = Path(directory) / "dos.parquet"
            pq.write_table(pa.table({
                "accessionNumber": pa.array([1], type=pa.uint64()),
                "filingDate": ["2026-09-27"],
                "_sourceKey": ["sec-filings/xml2tables/2026-09-27/dos/000000000000000001.parquet"],
            }), snapshot)
            pq.write_table(pa.table({
                "accessionNumber": pa.array([2], type=pa.uint64()),
                "filingDate": ["2026-09-28"],
            }), live)

            def copy_download(link, destination, _chunk_size, **_kwargs):
                shutil.copyfile(live if link is live_link else snapshot, destination)
                return destination

            with patch.object(datasets, "get_link", return_value=snapshot_link):
                with patch.object(datasets, "_recent_xml_keys", return_value={key}):
                    with patch.object(datasets, "_live_link", return_value=live_link):
                        with patch.object(datasets, "_download_link", side_effect=copy_download):
                            result = datasets.download("dos", filename=str(output), api_key="key")

            rows = pq.read_table(output).to_pylist()

        self.assertEqual([row["accessionNumber"] for row in rows], [1, 2])
        self.assertEqual(rows[1]["_sourceKey"], key)
        self.assertEqual(result["cost"], 0.11)
        self.assertEqual(result["live_files"], 1)


if __name__ == "__main__":
    unittest.main()
