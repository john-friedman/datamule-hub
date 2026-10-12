import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from datamulehub.v3.datasets import datasets


class MetadataDatasetTests(unittest.TestCase):
    def test_snapshot_resolution_and_delta_lookup(self):
        table="filing_metadata_entities"
        self.assertEqual(datasets.resolve_path(table),f"datasets/metadata/{table}/data.parquet")
        self.assertEqual(datasets._metadata_table_name(datasets.resolve_path(table)),table)
        self.assertIsNone(datasets._metadata_table_name("datasets/metadata/filing_metadata_documents/data.parquet"))
        with patch.object(datasets,"stream_sgml",return_value=[{"accession":[123456726000001]}]) as lookup:
            keys=datasets._delta_keys_for_date(table,"2026-10-09","test")
        lookup.assert_called_once_with(filing_date="2026-10-09",api_key="test")
        self.assertEqual(keys,{"sec-filings/metadata/2026-10-09/filing_metadata_entities/000123456726000001.parquet"})

    def test_snapshot_deduplication_and_list_merge(self):
        table="filing_metadata_filings"
        key=datasets._source_key(table,"2026-10-09",123456726000001)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            snapshot=root / "snapshot.parquet"
            delta=root / "delta.parquet"
            output=root / "merged.parquet"
            pq.write_table(pa.table({"accessionNumber":pa.array([123456726000001],type=pa.uint64()),"filingDate":["2026-10-09"],"items__list":[["1","1"]],"_sourceKey":[key]}),snapshot)
            candidates={key,"other"}
            datasets._snapshot_keys(snapshot,table,candidates)
            self.assertEqual(candidates,{"other"})
            next_key=datasets._source_key(table,"2026-10-09",123456726000002)
            pq.write_table(pa.table({"accessionNumber":pa.array([123456726000002],type=pa.uint64()),"filingDate":["2026-10-09"],"items__list":pa.array([[]],type=pa.list_(pa.string()))}),delta)
            datasets._merge_parquet([(snapshot,None),(delta,next_key)],output)
            rows=pq.read_table(output).to_pylist()
            self.assertEqual([r["items__list"] for r in rows],[["1","1"],[]])
            self.assertEqual(rows[1]["_sourceKey"],next_key)


if __name__ == "__main__":
    unittest.main()
