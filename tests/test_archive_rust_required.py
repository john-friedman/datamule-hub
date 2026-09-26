import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from datamulehub.v3.sec_filings_archive import download_sgml, download_tar


class RustArchiveRequiredTest(unittest.TestCase):
    def test_missing_binary_preserves_existing_output(self):
        for download in (download_tar, download_sgml):
            with self.subTest(download=download.__name__), TemporaryDirectory() as temporary:
                output_dir = Path(temporary) / "output"
                output_dir.mkdir()
                existing = output_dir / "keep.txt"
                existing.write_text("keep", encoding="utf-8")

                with patch("datamulehub.v3.sec_filings_archive._rust.find_binary",
                           return_value=None):
                    with self.assertRaisesRegex(RuntimeError,
                                                "Rust archive downloader is not installed"):
                        download(output_dir=output_dir, overwrite=True)

                self.assertEqual(existing.read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
