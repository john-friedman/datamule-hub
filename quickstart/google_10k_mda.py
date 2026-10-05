import logging
from pathlib import Path

from doc2dict import html2dict, get_title, flatten_dict
from doc2dict.mapping_dicts.sec_html_mapping_dicts import MAPPING_DICTS_BY_TYPE

from datamulehub import sec_filings_archive


# Hub hides logs by default.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

FOLDER = "Google"
OUTPUT_DIR = Path("google_mda")

sec_filings_archive.download_tar(
    ticker="GOOG",
    submission_type=["10-K"],
    document_type=["10-K"],
    output_dir=FOLDER,
    tar_max_size_mb=None,  # Write individual filing documents.
)

OUTPUT_DIR.mkdir(exist_ok=True)

for path in Path(FOLDER).rglob("*.htm*"):
    dct = html2dict(
        path.read_text(encoding="utf-8", errors="ignore"),
        mapping_dict=MAPPING_DICTS_BY_TYPE["10-K"],
    )
    matches = get_title(dct, title="item7")
    if not matches:
        continue

    mda = flatten_dict(matches[0][1])
    if not isinstance(mda, str):
        mda = "\n".join(str(part) for part in mda)

    (OUTPUT_DIR / f"{path.stem}.md").write_text(mda, encoding="utf-8")