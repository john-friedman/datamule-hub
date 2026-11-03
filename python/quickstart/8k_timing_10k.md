# 8-K item time to 10-K

When an 8-K is released does its item indicate when the next 10-K will come out?.

8-K sample restricted to '2025-01-01' to '2025-01-31'. Trivial to expand to full corpus.

## Using metadata extracted from the filing

Uses the filing's metadata, which is in the filing, but not the filing's content. 

| Extracted filing metadata SQL: Item | Observations | Matched observations | Mean days |
| --- | ---: | ---: | ---: |
| 1.01 | 867 | 796 | 93.81 |
| 1.02 | 65 | 51 | 86.20 |
| 1.03 | 11 | 1 | 55.00 |
| 1.05 | 1 | 0 | N/A |
| 2.01 | 87 | 68 | 95.15 |
| 2.02 | 1,077 | 1,064 | 74.50 |
| 2.03 | 365 | 351 | 74.01 |
| 2.04 | 13 | 8 | 153.88 |
| 2.05 | 34 | 33 | 70.94 |
| 2.06 | 7 | 6 | 44.83 |
| 3.01 | 168 | 133 | 97.56 |
| 3.02 | 330 | 314 | 84.65 |
| 3.03 | 85 | 63 | 83.81 |
| 4.01 | 34 | 31 | 132.77 |
| 4.02 | 14 | 12 | 78.08 |
| 5.01 | 28 | 8 | 76.25 |
| 5.02 | 1,007 | 950 | 82.37 |
| 5.03 | 189 | 163 | 79.34 |
| 5.05 | 6 | 6 | 133.33 |
| 5.06 | 3 | 3 | 104.33 |
| 5.07 | 174 | 147 | 148.42 |
| 5.08 | 6 | 6 | 86.50 |
| 6.01 | 2 | 1 | 142.00 |
| 6.02 | 11 | 11 | 48.64 |
| 6.04 | 3 | 3 | 54.33 |
| 6.05 | 3 | 3 | 66.00 |
| 7.01 | 1,473 | 1,431 | 75.23 |
| 8.01 | 1,432 | 1,268 | 83.49 |
| 9.01 | 3,900 | 3,664 | 84.82 |

### code

```python
from datamulehub import databases


databases.query(
    """
    SELECT item, COUNT(*) AS observations, COUNT(days) AS matched_observations,
           AVG(CAST(days AS DOUBLE)) AS mean_days
    FROM (
        SELECT DISTINCT accession, cik, days,
            COALESCE(
                NULLIF(REGEXP_EXTRACT(raw_item, '([1-9][.][0-9]{2})', 1), ''),
                m.item, raw_item
            ) AS item
        FROM (
            SELECT DISTINCT accession, cik, TRIM(raw_item) AS raw_item,
                   DATE_DIFF('day', filing_date, next_10k_filing_date) AS days
            FROM (
                SELECT CAST(f.accessionnumber AS BIGINT) AS accession,
                       TRY_CAST(e.entity_cik AS BIGINT) AS cik,
                       COALESCE(TRY_CAST(f.filing_date AS DATE),
                           CAST(TRY(DATE_PARSE(f.filing_date, '%Y%m%d')) AS DATE)) AS filing_date,
                       COALESCE(
                           NULLIF(f.items__list, CAST(ARRAY[] AS ARRAY(VARCHAR))),
                           NULLIF(f.item_number__list, CAST(ARRAY[] AS ARRAY(VARCHAR))),
                           NULLIF(f.item_information__list, CAST(ARRAY[] AS ARRAY(VARCHAR))),
                           ARRAY[COALESCE(f.items, f.item_number, f.item_information)]
                       ) AS header_items,
                       MIN(k.filing_date) AS next_10k_filing_date
                FROM filing_metadata_filings f
                JOIN sec_submission_details_table d
                  ON d.accessionnumber = f.accessionnumber
                 AND d.filingdate BETWEEN '2025-01-01' AND '2025-01-31'
                LEFT JOIN filing_metadata_entities e
                  ON e.accessionnumber = f.accessionnumber AND e.entity_role = 'filer'
                 AND e.filingdate BETWEEN '2025-01-01' AND '2025-01-31'
                LEFT JOIN (
                    SELECT DISTINCT TRY_CAST(e10.entity_cik AS BIGINT) AS cik,
                        COALESCE(TRY_CAST(f10.filing_date AS DATE),
                            CAST(TRY(DATE_PARSE(f10.filing_date, '%Y%m%d')) AS DATE)) AS filing_date
                    FROM filing_metadata_filings f10
                    JOIN sec_submission_details_table d10
                      ON d10.accessionnumber = f10.accessionnumber
                     AND d10.filingdate BETWEEN '2025-01-01' AND '2026-10-13'
                    JOIN filing_metadata_entities e10
                      ON e10.accessionnumber = f10.accessionnumber AND e10.entity_role = 'filer'
                     AND e10.filingdate BETWEEN '2025-01-01' AND '2026-10-13'
                    WHERE f10.filingdate BETWEEN '2025-01-01' AND '2026-10-13'
                      AND d10.submissiontype = '10-K'
                ) k
                  ON k.cik = TRY_CAST(e.entity_cik AS BIGINT)
                 AND k.filing_date > COALESCE(TRY_CAST(f.filing_date AS DATE),
                     CAST(TRY(DATE_PARSE(f.filing_date, '%Y%m%d')) AS DATE))
                 AND k.filing_date <= DATE '2026-10-13'
                WHERE f.filingdate BETWEEN '2025-01-01' AND '2025-01-31'
                  AND d.submissiontype IN ('8-K', '8-K/A')
                  AND COALESCE(TRY_CAST(f.filing_date AS DATE),
                      CAST(TRY(DATE_PARSE(f.filing_date, '%Y%m%d')) AS DATE))
                      BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
                GROUP BY f.accessionnumber, e.entity_cik, f.filing_date,
                         f.items__list, f.item_number__list, f.item_information__list,
                         f.items, f.item_number, f.item_information
            ) matched
            , UNNEST(header_items) AS u(raw_item)
            WHERE TRIM(raw_item) <> ''
        ) r
        LEFT JOIN (VALUES ('1.01', 'entry into a material definitive agreement'),
            ('1.02', 'termination of a material definitive agreement'),
            ('1.03', 'bankruptcy or receivership'),
            ('1.04', 'mine safety'),
            ('1.05', 'material cybersecurity'),
            ('2.01', 'completion of acquisition or disposition of assets'),
            ('2.02', 'results of operations and financial condition'),
            ('2.03', 'creation of a direct financial obligation'),
            ('2.04', 'triggering events that accelerate or increase'),
            ('2.05', 'cost associated with exit or disposal activities'),
            ('2.05', 'costs associated with exit or disposal activities'),
            ('2.06', 'material impairments'),
            ('3.01', 'notice of delisting or failure to satisfy'),
            ('3.02', 'unregistered sales of equity securities'),
            ('3.03', 'material modifications to rights of security holders'),
            ('3.03', 'material modification to rights of security holders'),
            ('4.01', 'changes in registrant'),
            ('4.02', 'non-reliance on previously issued financial statements'),
            ('5.01', 'changes in control of registrant'),
            ('5.02', 'departure of directors or certain officers'),
            ('5.03', 'amendments to articles of incorporation or bylaws'),
            ('5.04', 'temporary suspension of trading'),
            ('5.05', 'amendments to the registrant'),
            ('5.06', 'change in shell company status'),
            ('5.07', 'submission of matters to a vote of security holders'),
            ('5.08', 'shareholder nominations pursuant to exchange act rule 14a-11'),
            ('5.08', 'shareholder director nominations'),
            ('6.01', 'abs informational and computational material'),
            ('6.02', 'change of servicer or trustee'),
            ('6.03', 'change in credit enhancement or other external support'),
            ('6.04', 'failure to make a required distribution'),
            ('6.05', 'securities act updating disclosure'),
            ('7.01', 'regulation fd disclosure'),
            ('8.01', 'other events'),
            ('9.01', 'financial statements and exhibits')) AS m(item, prefix)
          ON LOWER(r.raw_item) LIKE m.prefix || '%'
    ) observations
    GROUP BY item
    ORDER BY item
    """,
    output_dir="raw/extracted_filings_metadata_sql",
)
```

## Using the SEC's submission.zip endpoint

Uses the SEC's reporting of what the filing metadata is.

| SEC endpoint SQL: Item | Observations | Matched observations | Mean days |
| --- | ---: | ---: | ---: |
| 1.01 | 867 | 796 | 93.81 |
| 1.02 | 65 | 51 | 86.20 |
| 1.03 | 11 | 1 | 55.00 |
| 1.05 | 1 | 0 | N/A |
| 2.01 | 87 | 68 | 95.15 |
| 2.02 | 1,077 | 1,064 | 74.50 |
| 2.03 | 365 | 351 | 74.01 |
| 2.04 | 13 | 8 | 153.88 |
| 2.05 | 34 | 33 | 70.94 |
| 2.06 | 7 | 6 | 44.83 |
| 3.01 | 168 | 133 | 97.56 |
| 3.02 | 330 | 314 | 84.65 |
| 3.03 | 85 | 63 | 83.81 |
| 4.01 | 34 | 31 | 132.77 |
| 4.02 | 14 | 12 | 78.08 |
| 5.01 | 28 | 8 | 76.25 |
| 5.02 | 1,007 | 950 | 82.37 |
| 5.03 | 189 | 163 | 79.34 |
| 5.05 | 6 | 6 | 133.33 |
| 5.06 | 3 | 3 | 104.33 |
| 5.07 | 174 | 147 | 148.42 |
| 5.08 | 6 | 6 | 86.50 |
| 6.01 | 2 | 1 | 142.00 |
| 6.02 | 11 | 11 | 48.64 |
| 6.04 | 3 | 3 | 54.33 |
| 6.05 | 3 | 3 | 66.00 |
| 7.01 | 1,473 | 1,431 | 75.23 |
| 8.01 | 1,432 | 1,269 | 83.50 |
| 9.01 | 3,900 | 3,665 | 84.83 |

### code

```python
from datamulehub import databases


databases.query(
    """
    SELECT item, COUNT(*) AS observations, COUNT(days) AS matched_observations,
           AVG(CAST(days AS DOUBLE)) AS mean_days
    FROM (
        SELECT DISTINCT accession, cik, TRIM(raw_item) AS item,
               DATE_DIFF('day', filing_date, next_10k_filing_date) AS days
        FROM (
            SELECT CAST(REPLACE(e.accessionnumber, '-', '') AS BIGINT) AS accession,
                   CAST(e.cik AS BIGINT) AS cik, e.items,
                   TRY_CAST(e.filingdate AS DATE) AS filing_date,
                   MIN(TRY_CAST(k.filingdate AS DATE)) AS next_10k_filing_date
            FROM submissions_metadata e
            LEFT JOIN submissions_metadata k
              ON k.cik = e.cik AND k."form" = '10-K'
             AND k.filingdate BETWEEN '2025-01-01' AND '2026-10-13'
             AND TRY_CAST(k.filingdate AS DATE) > TRY_CAST(e.filingdate AS DATE)
            WHERE e."form" IN ('8-K', '8-K/A')
              AND e.filingdate BETWEEN '2025-01-01' AND '2025-01-31'
            GROUP BY e.accessionnumber, e.cik, e.items, e.filingdate
        ) matched
        , UNNEST(SPLIT(COALESCE(items, ''), ',')) AS u(raw_item)
        WHERE TRIM(raw_item) <> ''
    ) observations
    GROUP BY item
    ORDER BY item
    """,
    output_dir="raw/sec_endpoint_sql",
)
```

## Using the filing's content

What is actually in the filing. Most correct.

This is a bit slow. Datamule will soon have all items and text in SQL queryable syntax. Should make this take ~5s.

| Filing downloads: Item | Observations | Matched observations | Mean days |
| --- | ---: | ---: | ---: |
| 1.01 | 861 | 790 | 93.37 |
| 1.02 | 65 | 51 | 86.20 |
| 1.03 | 11 | 1 | 55.00 |
| 1.05 | 1 | 0 | N/A |
| 2.01 | 86 | 67 | 95.58 |
| 2.02 | 1,077 | 1,064 | 74.74 |
| 2.03 | 364 | 351 | 74.01 |
| 2.04 | 13 | 8 | 153.88 |
| 2.05 | 34 | 33 | 70.94 |
| 2.06 | 7 | 6 | 44.83 |
| 3.01 | 167 | 132 | 97.67 |
| 3.02 | 330 | 314 | 84.69 |
| 3.03 | 84 | 62 | 84.15 |
| 4.01 | 34 | 31 | 132.77 |
| 4.02 | 13 | 11 | 81.18 |
| 5.01 | 27 | 7 | 79.57 |
| 5.02 | 1,001 | 944 | 82.38 |
| 5.03 | 189 | 163 | 79.34 |
| 5.05 | 6 | 6 | 133.33 |
| 5.06 | 3 | 3 | 104.33 |
| 5.07 | 174 | 147 | 148.42 |
| 5.08 | 6 | 6 | 86.50 |
| 6.01 | 2 | 1 | 142.00 |
| 6.02 | 11 | 11 | 48.64 |
| 6.04 | 3 | 3 | 54.33 |
| 6.05 | 3 | 3 | 66.00 |
| 7.01 | 1,471 | 1,429 | 75.48 |
| 8.01 | 1,433 | 1,269 | 83.78 |
| 9.01 | 3,955 | 3,720 | 84.54 |

### code

```python
"""Download 8-Ks, extract items, and measure days to the next company 10-K.

Install: pip install datamulehub doc2dict polars
Run: python compare_using_filing_downloads.py
Requires DATAMULE_API_KEY. Completed downloads and metadata queries are cached.
"""

import hashlib
import json
import re
import tarfile
from datetime import date
from pathlib import Path, PurePosixPath

import polars as pl
from datamulehub import databases, sec_filings_archive
from doc2dict import get_title, html2dict, txt2dict
from doc2dict.mapping_dicts.sec_html_mapping_dicts import dict_8k_html
from selectolax.parser import HTMLParser


ROOT = Path(__file__).resolve().parent
RAW = ROOT / "raw"
START = "2025-01-01"
END = "2025-01-31"
AS_OF = "2026-10-13"
TYPES = ["8-K", "8-K/A"]
ITEM_PATTERN = re.compile(
    next(rule["regex"] for rule in dict_8k_html["levels"][0]
         if rule["name"] == "item"), re.IGNORECASE,
)
VALID_ITEM = re.compile(r"(?:[1-9]|1[0-2]|[1-9]\.\d{1,2})")
ACCESSION_PATTERN = re.compile(r"(?:\d{18}|\d{10}-\d{2}-\d{6})")
ITEM_SCHEMA = {"accession": pl.Int64, "items": pl.List(pl.String)}
MODERN_ITEMS_FROM = "2004-08-23"
ITEM_EXTRACTION_VERSION = 2


def item_key(item):
    return tuple(map(int, item.split(".")))


def accession_from_path(name):
    for part in reversed(PurePosixPath(name).parts[:-1]):
        if ACCESSION_PATTERN.fullmatch(part):
            return int(part.replace("-", ""))
    raise ValueError(f"No accession directory in {name!r}")


def extract_items(content, suffix, allow_legacy_items=True):
    text = content.decode("utf-8-sig", errors="replace")
    is_html = suffix in {".htm", ".html"} or re.search(
        r"<(?:html|body)\b", text, re.IGNORECASE,
    )
    if is_html:
        parsed = html2dict(text, mapping_dict=dict_8k_html)
        text = HTMLParser(text).text(separator="\n")
    else:
        parsed = txt2dict(text.encode("utf-8"), mapping_dict=dict_8k_html)
    headings = [node["title"] for _, node in get_title(parsed, title_class="item")]
    # Also recognize plain-text, <pre>, and unstyled HTML item headings,
    # using the same SEC mapping. Filter out Regulation S-K references.
    items = set()
    for heading in headings + text.splitlines():
        match = ITEM_PATTERN.match(heading.strip())
        if match and VALID_ITEM.fullmatch(match.group(1)):
            item = match.group(1)
            if allow_legacy_items or "." in item:
                items.add(item)
    return sorted(items, key=item_key)


def download_8ks(start, end):
    folder = RAW / "8k"
    receipt = RAW / "8k_download.json"
    if receipt.exists():
        saved = json.loads(receipt.read_text(encoding="utf-8"))
        if (saved["start"], saved["end"]) != (start, end):
            raise ValueError("Cached 8-K download has different dates; use a fresh raw folder.")
        files = sorted({(Path(path) if Path(path).is_absolute() else ROOT / path).resolve()
                        for path in saved["files"]})
        if not files or not all(path.exists() for path in files):
            raise ValueError("Cached download is incomplete; check raw/8k_download.json.")
        return files
    files = sec_filings_archive.download_tar(
        submission_type=TYPES, document_type=TYPES,
        filing_date=(start, end), output_dir=str(folder),
    )
    # The downloader returns one destination per document, so many entries
    # point to the same TAR. Parse each archive only once.
    files = sorted({Path(path).resolve() for path in files})
    receipt.write_text(json.dumps({"start": start, "end": end,
                                  "files": [str(path) for path in files]}, indent=2),
                       encoding="utf-8")
    return files


def write_items(archives):
    archives = sorted(set(archives))
    fingerprint = hashlib.sha256(json.dumps([ITEM_EXTRACTION_VERSION, [
        (str(path), path.stat().st_size, path.stat().st_mtime_ns)
        for path in archives
    ]]).encode()).hexdigest()
    output = RAW / "8k_items.parquet"
    receipt = RAW / "8k_items.json"
    if output.exists() and receipt.exists():
        if json.loads(receipt.read_text())["archive_fingerprint"] == fingerprint:
            return pl.read_parquet(output)
    filings = {}
    for path in archives:
        with tarfile.open(path, "r|*") as archive:
            for member in archive:
                suffix = PurePosixPath(member.name).suffix.lower()
                if not member.isfile() or suffix not in {".htm", ".html", ".txt"}:
                    continue
                accession = accession_from_path(member.name)
                filing_date = PurePosixPath(member.name).parts[0]
                date.fromisoformat(filing_date)
                with archive.extractfile(member) as source:
                    try:
                        items = extract_items(source.read(), suffix,
                                              allow_legacy_items=filing_date < MODERN_ITEMS_FROM)
                    except Exception as error:
                        raise RuntimeError(f"Failed to parse {path.name}:{member.name}") from error
                filings.setdefault(accession, set()).update(items)
        print(f"Extracted {len(filings):,} accessions through {path.name}", flush=True)
    result = pl.DataFrame([
        {"accession": accession, "items": sorted(items, key=item_key)}
        for accession, items in sorted(filings.items())
    ], schema=ITEM_SCHEMA)
    if result.is_empty():
        raise ValueError("The archive contained no supported 8-K documents.")
    result.write_parquet(output, compression="zstd")
    receipt.write_text(json.dumps({"archive_fingerprint": fingerprint}), encoding="utf-8")
    return result


def query_metadata(start, end, as_of):
    # filingdate prunes the date partitions; filing_date is the header date
    # used for the actual timing calculation (usually formatted YYYYMMDD).
    sql = f"""SELECT DISTINCT
    CAST(f.accessionnumber AS BIGINT) AS accession,
    TRY_CAST(e.entity_cik AS BIGINT) AS cik,
    d.submissiontype AS submission_type,
    f.filing_date AS filing_date,
    f.filingdate AS metadata_partition_date
FROM filing_metadata_filings f
JOIN sec_submission_details_table d
  ON f.accessionnumber = d.accessionnumber
 AND d.filingdate BETWEEN '{start}' AND '{as_of}'
LEFT JOIN filing_metadata_entities e
  ON f.accessionnumber = e.accessionnumber
 AND e.entity_role = 'filer'
 AND e.filingdate BETWEEN '{start}' AND '{as_of}'
WHERE f.filingdate BETWEEN '{start}' AND '{as_of}'
  AND (
    d.submissiontype = '10-K'
    OR (d.submissiontype IN ('8-K', '8-K/A')
        AND f.filingdate BETWEEN '{start}' AND '{end}')
  )
"""
    sql_path = RAW / "filing_metadata.sql"
    output = RAW / "filing_metadata.parquet"
    if output.exists() and sql_path.exists() and sql_path.read_text(encoding="utf-8") == sql:
        return pl.read_parquet(output)
    print("Querying filing_metadata_filings for 8-K and 10-K dates...", flush=True)
    query = databases.query(sql, output_dir=str(RAW / "filing_metadata_query"))
    if not query["files"]:
        raise ValueError("The metadata query returned no Parquet files.")
    result = pl.read_parquet(query["files"]).unique()
    result.write_parquet(output, compression="zstd")
    sql_path.write_text(sql, encoding="utf-8")
    receipt = {key: query.get(key) for key in ("query_id", "cost", "billing")}
    (RAW / "filing_metadata_query.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(f"Saved {result.height:,} metadata rows; query cost ${query['cost'] or 0:.4f}", flush=True)
    return result


def match_filings(items, metadata, as_of):
    dates = pl.col("filing_date").cast(pl.String).str.strip_chars()
    metadata = metadata.with_columns(
        pl.col("accession", "cik").cast(pl.Int64),
        pl.coalesce(dates.str.strptime(pl.Date, "%Y%m%d", strict=False),
                    dates.str.strptime(pl.Date, "%Y-%m-%d", strict=False)).alias("filing_date"),
    ).select("accession", "cik", "submission_type", "filing_date").unique()
    conflicting = metadata.group_by("accession", "cik").agg(
        pl.col("filing_date").drop_nulls().n_unique().alias("dates")
    ).filter(pl.col("dates") > 1)
    if not conflicting.is_empty():
        raise ValueError("Conflicting header filing dates for the same accession and filer.")
    # Prefer a valid header date over a duplicate record with a missing date.
    metadata = metadata.sort("filing_date", nulls_last=True).unique(
        subset=["accession", "cik", "submission_type"], keep="first",
    )
    eights = metadata.filter(pl.col("submission_type").is_in(TYPES))
    pairs = items.join(eights, on="accession", how="left")
    eligible = pairs.filter(pl.col("cik").is_not_null() & pl.col("filing_date").is_not_null())
    tens = metadata.filter(
        (pl.col("submission_type") == "10-K") & pl.col("cik").is_not_null()
        & pl.col("filing_date").is_not_null()
        & (pl.col("filing_date") <= pl.lit(date.fromisoformat(as_of)))
    ).select("cik", pl.col("accession").alias("next_10k_accession"),
             pl.col("filing_date").alias("next_10k_filing_date"))
    # Break ties between 10-Ks on the same date deterministically by accession.
    tens = tens.sort("cik", "next_10k_filing_date", "next_10k_accession").unique(
        subset=["cik", "next_10k_filing_date"], keep="first", maintain_order=True,
    )
    matches = eligible.sort("cik", "filing_date").join_asof(
        tens, left_on="filing_date", right_on="next_10k_filing_date", by="cik",
        strategy="forward", allow_exact_matches=False, check_sortedness=False,
    ).select("accession", "cik", "next_10k_accession", "next_10k_filing_date")
    pairs = pairs.join(matches, on=["accession", "cik"], how="left").with_columns(
        (pl.col("next_10k_filing_date") - pl.col("filing_date"))
        .dt.total_days().alias("days_to_next_10k"),
        pl.when(pl.col("submission_type").is_null()).then(pl.lit("missing_filing_metadata"))
        .when(pl.col("cik").is_null()).then(pl.lit("missing_filer_cik"))
        .when(pl.col("filing_date").is_null()).then(pl.lit("missing_header_filing_date"))
        .when(pl.col("next_10k_accession").is_null()).then(pl.lit("no_later_10k_observed"))
        .otherwise(pl.lit("matched")).alias("match_status"),
    )
    timing = pairs.explode("items").rename({"items": "item"}).select(
        "accession", "cik", "submission_type", "filing_date", "item",
        "next_10k_accession", "next_10k_filing_date", "days_to_next_10k", "match_status",
    ).sort("accession", "cik", "item")
    return pairs, timing


def summarize(timing):
    summary = timing.filter(pl.col("item").is_not_null()).group_by("item").agg(
        pl.len().alias("observations"),
        pl.col("days_to_next_10k").count().alias("matched_observations"),
        pl.col("days_to_next_10k").mean().alias("mean_days"),
    ).with_columns(pl.col("observations", "matched_observations").cast(pl.Int64))
    order = sorted(summary["item"].to_list(), key=item_key)
    return pl.DataFrame({"item": order}).join(summary, on="item", how="left", maintain_order="left")


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    items = write_items(download_8ks(START, END))
    metadata = query_metadata(START, END, AS_OF)
    _, timing = match_filings(items, metadata, AS_OF)
    summary = summarize(timing)
    if summary.is_empty():
        raise ValueError("No identified items are available.")
    output = ROOT / "compare_using_filing_downloads.parquet"
    summary.write_parquet(output, compression="zstd")
    print(f"Wrote {summary.height:,} item means to {output.name}", flush=True)


if __name__ == "__main__":
    main()
```
