"""Generate datamulehub's XML table lookup map from secinfrarust."""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path


def document_types_by_table(secinfra):
    registry = secinfra / "src/xml2tables/registry.rs"
    source = registry.read_text(encoding="utf-8")
    supported_source, mapping_source = source.split("pub fn mapping_json_for_document_type", 1)
    supported = set(re.findall(r'"([^"\n]+)"', supported_source))
    if not supported:
        raise ValueError("No supported XML document types found")

    arm_pattern = re.compile(
        r'(?P<types>(?:"[^"\n]+"\s*(?:\|\s*)?)+)'
        r'\s*=>\s*(?:\{\s*)?Some\(include_str!\(\s*'
        r'"(?P<path>[^"\n]+)"\s*\)\s*\)',
        re.MULTILINE,
    )
    result = defaultdict(set)
    matched = set()
    for arm in arm_pattern.finditer(mapping_source):
        document_types = re.findall(r'"([^"\n]+)"', arm.group("types"))
        mapping_path = (registry.parent / arm.group("path")).resolve()
        tables = json.loads(mapping_path.read_text(encoding="utf-8-sig"))
        if not isinstance(tables, dict) or not tables:
            raise ValueError(f"No table names in {mapping_path}")
        for document_type in document_types:
            if document_type in matched:
                raise ValueError(f"Duplicate document type: {document_type}")
            matched.add(document_type)
            for table in tables:
                result[table].add(document_type)

    if matched != supported:
        raise ValueError(
            f"Registry mismatch: missing={sorted(supported - matched)}, "
            f"unexpected={sorted(matched - supported)}"
        )
    return {table: tuple(sorted(types)) for table, types in sorted(result.items())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("secinfra", type=Path, help="Path to the secinfrarust checkout")
    args = parser.parse_args()
    mapping = document_types_by_table(args.secinfra.resolve())
    output = Path(__file__).resolve().parents[1] / "datamulehub/v3/datasets/document_types.py"
    lines = [
        '"""Generated from secinfrarust XML table registry and mapping JSON files.\n\n',
        'Run scripts/generate_xml_table_document_types.py to refresh this file.\n',
        '"""\n\n',
        "DOCUMENT_TYPES_BY_TABLE = {\n",
    ]
    lines.extend(f"    {table!r}: {types!r},\n" for table, types in mapping.items())
    lines.append("}\n")
    with output.open("w", encoding="utf-8", newline="\n") as file:
        file.writelines(lines)
    print(f"Generated {len(mapping)} table names in {output}")


if __name__ == "__main__":
    main()
