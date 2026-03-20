#!/usr/bin/env python3
"""Map taxa to viable taxdump versions using TSV indexes (no SQLite)."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def read_tax_ids(path: Path) -> list[int]:
    ids: list[int] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            ids.append(int(s))
    return ids


def read_names(path: Path) -> list[str]:
    names: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            names.append(s)
    return names


def read_tax_name_pairs(path: Path) -> list[tuple[int, str]]:
    pairs: list[tuple[int, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        required = {"tax_id", "name_txt"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"{path} must have TSV headers: tax_id and name_txt")
        for row in reader:
            pairs.append((int(row["tax_id"]), row["name_txt"]))
    return pairs


def parse_versions_csv(s: str) -> set[str]:
    if not s:
        return set()
    return {v for v in s.split(",") if v}


def read_tsv_rows(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            yield row


def write_detail_tsv(path: Path, rows: list[dict[str, str | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "query_type",
            "tax_id",
            "name_txt",
            "match_source",
            "version_count",
            "versions_csv",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def write_intersection(path: Path, versions: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["version_id"])
        for version in sorted(versions):
            writer.writerow([version])


def main() -> int:
    parser = argparse.ArgumentParser(description="Map taxa to viable taxdump versions")
    parser.add_argument(
        "--record-sheets-dir",
        default="data/record_sheets",
        help="Directory containing taxon_version_index.tsv and taxon_name_version_index.tsv",
    )
    parser.add_argument("--tax-ids", help="Text file with one tax_id per line")
    parser.add_argument("--names", help="Text file with one exact name_txt per line")
    parser.add_argument(
        "--tax-name-pairs",
        help="TSV with columns tax_id and name_txt for exact taxon-name matching",
    )
    parser.add_argument(
        "--allow-synonym-fallback",
        action="store_true",
        help=(
            "If an exact name has no scientific-name match, use non-scientific "
            "name classes (for example synonyms) as fallback"
        ),
    )
    parser.add_argument("--out-dir", default="data/query_results", help="Output directory")
    args = parser.parse_args()

    if not args.tax_ids and not args.names and not args.tax_name_pairs:
        raise SystemExit("Provide --tax-ids and/or --names and/or --tax-name-pairs")

    sheets_dir = Path(args.record_sheets_dir)
    taxon_index_path = sheets_dir / "taxon_version_index.tsv"
    taxon_name_index_path = sheets_dir / "taxon_name_version_index.tsv"

    if not taxon_index_path.exists():
        raise SystemExit(f"Missing input file: {taxon_index_path}")
    if not taxon_name_index_path.exists():
        raise SystemExit(f"Missing input file: {taxon_name_index_path}")

    req_tax_ids = read_tax_ids(Path(args.tax_ids)) if args.tax_ids else []
    req_pairs = read_tax_name_pairs(Path(args.tax_name_pairs)) if args.tax_name_pairs else []
    req_names = read_names(Path(args.names)) if args.names else []

    req_tax_id_set = set(req_tax_ids)
    req_pair_set = set(req_pairs)
    req_name_set = set(req_names)

    tax_id_versions: dict[int, set[str]] = {}
    for row in read_tsv_rows(taxon_index_path):
        tax_id = int(row["tax_id"])
        if tax_id in req_tax_id_set:
            tax_id_versions[tax_id] = parse_versions_csv(row["versions_csv"])

    pair_scientific_versions: dict[tuple[int, str], set[str]] = {
        key: set() for key in req_pair_set
    }
    pair_non_scientific_versions: dict[tuple[int, str], set[str]] = {
        key: set() for key in req_pair_set
    }
    name_scientific_versions: dict[str, set[str]] = {name: set() for name in req_name_set}
    name_non_scientific_versions: dict[str, set[str]] = {name: set() for name in req_name_set}

    for row in read_tsv_rows(taxon_name_index_path):
        tax_id = int(row["tax_id"])
        name_txt = row["name_txt"]
        name_class = row["name_class"]
        versions = parse_versions_csv(row["versions_csv"])

        pair_key = (tax_id, name_txt)
        if pair_key in req_pair_set:
            if name_class == "scientific name":
                pair_scientific_versions[pair_key].update(versions)
            else:
                pair_non_scientific_versions[pair_key].update(versions)

        if name_txt in req_name_set:
            if name_class == "scientific name":
                name_scientific_versions[name_txt].update(versions)
            else:
                name_non_scientific_versions[name_txt].update(versions)

    all_sets: list[set[str]] = []
    detail_rows: list[dict[str, str | int]] = []
    warnings: list[str] = []

    for tax_id in req_tax_ids:
        versions = tax_id_versions.get(tax_id, set())
        all_sets.append(versions)
        detail_rows.append(
            {
                "query_type": "tax_id",
                "tax_id": tax_id,
                "name_txt": "",
                "match_source": "tax_id",
                "version_count": len(versions),
                "versions_csv": ",".join(sorted(versions)),
            }
        )

    for tax_id, name_txt in req_pairs:
        key = (tax_id, name_txt)
        scientific_versions = pair_scientific_versions.get(key, set())
        synonym_versions = pair_non_scientific_versions.get(key, set())
        match_source = "scientific_name"
        versions = scientific_versions

        if not versions and args.allow_synonym_fallback and synonym_versions:
            versions = synonym_versions
            match_source = "synonym_fallback"
        elif not versions and synonym_versions:
            match_source = "none_synonym_only"
            warnings.append(
                f"(tax_id={tax_id}, name_txt={name_txt!r}) matched only non-scientific "
                "names; rerun with --allow-synonym-fallback to include these versions."
            )
        elif not versions:
            match_source = "none"
            warnings.append(f"(tax_id={tax_id}, name_txt={name_txt!r}) had no matches in any name class.")

        all_sets.append(versions)
        detail_rows.append(
            {
                "query_type": "tax_id_name",
                "tax_id": tax_id,
                "name_txt": name_txt,
                "match_source": match_source,
                "version_count": len(versions),
                "versions_csv": ",".join(sorted(versions)),
            }
        )

    for name_txt in req_names:
        scientific_versions = name_scientific_versions.get(name_txt, set())
        synonym_versions = name_non_scientific_versions.get(name_txt, set())
        match_source = "scientific_name"
        versions = scientific_versions

        if not versions and args.allow_synonym_fallback and synonym_versions:
            versions = synonym_versions
            match_source = "synonym_fallback"
        elif not versions and synonym_versions:
            match_source = "none_synonym_only"
            warnings.append(
                f"(name_txt={name_txt!r}) matched only non-scientific names; "
                "rerun with --allow-synonym-fallback to include these versions."
            )
        elif not versions:
            match_source = "none"
            warnings.append(f"(name_txt={name_txt!r}) had no matches in any name class.")

        all_sets.append(versions)
        detail_rows.append(
            {
                "query_type": "name_txt",
                "tax_id": "",
                "name_txt": name_txt,
                "match_source": match_source,
                "version_count": len(versions),
                "versions_csv": ",".join(sorted(versions)),
            }
        )

    viable_versions = set.intersection(*all_sets) if all_sets else set()

    out_dir = Path(args.out_dir)
    details_path = out_dir / "query_details.tsv"
    viable_path = out_dir / "viable_versions.tsv"

    write_detail_tsv(details_path, detail_rows)
    write_intersection(viable_path, viable_versions)

    print(f"Wrote: {details_path}")
    print(f"Wrote: {viable_path}")
    print(f"Viable versions (intersection): {len(viable_versions)}")
    if warnings:
        print(f"Warnings: {len(warnings)}", file=sys.stderr)
        for message in warnings:
            print(f"WARNING: {message}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
