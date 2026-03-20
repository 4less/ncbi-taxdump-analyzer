#!/usr/bin/env python3
"""Export record-sheet TSVs from TSV presence tables (no SQLite)."""

from __future__ import annotations

import argparse
import csv
import subprocess
import tempfile
from pathlib import Path


def strip_header_to_temp(input_path: Path, temp_dir: Path) -> Path:
    body_path = temp_dir / f"{input_path.stem}.body.tsv"
    with input_path.open("r", encoding="utf-8", newline="") as src, body_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        next(src, None)
        for line in src:
            dst.write(line)
    return body_path


def sort_file(input_path: Path, output_path: Path, keys: list[str], unique: bool = False) -> None:
    cmd = ["sort", "-t", "\t", "-o", str(output_path)]
    if unique:
        cmd.append("-u")
    cmd.extend(keys)
    cmd.append(str(input_path))
    subprocess.run(cmd, check=True)


def write_with_header(out_path: Path, header: str, body_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("w", encoding="utf-8", newline="") as out_f:
        out_f.write(header.rstrip("\n") + "\n")
        with body_path.open("r", encoding="utf-8", newline="") as in_f:
            for line in in_f:
                out_f.write(line)
                count += 1
    return count


def write_taxon_version_index(sorted_taxon_by_taxid_path: Path, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(["tax_id", "version_count", "first_version", "last_version", "versions_csv"])

        prev_tax_id: int | None = None
        versions: list[str] = []
        seen_versions: set[str] = set()

        with sorted_taxon_by_taxid_path.open("r", encoding="utf-8", newline="") as in_f:
            for line in in_f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                version_id, tax_id_s = parts[0], parts[1]
                try:
                    tax_id = int(tax_id_s)
                except ValueError:
                    continue

                if prev_tax_id is None:
                    prev_tax_id = tax_id

                if tax_id != prev_tax_id:
                    if versions:
                        writer.writerow(
                            [
                                prev_tax_id,
                                len(versions),
                                versions[0],
                                versions[-1],
                                ",".join(versions),
                            ]
                        )
                        count += 1
                    prev_tax_id = tax_id
                    versions = []
                    seen_versions = set()

                if version_id not in seen_versions:
                    seen_versions.add(version_id)
                    versions.append(version_id)

        if prev_tax_id is not None and versions:
            writer.writerow([prev_tax_id, len(versions), versions[0], versions[-1], ",".join(versions)])
            count += 1

    return count


def write_taxon_name_version_index(sorted_name_by_key_path: Path, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "tax_id",
                "name_txt",
                "unique_name",
                "name_class",
                "version_count",
                "first_version",
                "last_version",
                "versions_csv",
            ]
        )

        prev_key: tuple[int, str, str, str] | None = None
        versions: list[str] = []
        seen_versions: set[str] = set()

        with sorted_name_by_key_path.open("r", encoding="utf-8", newline="") as in_f:
            for line in in_f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 5:
                    continue
                version_id, tax_id_s, name_txt, unique_name, name_class = parts[:5]
                try:
                    tax_id = int(tax_id_s)
                except ValueError:
                    continue

                key = (tax_id, name_txt, unique_name, name_class)

                if prev_key is None:
                    prev_key = key

                if key != prev_key:
                    if versions:
                        writer.writerow(
                            [
                                prev_key[0],
                                prev_key[1],
                                prev_key[2],
                                prev_key[3],
                                len(versions),
                                versions[0],
                                versions[-1],
                                ",".join(versions),
                            ]
                        )
                        count += 1
                    prev_key = key
                    versions = []
                    seen_versions = set()

                if version_id not in seen_versions:
                    seen_versions.add(version_id)
                    versions.append(version_id)

        if prev_key is not None and versions:
            writer.writerow(
                [
                    prev_key[0],
                    prev_key[1],
                    prev_key[2],
                    prev_key[3],
                    len(versions),
                    versions[0],
                    versions[-1],
                    ",".join(versions),
                ]
            )
            count += 1

    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Export taxdump record-sheet TSVs")
    parser.add_argument("--index-dir", default="data/index", help="Input directory with presence TSVs")
    parser.add_argument("--out-dir", default="data/record_sheets", help="Output directory for TSV sheets")
    args = parser.parse_args()

    index_dir = Path(args.index_dir)
    taxon_presence_path = index_dir / "taxon_presence.tsv"
    taxon_name_presence_path = index_dir / "taxon_name_presence.tsv"

    if not taxon_presence_path.exists():
        raise SystemExit(f"Input not found: {taxon_presence_path}")
    if not taxon_name_presence_path.exists():
        raise SystemExit(f"Input not found: {taxon_name_presence_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="taxdump_export_") as tmp:
        tmp_dir = Path(tmp)

        taxon_body = strip_header_to_temp(taxon_presence_path, tmp_dir)
        name_body = strip_header_to_temp(taxon_name_presence_path, tmp_dir)

        # taxon_in_version.tsv sorted by (version_id, tax_id)
        taxon_sorted_by_version = tmp_dir / "taxon.sorted.by_version.tsv"
        sort_file(taxon_body, taxon_sorted_by_version, ["-k1,1", "-k2,2n"], unique=True)
        taxon_in_version_count = write_with_header(
            out_dir / "taxon_in_version.tsv",
            "version_id\ttax_id",
            taxon_sorted_by_version,
        )

        # taxon_version_index.tsv grouped by tax_id
        taxon_sorted_by_taxid = tmp_dir / "taxon.sorted.by_taxid.tsv"
        sort_file(taxon_body, taxon_sorted_by_taxid, ["-k2,2n", "-k1,1"], unique=True)
        taxon_version_index_count = write_taxon_version_index(
            taxon_sorted_by_taxid, out_dir / "taxon_version_index.tsv"
        )

        # taxon_name_in_version.tsv sorted by (version_id, tax_id, name_txt, name_class)
        name_sorted_by_version = tmp_dir / "name.sorted.by_version.tsv"
        sort_file(name_body, name_sorted_by_version, ["-k1,1", "-k2,2n", "-k3,3", "-k5,5"], unique=True)
        taxon_name_in_version_count = write_with_header(
            out_dir / "taxon_name_in_version.tsv",
            "version_id\ttax_id\tname_txt\tunique_name\tname_class",
            name_sorted_by_version,
        )

        # taxon_name_version_index.tsv grouped by (tax_id, name_txt, unique_name, name_class)
        name_sorted_by_key = tmp_dir / "name.sorted.by_key.tsv"
        sort_file(name_body, name_sorted_by_key, ["-k2,2n", "-k3,3", "-k4,4", "-k5,5", "-k1,1"], unique=True)
        taxon_name_version_index_count = write_taxon_name_version_index(
            name_sorted_by_key, out_dir / "taxon_name_version_index.tsv"
        )

    print(f"taxon_in_version: {taxon_in_version_count} rows")
    print(f"taxon_version_index: {taxon_version_index_count} rows")
    print(f"taxon_name_in_version: {taxon_name_in_version_count} rows")
    print(f"taxon_name_version_index: {taxon_name_version_index_count} rows")
    print(f"Output directory: {out_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
