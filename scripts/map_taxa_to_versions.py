#!/usr/bin/env python3
"""Map taxa to viable taxdump versions using the presence DB."""

from __future__ import annotations

import argparse
import csv
import sqlite3
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


def versions_for_tax_id(conn: sqlite3.Connection, tax_id: int) -> set[str]:
    cur = conn.execute(
        "SELECT version_id FROM taxon_presence WHERE tax_id = ? ORDER BY version_id",
        (tax_id,),
    )
    return {row[0] for row in cur.fetchall()}


def versions_for_tax_name(conn: sqlite3.Connection, tax_id: int, name_txt: str) -> set[str]:
    cur = conn.execute(
        """
        SELECT version_id
        FROM taxon_name_presence
        WHERE tax_id = ? AND name_txt = ?
        ORDER BY version_id
        """,
        (tax_id, name_txt),
    )
    return {row[0] for row in cur.fetchall()}


def write_detail_tsv(path: Path, rows: list[dict[str, str | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "query_type",
            "tax_id",
            "name_txt",
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
    parser.add_argument("--db-path", default="data/index/presence.sqlite", help="Input SQLite DB")
    parser.add_argument("--tax-ids", help="Text file with one tax_id per line")
    parser.add_argument(
        "--tax-name-pairs",
        help="TSV with columns tax_id and name_txt for exact taxon-name matching",
    )
    parser.add_argument("--out-dir", default="data/query_results", help="Output directory")
    args = parser.parse_args()

    if not args.tax_ids and not args.tax_name_pairs:
        raise SystemExit("Provide --tax-ids and/or --tax-name-pairs")

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)

    all_sets: list[set[str]] = []
    detail_rows: list[dict[str, str | int]] = []

    if args.tax_ids:
        for tax_id in read_tax_ids(Path(args.tax_ids)):
            versions = versions_for_tax_id(conn, tax_id)
            all_sets.append(versions)
            detail_rows.append(
                {
                    "query_type": "tax_id",
                    "tax_id": tax_id,
                    "name_txt": "",
                    "version_count": len(versions),
                    "versions_csv": ",".join(sorted(versions)),
                }
            )

    if args.tax_name_pairs:
        for tax_id, name_txt in read_tax_name_pairs(Path(args.tax_name_pairs)):
            versions = versions_for_tax_name(conn, tax_id, name_txt)
            all_sets.append(versions)
            detail_rows.append(
                {
                    "query_type": "tax_id_name",
                    "tax_id": tax_id,
                    "name_txt": name_txt,
                    "version_count": len(versions),
                    "versions_csv": ",".join(sorted(versions)),
                }
            )

    conn.close()

    viable_versions = set.intersection(*all_sets) if all_sets else set()

    out_dir = Path(args.out_dir)
    details_path = out_dir / "query_details.tsv"
    viable_path = out_dir / "viable_versions.tsv"

    write_detail_tsv(details_path, detail_rows)
    write_intersection(viable_path, viable_versions)

    print(f"Wrote: {details_path}")
    print(f"Wrote: {viable_path}")
    print(f"Viable versions (intersection): {len(viable_versions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
