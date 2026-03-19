#!/usr/bin/env python3
"""Export record-sheet TSVs from the taxdump presence SQLite DB."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def query_rows(conn: sqlite3.Connection, sql: str):
    cur = conn.execute(sql)
    cols = [desc[0] for desc in cur.description]
    for row in cur:
        yield dict(zip(cols, row))


def write_tsv(path: Path, rows, fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Export taxdump record-sheet TSVs")
    parser.add_argument("--db-path", default="data/index/presence.sqlite", help="Input SQLite DB")
    parser.add_argument("--out-dir", default="data/record_sheets", help="Output directory for TSV sheets")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    counts = {}

    counts["taxon_in_version"] = write_tsv(
        out_dir / "taxon_in_version.tsv",
        query_rows(
            conn,
            """
            SELECT version_id, tax_id
            FROM taxon_presence
            ORDER BY version_id, tax_id
            """,
        ),
        ["version_id", "tax_id"],
    )

    counts["taxon_version_index"] = write_tsv(
        out_dir / "taxon_version_index.tsv",
        query_rows(
            conn,
            """
            SELECT
                tax_id,
                COUNT(*) AS version_count,
                MIN(version_id) AS first_version,
                MAX(version_id) AS last_version,
                GROUP_CONCAT(version_id, ',') AS versions_csv
            FROM taxon_presence
            GROUP BY tax_id
            ORDER BY tax_id
            """,
        ),
        ["tax_id", "version_count", "first_version", "last_version", "versions_csv"],
    )

    counts["taxon_name_in_version"] = write_tsv(
        out_dir / "taxon_name_in_version.tsv",
        query_rows(
            conn,
            """
            SELECT version_id, tax_id, name_txt, unique_name, name_class
            FROM taxon_name_presence
            ORDER BY version_id, tax_id, name_txt, name_class
            """,
        ),
        ["version_id", "tax_id", "name_txt", "unique_name", "name_class"],
    )

    counts["taxon_name_version_index"] = write_tsv(
        out_dir / "taxon_name_version_index.tsv",
        query_rows(
            conn,
            """
            SELECT
                tax_id,
                name_txt,
                COALESCE(unique_name, '') AS unique_name,
                name_class,
                COUNT(*) AS version_count,
                MIN(version_id) AS first_version,
                MAX(version_id) AS last_version,
                GROUP_CONCAT(version_id, ',') AS versions_csv
            FROM taxon_name_presence
            GROUP BY tax_id, name_txt, COALESCE(unique_name, ''), name_class
            ORDER BY tax_id, name_txt, name_class
            """,
        ),
        [
            "tax_id",
            "name_txt",
            "unique_name",
            "name_class",
            "version_count",
            "first_version",
            "last_version",
            "versions_csv",
        ],
    )

    conn.close()

    for key, value in counts.items():
        print(f"{key}: {value} rows")
    print(f"Output directory: {out_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
