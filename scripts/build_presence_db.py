#!/usr/bin/env python3
"""Build a SQLite presence database from archived NCBI taxdump zip files."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import zipfile
from pathlib import Path

BATCH_SIZE = 100_000


def parse_dmp_line(raw: str) -> list[str]:
    # NCBI .dmp rows are pipe-separated with surrounding tabs: "field\t|\tfield\t|"
    stripped = raw.rstrip("\n")
    if stripped.endswith("\t|"):
        stripped = stripped[:-2]
    parts = [part.strip() for part in stripped.split("\t|\t")]
    return parts


def iter_archive_files(archives_dir: Path, glob_pattern: str) -> list[Path]:
    archives = sorted(archives_dir.glob(glob_pattern))
    return [p for p in archives if p.is_file()]


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS versions (
            version_id TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            dump_date TEXT,
            processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS taxon_presence (
            version_id TEXT NOT NULL,
            tax_id INTEGER NOT NULL,
            PRIMARY KEY (version_id, tax_id)
        );

        CREATE TABLE IF NOT EXISTS taxon_name_presence (
            version_id TEXT NOT NULL,
            tax_id INTEGER NOT NULL,
            name_txt TEXT NOT NULL,
            unique_name TEXT,
            name_class TEXT NOT NULL,
            PRIMARY KEY (version_id, tax_id, name_txt, name_class)
        );

        CREATE INDEX IF NOT EXISTS idx_taxon_presence_taxid
            ON taxon_presence(tax_id, version_id);

        CREATE INDEX IF NOT EXISTS idx_taxon_name_presence_key
            ON taxon_name_presence(tax_id, name_txt, name_class, version_id);
        """
    )


def parse_dump_date(version_id: str) -> str | None:
    # Example: taxdmp_2024-01-01 -> 2024-01-01
    if "_" not in version_id:
        return None
    maybe_date = version_id.rsplit("_", 1)[-1]
    if len(maybe_date) == 10 and maybe_date[4] == "-" and maybe_date[7] == "-":
        return maybe_date
    return None


def ingest_archive(conn: sqlite3.Connection, archive_path: Path) -> tuple[int, int]:
    version_id = archive_path.stem
    dump_date = parse_dump_date(version_id)

    with conn:
        conn.execute(
            """
            INSERT INTO versions(version_id, filename, dump_date)
            VALUES(?, ?, ?)
            ON CONFLICT(version_id) DO UPDATE SET
                filename = excluded.filename,
                dump_date = excluded.dump_date,
                processed_at = CURRENT_TIMESTAMP
            """,
            (version_id, archive_path.name, dump_date),
        )

        conn.execute("DELETE FROM taxon_presence WHERE version_id = ?", (version_id,))
        conn.execute("DELETE FROM taxon_name_presence WHERE version_id = ?", (version_id,))

        with zipfile.ZipFile(archive_path, "r") as zf:
            with zf.open("nodes.dmp", "r") as nodes_file:
                taxon_rows_batch: list[tuple[str, int]] = []
                taxon_rows = 0
                for raw in nodes_file:
                    line = raw.decode("utf-8", errors="replace")
                    parts = parse_dmp_line(line)
                    if not parts or not parts[0]:
                        continue
                    try:
                        tax_id = int(parts[0])
                    except ValueError:
                        continue
                    taxon_rows_batch.append((version_id, tax_id))
                    if len(taxon_rows_batch) >= BATCH_SIZE:
                        conn.executemany(
                            "INSERT OR IGNORE INTO taxon_presence(version_id, tax_id) VALUES(?, ?)",
                            taxon_rows_batch,
                        )
                        taxon_rows += len(taxon_rows_batch)
                        taxon_rows_batch.clear()
                if taxon_rows_batch:
                    conn.executemany(
                        "INSERT OR IGNORE INTO taxon_presence(version_id, tax_id) VALUES(?, ?)",
                        taxon_rows_batch,
                    )
                    taxon_rows += len(taxon_rows_batch)
                    taxon_rows_batch.clear()

            with zf.open("names.dmp", "r") as names_file:
                name_rows_batch: list[tuple[str, int, str, str | None, str]] = []
                name_rows = 0
                for raw in names_file:
                    line = raw.decode("utf-8", errors="replace")
                    parts = parse_dmp_line(line)
                    if len(parts) < 4:
                        continue
                    try:
                        tax_id = int(parts[0])
                    except ValueError:
                        continue

                    name_txt = parts[1]
                    unique_name = parts[2] if parts[2] else None
                    name_class = parts[3]
                    name_rows_batch.append((version_id, tax_id, name_txt, unique_name, name_class))
                    if len(name_rows_batch) >= BATCH_SIZE:
                        conn.executemany(
                            """
                            INSERT OR IGNORE INTO taxon_name_presence(
                                version_id, tax_id, name_txt, unique_name, name_class
                            ) VALUES(?, ?, ?, ?, ?)
                            """,
                            name_rows_batch,
                        )
                        name_rows += len(name_rows_batch)
                        name_rows_batch.clear()
                if name_rows_batch:
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO taxon_name_presence(
                            version_id, tax_id, name_txt, unique_name, name_class
                        ) VALUES(?, ?, ?, ?, ?)
                        """,
                        name_rows_batch,
                    )
                    name_rows += len(name_rows_batch)
                    name_rows_batch.clear()

    return taxon_rows, name_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build taxon presence DB across taxdump versions")
    parser.add_argument("--archives-dir", default="data/archives", help="Directory containing *.zip archives")
    parser.add_argument("--archives-glob", default="*_*.zip", help="Glob pattern to select archives")
    parser.add_argument("--db-path", default="data/index/presence.sqlite", help="Output SQLite DB path")
    parser.add_argument(
        "--manifest-out",
        default="data/manifests/ingestion_manifest.tsv",
        help="Write per-archive ingestion status TSV",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit archives for test runs")
    args = parser.parse_args()

    archives_dir = Path(args.archives_dir)
    if not archives_dir.exists():
        print(f"Archive directory does not exist: {archives_dir}", file=sys.stderr)
        return 1

    archives = iter_archive_files(archives_dir, args.archives_glob)
    if args.limit is not None:
        archives = archives[: args.limit]

    if not archives:
        print("No archives found.", file=sys.stderr)
        return 1

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest_out)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    init_db(conn)

    rows: list[dict[str, str | int]] = []

    for archive in archives:
        version_id = archive.stem
        status = "ok"
        taxon_rows = 0
        name_rows = 0
        error = ""

        try:
            taxon_rows, name_rows = ingest_archive(conn, archive)
        except KeyError as exc:
            status = "missing_file"
            error = str(exc)
        except zipfile.BadZipFile as exc:
            status = "bad_zip"
            error = str(exc)
        except Exception as exc:  # noqa: BLE001
            status = "error"
            error = str(exc)

        rows.append(
            {
                "version_id": version_id,
                "filename": archive.name,
                "status": status,
                "taxon_rows": taxon_rows,
                "name_rows": name_rows,
                "error": error,
            }
        )

        print(f"{status:>12}  {archive.name}  taxa={taxon_rows}  names={name_rows}")

    conn.close()

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["version_id", "filename", "status", "taxon_rows", "name_rows", "error"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    ok = sum(1 for r in rows if r["status"] == "ok")
    print(f"Wrote DB: {db_path}")
    print(f"Wrote manifest: {manifest_path}")
    print(f"Archives processed: {len(rows)}, successful: {ok}")

    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
