#!/usr/bin/env python3
"""Build TSV presence tables from archived NCBI taxdump zip files."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
import io
import os
import shutil
import sys
import tempfile
import time
import zipfile
from pathlib import Path


def parse_dmp_line(raw: str) -> list[str]:
    # NCBI .dmp rows are pipe-separated with surrounding tabs: "field\t|\tfield\t|"
    stripped = raw.rstrip("\n")
    if stripped.endswith("\t|"):
        stripped = stripped[:-2]
    return [part.strip() for part in stripped.split("\t|\t")]


def iter_archive_files(archives_dir: Path, glob_pattern: str) -> list[Path]:
    archives = sorted(archives_dir.glob(glob_pattern))
    return [p for p in archives if p.is_file()]


def parse_dump_date(version_id: str) -> str | None:
    if "_" not in version_id:
        return None
    maybe_date = version_id.rsplit("_", 1)[-1]
    if len(maybe_date) == 10 and maybe_date[4] == "-" and maybe_date[7] == "-":
        return maybe_date
    return None


def process_nodes_stream(nodes_stream, version_id: str, taxon_writer: csv.writer) -> int:
    taxon_rows = 0
    for line in nodes_stream:
        parts = parse_dmp_line(line)
        if not parts or not parts[0]:
            continue
        try:
            tax_id = int(parts[0])
        except ValueError:
            continue
        taxon_writer.writerow([version_id, tax_id])
        taxon_rows += 1
    return taxon_rows


def process_names_stream(names_stream, version_id: str, name_writer: csv.writer) -> int:
    name_rows = 0
    for line in names_stream:
        parts = parse_dmp_line(line)
        if len(parts) < 4:
            continue
        try:
            tax_id = int(parts[0])
        except ValueError:
            continue

        name_txt = parts[1]
        unique_name = parts[2]
        name_class = parts[3]
        name_writer.writerow([version_id, tax_id, name_txt, unique_name, name_class])
        name_rows += 1
    return name_rows


def ingest_archive_to_temp(archive_path_s: str, tmp_dir_s: str) -> dict[str, str | int | float]:
    archive_path = Path(archive_path_s)
    tmp_dir = Path(tmp_dir_s)
    version_id = archive_path.stem

    taxon_tmp = tmp_dir / f"{version_id}.taxon.tsv"
    name_tmp = tmp_dir / f"{version_id}.name.tsv"

    status = "ok"
    taxon_rows = 0
    name_rows = 0
    load_seconds = 0.0
    process_seconds = 0.0
    error = ""

    archive_start = time.perf_counter()
    try:
        load_start = time.perf_counter()
        with zipfile.ZipFile(archive_path, "r") as zf:
            nodes_bytes = zf.read("nodes.dmp")
            names_bytes = zf.read("names.dmp")
        load_seconds = time.perf_counter() - load_start

        process_start = time.perf_counter()
        with taxon_tmp.open("w", newline="", encoding="utf-8") as taxon_f:
            taxon_writer = csv.writer(taxon_f, delimiter="\t", lineterminator="\n")
            with io.TextIOWrapper(io.BytesIO(nodes_bytes), encoding="utf-8", errors="replace") as nodes_stream:
                taxon_rows = process_nodes_stream(nodes_stream, version_id, taxon_writer)

        with name_tmp.open("w", newline="", encoding="utf-8") as name_f:
            name_writer = csv.writer(name_f, delimiter="\t", lineterminator="\n")
            with io.TextIOWrapper(io.BytesIO(names_bytes), encoding="utf-8", errors="replace") as names_stream:
                name_rows = process_names_stream(names_stream, version_id, name_writer)

        process_seconds = time.perf_counter() - process_start
    except KeyError as exc:
        status = "missing_file"
        error = str(exc)
    except zipfile.BadZipFile as exc:
        status = "bad_zip"
        error = str(exc)
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error = str(exc)

    total_seconds = time.perf_counter() - archive_start

    if status != "ok":
        if taxon_tmp.exists():
            taxon_tmp.unlink()
        if name_tmp.exists():
            name_tmp.unlink()

    return {
        "version_id": version_id,
        "filename": archive_path.name,
        "dump_date": parse_dump_date(version_id) or "",
        "status": status,
        "taxon_rows": taxon_rows,
        "name_rows": name_rows,
        "load_seconds": f"{load_seconds:.3f}",
        "process_seconds": f"{process_seconds:.3f}",
        "total_seconds": f"{total_seconds:.3f}",
        "error": error,
        "taxon_tmp": str(taxon_tmp) if status == "ok" else "",
        "name_tmp": str(name_tmp) if status == "ok" else "",
    }


def append_file(src: Path, dst_f) -> None:
    with src.open("r", encoding="utf-8", newline="") as s:
        shutil.copyfileobj(s, dst_f)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build taxon presence TSVs across taxdump versions")
    parser.add_argument("--archives-dir", default="data/archives", help="Directory containing *.zip archives")
    parser.add_argument("--archives-glob", default="*_*.zip", help="Glob pattern to select archives")
    parser.add_argument(
        "--index-dir",
        default=str(Path.home() / ".taxdet" / "index" / "ncbi_index"),
        help="Output directory for TSV index tables",
    )
    parser.add_argument(
        "--manifest-out",
        default="data/manifests/ingestion_manifest.tsv",
        help="Write per-archive ingestion status TSV",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit archives for test runs")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (os.cpu_count() or 1) // 2),
        help="Number of worker processes (archive-level parallelism)",
    )
    args = parser.parse_args()

    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")

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

    index_dir = Path(args.index_dir)
    index_dir.mkdir(parents=True, exist_ok=True)
    taxon_presence_path = index_dir / "taxon_presence.tsv"
    taxon_name_presence_path = index_dir / "taxon_name_presence.tsv"

    manifest_path = Path(args.manifest_out)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    order = {p.stem: i for i, p in enumerate(archives)}
    rows: list[dict[str, str | int]] = []

    with tempfile.TemporaryDirectory(prefix="taxdump_ingest_") as tmp_dir_s:
        # Process archives in parallel; each worker writes per-archive temporary bodies.
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(ingest_archive_to_temp, str(a), tmp_dir_s) for a in archives]
            for fut in as_completed(futures):
                row = fut.result()
                rows.append(row)
                print(
                    f"{str(row['status']):>12}  {row['filename']}  "
                    f"taxa={row['taxon_rows']}  names={row['name_rows']}  "
                    f"load={float(str(row['load_seconds'])):.2f}s  "
                    f"process={float(str(row['process_seconds'])):.2f}s  "
                    f"total={float(str(row['total_seconds'])):.2f}s"
                )

        # Merge worker outputs in deterministic archive order.
        rows.sort(key=lambda r: order.get(str(r["version_id"]), 10**9))

        with taxon_presence_path.open("w", newline="", encoding="utf-8") as taxon_f, taxon_name_presence_path.open(
            "w", newline="", encoding="utf-8"
        ) as name_f:
            taxon_f.write("version_id\ttax_id\n")
            name_f.write("version_id\ttax_id\tname_txt\tunique_name\tname_class\n")

            for row in rows:
                if row["status"] != "ok":
                    continue
                append_file(Path(str(row["taxon_tmp"])), taxon_f)
                append_file(Path(str(row["name_tmp"])), name_f)

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "version_id",
            "filename",
            "dump_date",
            "status",
            "taxon_rows",
            "name_rows",
            "load_seconds",
            "process_seconds",
            "total_seconds",
            "error",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    ok = sum(1 for r in rows if r["status"] == "ok")
    print(f"Wrote taxon presence TSV: {taxon_presence_path}")
    print(f"Wrote taxon-name presence TSV: {taxon_name_presence_path}")
    print(f"Wrote manifest: {manifest_path}")
    print(f"Archives processed: {len(rows)}, successful: {ok}")

    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
