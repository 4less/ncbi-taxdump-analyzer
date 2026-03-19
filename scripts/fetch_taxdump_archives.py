#!/usr/bin/env python3
"""Discover and download NCBI taxdump archives."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html.parser
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable

DEFAULT_ARCHIVE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/"
DEFAULT_PATTERN = r"^(taxdmp|new_taxdump)_(\d{4}-\d{2}-\d{2})\.zip$"


class _HrefParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for key, value in attrs:
            if key == "href" and value:
                self.hrefs.append(value)


def sha256sum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_listing(base_url: str) -> list[str]:
    req = urllib.request.Request(base_url, headers={"User-Agent": "ncbi-taxdump-analyzer/1.0"})
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    parser = _HrefParser()
    parser.feed(body)
    return parser.hrefs


def iter_archives(hrefs: Iterable[str], pattern: re.Pattern[str]) -> list[tuple[str, str, str]]:
    seen: set[str] = set()
    matches: list[tuple[str, str, str]] = []
    for href in hrefs:
        name = href.split("?")[0].strip()
        match = pattern.match(name)
        if not match:
            continue
        if name in seen:
            continue
        seen.add(name)
        dump_type, dump_date = match.group(1), match.group(2)
        matches.append((name, dump_type, dump_date))

    matches.sort(key=lambda x: (x[2], x[0]))
    return matches


def download_file(url: str, target: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "ncbi-taxdump-analyzer/1.0"})
    with urllib.request.urlopen(req) as resp, target.open("wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download all available NCBI taxdump archives.")
    parser.add_argument("--archive-url", default=DEFAULT_ARCHIVE_URL, help="Archive listing URL")
    parser.add_argument("--filename-regex", default=DEFAULT_PATTERN, help="Regex for archive file names")
    parser.add_argument("--out-dir", default="data/archives", help="Directory for downloaded zip files")
    parser.add_argument("--manifest", default="data/manifests/archives_manifest.tsv", help="Output TSV manifest")
    parser.add_argument("--max-files", type=int, default=None, help="Limit number of files for test runs")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading files already present")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        regex = re.compile(args.filename_regex)
    except re.error as exc:
        print(f"Invalid regex: {exc}", file=sys.stderr)
        return 2

    try:
        hrefs = fetch_listing(args.archive_url)
    except urllib.error.URLError as exc:
        print(f"Failed to fetch listing from {args.archive_url}: {exc}", file=sys.stderr)
        return 1

    archives = iter_archives(hrefs, regex)
    if args.max_files is not None:
        archives = archives[: args.max_files]

    if not archives:
        print("No archive files matched the configured pattern.", file=sys.stderr)
        return 1

    rows: list[dict[str, str | int]] = []

    for name, dump_type, dump_date in archives:
        source_url = urllib.parse.urljoin(args.archive_url, name)
        target_path = out_dir / name

        downloaded = 0
        status = "downloaded"

        if args.skip_existing and target_path.exists():
            status = "skipped_existing"
        else:
            try:
                download_file(source_url, target_path)
            except urllib.error.URLError as exc:
                status = f"error:{exc.reason if hasattr(exc, 'reason') else exc}"
                print(f"ERROR {name}: {exc}", file=sys.stderr)
            else:
                downloaded = 1

        size_bytes = target_path.stat().st_size if target_path.exists() else 0
        sha256 = sha256sum(target_path) if target_path.exists() else ""

        rows.append(
            {
                "filename": name,
                "version_id": Path(name).stem,
                "dump_type": dump_type,
                "dump_date": dump_date,
                "url": source_url,
                "local_path": str(target_path),
                "size_bytes": size_bytes,
                "sha256": sha256,
                "status": status,
                "downloaded": downloaded,
            }
        )

        print(f"{status:>16}  {name}")

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "filename",
            "version_id",
            "dump_type",
            "dump_date",
            "url",
            "local_path",
            "size_bytes",
            "sha256",
            "status",
            "downloaded",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    downloaded_count = sum(int(r["downloaded"]) for r in rows)
    print(f"Wrote manifest: {manifest_path}")
    print(f"Archives matched: {len(rows)}, downloaded now: {downloaded_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
