#!/usr/bin/env python3
"""Discover and download GTDB release taxonomy tables."""

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

DEFAULT_RELEASES_URL = "https://data.gtdb.ecogenomic.org/releases/"


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


def fetch_listing(url: str) -> list[str]:
    req = urllib.request.Request(url, headers={"User-Agent": "ncbi-taxdump-analyzer/1.0"})
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    p = _HrefParser()
    p.feed(body)
    return p.hrefs


def find_release_dirs(hrefs: Iterable[str]) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    seen: set[int] = set()
    for href in hrefs:
        name = href.strip().rstrip("/")
        m = re.fullmatch(r"release(\d+)", name)
        if not m:
            continue
        rel = int(m.group(1))
        if rel in seen:
            continue
        seen.add(rel)
        out.append((rel, f"release{rel}/"))
    out.sort(key=lambda x: x[0])
    return out


def find_subrelease_dir(hrefs: Iterable[str]) -> str | None:
    best: tuple[tuple[int, ...], str] | None = None
    for href in hrefs:
        name = href.strip().rstrip("/")
        m = re.fullmatch(r"(\d+)\.(\d+)", name)
        if not m:
            continue
        key = (int(m.group(1)), int(m.group(2)))
        if best is None or key > best[0]:
            best = (key, f"{name}/")
    return None if best is None else best[1]


def choose_taxonomy_name(hrefs: Iterable[str], stem: str) -> str | None:
    names = [h.split("?")[0] for h in hrefs]
    # Newer GTDB releases use names like bac120_taxonomy_r226.tsv(.gz),
    # older ones may use bac120_taxonomy.tsv(.gz).
    patterns = [
        re.compile(rf"^{re.escape(stem)}_taxonomy_r\d+\.tsv\.gz$"),
        re.compile(rf"^{re.escape(stem)}_taxonomy_r\d+\.tsv$"),
        re.compile(rf"^{re.escape(stem)}_taxonomy\.tsv\.gz$"),
        re.compile(rf"^{re.escape(stem)}_taxonomy\.tsv$"),
    ]
    for pat in patterns:
        matches = sorted([n for n in names if pat.fullmatch(n)])
        if matches:
            return matches[-1]
    return None


def download_file(url: str, target: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "ncbi-taxdump-analyzer/1.0"})
    with urllib.request.urlopen(req) as resp, target.open("wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)


def main() -> int:
    ap = argparse.ArgumentParser(description="Download GTDB taxonomy files across releases")
    ap.add_argument("--releases-url", default=DEFAULT_RELEASES_URL)
    ap.add_argument("--out-dir", default="data/gtdb")
    ap.add_argument("--manifest", default="data/manifests/gtdb_taxonomy_manifest.tsv")
    ap.add_argument("--max-releases", type=int, default=None)
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        root_hrefs = fetch_listing(args.releases_url)
    except urllib.error.URLError as exc:
        print(f"Failed to fetch GTDB releases listing: {exc}", file=sys.stderr)
        return 1

    releases = find_release_dirs(root_hrefs)
    if args.max_releases is not None:
        releases = releases[-args.max_releases :]

    if not releases:
        print("No GTDB release directories found.", file=sys.stderr)
        return 1

    rows: list[dict[str, str | int]] = []

    for rel_num, rel_dir in releases:
        rel_url = urllib.parse.urljoin(args.releases_url, rel_dir)
        try:
            rel_hrefs = fetch_listing(rel_url)
        except urllib.error.URLError as exc:
            print(f"ERROR release {rel_num}: {exc}", file=sys.stderr)
            rows.append(
                {
                    "release": f"r{rel_num}",
                    "group": "all",
                    "filename": "",
                    "url": rel_url,
                    "local_path": "",
                    "status": f"error:{exc}",
                    "downloaded": 0,
                    "size_bytes": 0,
                    "sha256": "",
                }
            )
            continue

        sub = find_subrelease_dir(rel_hrefs)
        if sub is None:
            rows.append(
                {
                    "release": f"r{rel_num}",
                    "group": "all",
                    "filename": "",
                    "url": rel_url,
                    "local_path": "",
                    "status": "missing_subrelease",
                    "downloaded": 0,
                    "size_bytes": 0,
                    "sha256": "",
                }
            )
            continue

        sub_url = urllib.parse.urljoin(rel_url, sub)
        try:
            sub_hrefs = fetch_listing(sub_url)
        except urllib.error.URLError as exc:
            rows.append(
                {
                    "release": f"r{rel_num}",
                    "group": "all",
                    "filename": "",
                    "url": sub_url,
                    "local_path": "",
                    "status": f"error:{exc}",
                    "downloaded": 0,
                    "size_bytes": 0,
                    "sha256": "",
                }
            )
            continue

        # Archaea marker set changed from ar122 (r89–r202) to ar53 (r207+).
        # Try ar53 first; fall back to ar122 for older releases.
        archaea_stem = "ar53"
        if choose_taxonomy_name(sub_hrefs, "ar53") is None and choose_taxonomy_name(sub_hrefs, "ar122") is not None:
            archaea_stem = "ar122"

        for grp in ("bac120", archaea_stem):
            src_name = choose_taxonomy_name(sub_hrefs, grp)
            if not src_name:
                rows.append(
                    {
                        "release": f"r{rel_num}",
                        "group": grp,
                        "filename": "",
                        "url": sub_url,
                        "local_path": "",
                        "status": "missing_taxonomy_file",
                        "downloaded": 0,
                        "size_bytes": 0,
                        "sha256": "",
                    }
                )
                continue

            src_url = urllib.parse.urljoin(sub_url, src_name)
            ext = ".tsv.gz" if src_name.endswith(".tsv.gz") else ".tsv"
            out_name = f"gtdb_r{rel_num}_{grp}_taxonomy{ext}"
            target = out_dir / out_name

            status = "downloaded"
            downloaded = 0
            if args.skip_existing and target.exists():
                status = "skipped_existing"
            else:
                try:
                    download_file(src_url, target)
                except urllib.error.URLError as exc:
                    status = f"error:{exc}"
                    print(f"ERROR r{rel_num} {grp}: {exc}", file=sys.stderr)
                else:
                    downloaded = 1

            rows.append(
                {
                    "release": f"r{rel_num}",
                    "group": grp,
                    "filename": out_name,
                    "url": src_url,
                    "local_path": str(target),
                    "status": status,
                    "downloaded": downloaded,
                    "size_bytes": target.stat().st_size if target.exists() else 0,
                    "sha256": sha256sum(target) if target.exists() else "",
                }
            )
            print(f"{status:>16}  {out_name}")

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "release",
            "group",
            "filename",
            "url",
            "local_path",
            "status",
            "downloaded",
            "size_bytes",
            "sha256",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(rows)

    downloaded_now = sum(int(r["downloaded"]) for r in rows)
    print(f"Wrote manifest: {manifest_path}")
    print(f"Rows: {len(rows)}, downloaded now: {downloaded_now}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
