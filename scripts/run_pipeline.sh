#!/usr/bin/env bash
set -euo pipefail

ARCHIVE_DIR=${ARCHIVE_DIR:-data/archives}
DB_PATH=${DB_PATH:-data/index/presence.sqlite}
SHEETS_DIR=${SHEETS_DIR:-data/record_sheets}

python3 scripts/fetch_taxdump_archives.py \
  --out-dir "$ARCHIVE_DIR" \
  --manifest data/manifests/archives_manifest.tsv \
  --skip-existing

python3 scripts/build_presence_db.py \
  --archives-dir "$ARCHIVE_DIR" \
  --db-path "$DB_PATH" \
  --manifest-out data/manifests/ingestion_manifest.tsv

python3 scripts/export_record_sheets.py \
  --db-path "$DB_PATH" \
  --out-dir "$SHEETS_DIR"

printf 'Pipeline complete.\n'
printf 'DB: %s\n' "$DB_PATH"
printf 'Record sheets: %s\n' "$SHEETS_DIR"
