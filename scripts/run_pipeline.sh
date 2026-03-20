#!/usr/bin/env bash
set -euo pipefail

ARCHIVE_DIR=${ARCHIVE_DIR:-data/archives}
INDEX_DIR=${INDEX_DIR:-data/index}
RUST_MANIFEST=${RUST_MANIFEST:-Cargo.toml}
INGEST_WORKERS=${INGEST_WORKERS:-}
INGEST_LIMIT=${INGEST_LIMIT:-}
TAXON_SCOPE=${TAXON_SCOPE:-all}
ROOT_TAXIDS=${ROOT_TAXIDS:-}

python3 scripts/fetch_taxdump_archives.py \
  --out-dir "$ARCHIVE_DIR" \
  --manifest data/manifests/archives_manifest.tsv \
  --skip-existing

RUST_CMD=(cargo run --release --manifest-path "$RUST_MANIFEST" --bin build_presence -- \
  --archives-dir "$ARCHIVE_DIR" \
  --index-dir "$INDEX_DIR" \
  --manifest-out data/manifests/ingestion_manifest.tsv \
  --taxon-scope "$TAXON_SCOPE")

if [[ -n "$INGEST_WORKERS" ]]; then
  RUST_CMD+=(--workers "$INGEST_WORKERS")
fi

if [[ -n "$INGEST_LIMIT" ]]; then
  RUST_CMD+=(--limit "$INGEST_LIMIT")
fi

if [[ -n "$ROOT_TAXIDS" ]]; then
  IFS=',' read -r -a ROOT_TAXID_ARR <<< "$ROOT_TAXIDS"
  for taxid in "${ROOT_TAXID_ARR[@]}"; do
    taxid_trimmed="$(echo "$taxid" | xargs)"
    if [[ -n "$taxid_trimmed" ]]; then
      RUST_CMD+=(--root-taxid "$taxid_trimmed")
    fi
  done
fi

"${RUST_CMD[@]}"

printf 'Pipeline complete.\n'
printf 'Index dir: %s\n' "$INDEX_DIR"
printf 'Indexes written:\n'
printf '  %s\n' "$INDEX_DIR/version_columns.tsv"
printf '  %s\n' "$INDEX_DIR/taxid_matrix.tsv"
printf '  %s\n' "$INDEX_DIR/scientific_name_matrix.tsv"
printf '  %s\n' "$INDEX_DIR/taxid_scientific_name_matrix.tsv"
printf '  %s\n' "$INDEX_DIR/taxid_any_name_matrix.tsv"

mkdir -p data/manifests
{
  echo "export NCBI_INDEX_DIR=\"$INDEX_DIR\""
} > data/manifests/index_paths.env
printf 'Wrote env defaults: %s\n' "data/manifests/index_paths.env"
