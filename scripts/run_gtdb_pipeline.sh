#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${GTDB_OUT_DIR:-data/gtdb}"
INDEX_DIR="${GTDB_INDEX_DIR:-data/gtdb_index}"
MANIFEST_FETCH="${GTDB_FETCH_MANIFEST:-data/manifests/gtdb_taxonomy_manifest.tsv}"
MANIFEST_INGEST="${GTDB_INGEST_MANIFEST:-data/manifests/gtdb_ingestion_manifest.tsv}"
MAX_RELEASES="${GTDB_MAX_RELEASES:-}"
LIMIT_RELEASES="${GTDB_INGEST_LIMIT:-}"

FETCH_CMD=(python3 scripts/fetch_gtdb_taxonomy.py --out-dir "$OUT_DIR" --manifest "$MANIFEST_FETCH" --skip-existing)
if [[ -n "$MAX_RELEASES" ]]; then
  FETCH_CMD+=(--max-releases "$MAX_RELEASES")
fi
"${FETCH_CMD[@]}"

RUST_MANIFEST="Cargo.toml"
INGEST_CMD=(cargo run --release --manifest-path "$RUST_MANIFEST" --bin build_presence_gtdb -- \
  --taxonomy-dir "$OUT_DIR" \
  --index-dir "$INDEX_DIR" \
  --manifest-out "$MANIFEST_INGEST")
if [[ -n "$LIMIT_RELEASES" ]]; then
  INGEST_CMD+=(--limit "$LIMIT_RELEASES")
fi
"${INGEST_CMD[@]}"

printf '\nGTDB index files:\n'
printf '  %s\n' "$INDEX_DIR/version_columns.tsv"
printf '  %s\n' "$INDEX_DIR/taxid_matrix.tsv"
printf '  %s\n' "$INDEX_DIR/scientific_name_matrix.tsv"
printf '  %s\n' "$INDEX_DIR/taxid_scientific_name_matrix.tsv"
printf '  %s\n' "$INDEX_DIR/taxid_any_name_matrix.tsv"

mkdir -p data/manifests
if [[ -f data/manifests/index_paths.env ]]; then
  grep -v '^export GTDB_INDEX_DIR=' data/manifests/index_paths.env > data/manifests/index_paths.env.tmp || true
  mv data/manifests/index_paths.env.tmp data/manifests/index_paths.env
fi
{
  echo "export GTDB_INDEX_DIR=\"$INDEX_DIR\""
} >> data/manifests/index_paths.env
printf 'Updated env defaults: %s\n' "data/manifests/index_paths.env"
