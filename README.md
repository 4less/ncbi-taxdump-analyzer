# ncbi-taxdump-analyzer

Scripts to:
- scrape/download all available archived NCBI taxdumps,
- scrape/download GTDB taxonomy releases,
- build compact version indexes in Rust,
- map a user-supplied set of taxa to viable taxdump versions in Rust.

## Scripts

- `scripts/fetch_taxdump_archives.py`
  - Scrapes `https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/`.
  - Downloads matching archives (default: `taxdmp_YYYY-MM-DD.zip` and `new_taxdump_YYYY-MM-DD.zip`).
  - Writes `data/manifests/archives_manifest.tsv`.

- `scripts/fetch_gtdb_taxonomy.py`
  - Scrapes GTDB release directories at `https://data.gtdb.ecogenomic.org/releases/`.
  - Downloads `bac120_taxonomy.tsv(.gz)` and `ar53_taxonomy.tsv(.gz)` per release.
  - Writes `data/manifests/gtdb_taxonomy_manifest.tsv`.

- `scripts/build_presence_db.py`
  - Legacy Python ingester (kept for reference).

- `build_presence` (Rust binary)
  - Reads downloaded zip files (`nodes.dmp`, `names.dmp`).
  - Builds compact TSV hash-map indexes using compact version IDs:
    - `data/index/taxid_versions.tsv`
    - `data/index/scientific_name_versions.tsv`
    - `data/index/taxid_scientific_name_versions.tsv`
    - `data/index/taxid_any_name_versions.tsv`
  - Version format is compact: `n-yy-mm` (new taxdump) / `t-yy-mm` (taxdmp).
  - Writes `data/manifests/ingestion_manifest.tsv`.

- `build_presence_gtdb` (Rust binary)
  - Reads downloaded GTDB taxonomy files (`gtdb_r*_bac120_taxonomy.tsv(.gz)`, `gtdb_r*_ar53_taxonomy.tsv(.gz)`).
  - Builds the same matrix index format used by the mapper into `data/gtdb_index`.
  - Uses compact GTDB version IDs: `g-r###` (for example `g-r226`).
  - GTDB rank-prefixed names are indexed with and without prefix (for example `d__`, `p__`, `c__`, `o__`, `f__`, `g__`, `s__`), so both query forms match.
  - Writes `data/manifests/gtdb_ingestion_manifest.tsv`.

- `map_taxa_to_versions` (Rust binary)
  - Reads the compact index TSVs and resolves:
    - tax IDs only
    - names only (`name_txt`)
    - exact `(tax_id, name_txt)` pairs
    - slash-delimited name alternatives in a single query (e.g. `A/B/C`) as OR-within-row
  - Scientific name matches have precedence; `--allow-synonym-fallback` enables fallback to non-scientific names.
  - By default, auto-loads available NCBI and GTDB indexes, detects best taxonomy match, and selects the best index.
  - Optional `--index-dir` can be provided multiple times to force/limit index candidates.
  - By default, all queries participate in a strict intersection to infer the single plausible version when possible.
  - `--ignore-failed` excludes failed/unmatched queries from that intersection.
  - Writes:
    - `prefix.details.log`
    - `prefix.warnings.log`
    - `prefix.result.log`

- `scripts/run_pipeline.sh`
  - End-to-end wrapper for fetch -> Rust compact index build.

- `scripts/run_gtdb_pipeline.sh`
  - End-to-end wrapper for GTDB fetch -> GTDB Rust compact index build.

## Quick start

Run full pipeline:

```bash
./scripts/run_pipeline.sh
```

Limit indexing scope to bacteria only:

```bash
TAXON_SCOPE=bacteria ./scripts/run_pipeline.sh
```

Limit indexing scope to custom root taxa (comma-separated taxids):

```bash
ROOT_TAXIDS=2,2157 ./scripts/run_pipeline.sh
```

Step-by-step:

```bash
python3 scripts/fetch_taxdump_archives.py --skip-existing
cargo run --release --manifest-path Cargo.toml --bin build_presence -- \
  --archives-dir data/archives \
  --index-dir data/index \
  --manifest-out data/manifests/ingestion_manifest.tsv
```

GTDB pipeline:

```bash
./scripts/run_gtdb_pipeline.sh
```

GTDB step-by-step:

```bash
python3 scripts/fetch_gtdb_taxonomy.py --skip-existing
cargo run --release --manifest-path Cargo.toml --bin build_presence_gtdb -- \
  --taxonomy-dir data/gtdb \
  --index-dir data/gtdb_index \
  --manifest-out data/manifests/gtdb_ingestion_manifest.tsv
```

## Mapping a set of taxa to viable versions

Tax IDs file (`input/tax_ids.txt`, one tax_id per line):

```text
9606
10090
7227
```

Tax ID + name file (`input/tax_name_pairs.tsv`):

```tsv
tax_id	name_txt
9606	Homo sapiens
10090	Mus musculus
```

Lineage pairs are also supported in the same `--tax-name-pairs` file
(no header required), using `|`-separated levels:

```tsv
2|1224|1236|91347|543	Bacteria|Proteobacteria|Gammaproteobacteria|Enterobacterales|Enterobacteriaceae
2|1224|1236|(135623/91347)	Bacteria|Proteobacteria|Gammaproteobacteria|(Vibrionales/Enterobacterales)
```

Names file (`input/names.txt`, one exact `name_txt` per line):

```text
Homo sapiens
Mus musculus
```

For GTDB, a full lineage in one line is also supported and treated as
AND across levels (split on `;`), e.g.:

```text
d__Bacteria;p__Bacillota_A;c__Clostridia;o__Tissierellales;f__Peptoniphilaceae;g__Ezakiella;s__Ezakiella massiliensis
```

Run mapping:

```bash
cargo run --release --manifest-path Cargo.toml --bin map_taxa_to_versions -- \
  --index-dir data/index \
  --tax-ids input/tax_ids.txt \
  --names input/names.txt \
  --tax-name-pairs input/tax_name_pairs.tsv \
  --output-prefix data/query_results/query
```

If `--index-dir` is omitted, mapper uses:
- `TAXON_INDEX_DIRS` (comma/colon/semicolon-separated), or
- `NCBI_INDEX_DIR` and `GTDB_INDEX_DIR` (defaults: `data/index`, `data/gtdb_index`).

Allow synonym fallback when there is no scientific-name match:

`--allow-synonym-fallback`

Exclude failed/unmatched queries from intersection:

`--ignore-failed`

The key output is `prefix.result.log` (also printed to stdout), with detailed rows in `prefix.details.log` and warnings in `prefix.warnings.log`.

## Notes

- Outputs are TSV.
- Compact version IDs use `n-yy-mm` / `t-yy-mm`.
- No SQLite dependency in the pipeline.
- Re-running ingestion rewrites compact index TSVs from archives.
- For quick tests use `--max-files` on fetch, and `--limit` on build.
