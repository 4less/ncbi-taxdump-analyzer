# ncbi-taxdump-analyzer

Scripts to:
- scrape/download all available archived NCBI taxdumps,
- index taxon and taxon-name presence across versions,
- export record sheets that show where each taxon or `(tax_id, name_txt)` occurs,
- map a user-supplied set of taxa to viable taxdump versions.

## Scripts

- `scripts/fetch_taxdump_archives.py`
  - Scrapes `https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/`.
  - Downloads matching archives (default: `taxdmp_YYYY-MM-DD.zip` and `new_taxdump_YYYY-MM-DD.zip`).
  - Writes `data/manifests/archives_manifest.tsv`.

- `scripts/build_presence_db.py`
  - Reads downloaded zip files (`nodes.dmp`, `names.dmp`).
  - Builds `data/index/presence.sqlite` with:
    - `taxon_presence(version_id, tax_id)`
    - `taxon_name_presence(version_id, tax_id, name_txt, unique_name, name_class)`
  - Writes `data/manifests/ingestion_manifest.tsv`.

- `scripts/export_record_sheets.py`
  - Exports TSV record sheets:
    - `taxon_in_version.tsv`
    - `taxon_version_index.tsv`
    - `taxon_name_in_version.tsv`
    - `taxon_name_version_index.tsv`

- `scripts/map_taxa_to_versions.py`
  - Maps your taxon queries to taxdump versions.
  - Supports both:
    - tax IDs only
    - exact `(tax_id, name_txt)` pairs
  - Writes:
    - `data/query_results/query_details.tsv`
    - `data/query_results/viable_versions.tsv` (intersection across all input queries)

- `scripts/run_pipeline.sh`
  - End-to-end wrapper for fetch -> build DB -> export sheets.

## Quick start

Run full pipeline:

```bash
./scripts/run_pipeline.sh
```

Step-by-step:

```bash
python3 scripts/fetch_taxdump_archives.py --skip-existing
python3 scripts/build_presence_db.py
python3 scripts/export_record_sheets.py
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

Run mapping:

```bash
python3 scripts/map_taxa_to_versions.py \
  --tax-ids input/tax_ids.txt \
  --tax-name-pairs input/tax_name_pairs.tsv
```

The key output is `data/query_results/viable_versions.tsv`, containing versions where **all** requested taxa/tuples are present.

## Notes

- Outputs are TSV for easy loading into R/Pandas/SQL.
- `version_id` is the zip stem (for example `taxdmp_2024-01-01`).
- Re-running ingestion is idempotent per version: rows for that version are replaced.
- For quick tests use `--max-files` on fetch, and `--limit` on build.
# ncbi-taxdump-analyzer
