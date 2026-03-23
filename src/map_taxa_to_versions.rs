use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(about = "Map taxa to viable taxdump versions using bitset matrices")]
struct Args {
    #[arg(long = "index-dir")]
    index_dirs: Vec<PathBuf>,

    #[arg(long)]
    tax_ids: Option<PathBuf>,

    #[arg(long)]
    names: Option<PathBuf>,

    #[arg(long)]
    tax_name_pairs: Option<PathBuf>,

    #[arg(long)]
    allow_synonym_fallback: bool,

    #[arg(long)]
    ignore_failed: bool,

    #[arg(long, default_value = "data/query_results/query")]
    output_prefix: PathBuf,
}

#[derive(Clone)]
struct DetailRow {
    query_type: String,
    tax_id: String,
    name_txt: String,
    match_source: String,
    bitset: Vec<u64>,
}

#[derive(Clone)]
struct PairRequest {
    alternatives: Vec<(u32, String)>,
    display_tax_id: String,
    display_name_txt: String,
}

#[derive(Clone)]
struct QueryInputs {
    req_tax_ids: Vec<u32>,
    req_names: Vec<String>,
    req_pair_requests: Vec<PairRequest>,
}

struct EvalResult {
    index_dir: PathBuf,
    taxonomy: String,
    version_cols: Vec<String>,
    details: Vec<DetailRow>,
    viable_versions: Vec<String>,
    failed_queries: usize,
    used_for_intersection: usize,
    warnings: Vec<String>,
}

fn read_lines(path: &Path) -> Result<Vec<String>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let s = line?;
        let t = s.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        out.push(t.to_string());
    }
    Ok(out)
}

fn read_tax_ids(path: &Path) -> Result<Vec<u32>> {
    read_lines(path)?
        .into_iter()
        .map(|s| s.parse::<u32>().with_context(|| format!("invalid tax_id: {s}")))
        .collect()
}

fn read_names(path: &Path) -> Result<Vec<String>> {
    read_lines(path)
}

fn expand_name_queries(names: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for name in names {
        if name.contains(';') {
            for token in name.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                out.push(token.to_string());
            }
        } else {
            out.push(name);
        }
    }
    out
}

fn split_alt_tokens(raw: &str) -> Vec<String> {
    let t = raw.trim();
    let inner = if t.starts_with('(') && t.ends_with(')') && t.len() >= 2 {
        &t[1..t.len() - 1]
    } else {
        t
    };
    inner
        .split('/')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn split_lineage_tokens(raw: &str) -> Vec<String> {
    raw.split('|')
        .map(|s| s.trim())
        .map(|s| s.to_string())
        .collect()
}

fn parse_taxid_alts(raw: &str) -> Result<Vec<u32>> {
    let alts = split_alt_tokens(raw);
    if alts.is_empty() {
        anyhow::bail!("empty tax_id token");
    }
    alts
        .into_iter()
        .map(|x| x.parse::<u32>().with_context(|| format!("invalid tax_id: {x}")))
        .collect()
}

fn aligned_alternatives(tax_token: &str, name_token: &str) -> Result<Vec<(u32, String)>> {
    let tax_alts = parse_taxid_alts(tax_token)?;
    let name_alts = split_alt_tokens(name_token);
    if name_alts.is_empty() {
        anyhow::bail!("empty name token");
    }
    let mut out = Vec::new();
    if tax_alts.len() == name_alts.len() {
        for (t, n) in tax_alts.into_iter().zip(name_alts.into_iter()) {
            out.push((t, n));
        }
    } else if tax_alts.len() == 1 {
        for n in name_alts {
            out.push((tax_alts[0], n));
        }
    } else if name_alts.len() == 1 {
        for t in tax_alts {
            out.push((t, name_alts[0].clone()));
        }
    } else {
        // Fallback for malformed mixed alternatives: broaden to cartesian product.
        for t in &tax_alts {
            for n in &name_alts {
                out.push((*t, n.clone()));
            }
        }
    }
    Ok(out)
}

fn read_tax_name_pairs(path: &Path) -> Result<(Vec<PairRequest>, Vec<String>)> {
    let lines = read_lines(path)?;
    let mut out = Vec::new();
    let mut warnings = Vec::new();

    for (lineno, line) in lines.into_iter().enumerate() {
        let mut parts = line.splitn(2, '\t');
        let left = parts.next().unwrap_or_default().trim();
        let right = parts.next().unwrap_or_default().trim();
        if right.is_empty() {
            anyhow::bail!(
                "invalid line {} in {}: expected two tab-separated columns",
                lineno + 1,
                path.display()
            );
        }

        if lineno == 0 && left.eq_ignore_ascii_case("tax_id") && right.eq_ignore_ascii_case("name_txt")
        {
            continue;
        }

        let tax_levels = split_lineage_tokens(left);
        let name_levels = split_lineage_tokens(right);
        if tax_levels.len() != name_levels.len() {
            warnings.push(format!(
                "line {} in {} has mismatched lineage depth: {} vs {}; applying best-effort level alignment.",
                lineno + 1,
                path.display(),
                tax_levels.len(),
                name_levels.len()
            ));
        }

        let max_levels = tax_levels.len().max(name_levels.len());
        for level_idx in 0..max_levels {
            let tax_token = tax_levels.get(level_idx).map(String::as_str).unwrap_or("");
            let name_token = name_levels.get(level_idx).map(String::as_str).unwrap_or("");
            if tax_token.trim().is_empty() || name_token.trim().is_empty() {
                warnings.push(format!(
                    "line {} level {} in {} has missing tax_id or name; skipping this level.",
                    lineno + 1,
                    level_idx + 1,
                    path.display()
                ));
                continue;
            }

            let alternatives = match aligned_alternatives(tax_token, name_token) {
                Ok(a) => a,
                Err(err) => {
                    warnings.push(format!(
                        "line {} level {} in {} could not parse alternatives ({}); skipping this level.",
                        lineno + 1,
                        level_idx + 1,
                        path.display(),
                        err
                    ));
                    continue;
                }
            };
            out.push(PairRequest {
                alternatives,
                display_tax_id: tax_token.to_string(),
                display_name_txt: name_token.to_string(),
            });
        }
    }

    Ok((out, warnings))
}

fn parse_bitset_hex(s: &str, chunk_count: usize) -> Vec<u64> {
    if chunk_count == 0 {
        return Vec::new();
    }
    let mut out = vec![0_u64; chunk_count];
    for (i, slot) in out.iter_mut().enumerate().take(chunk_count) {
        let start = i * 16;
        let end = start + 16;
        if end <= s.len() {
            *slot = u64::from_str_radix(&s[start..end], 16).unwrap_or(0);
        }
    }
    out
}

fn and_assign(dst: &mut [u64], src: &[u64]) {
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d &= *s;
    }
}

fn or_assign(dst: &mut [u64], src: &[u64]) {
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d |= *s;
    }
}

fn bitset_is_empty(bs: &[u64]) -> bool {
    bs.iter().all(|x| *x == 0)
}

fn split_name_variants(name_txt: &str) -> Vec<String> {
    let variants: Vec<String> = name_txt
        .split('/')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if variants.is_empty() {
        vec![name_txt.trim().to_string()]
    } else {
        variants
    }
}

fn bitset_to_versions(bs: &[u64], version_cols: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for (i, version) in version_cols.iter().enumerate() {
        let chunk = i / 64;
        let bit = i % 64;
        if chunk < bs.len() && ((bs[chunk] >> bit) & 1) == 1 {
            out.push(version.clone());
        }
    }
    out
}

fn required_index_files(index_dir: &Path) -> [PathBuf; 5] {
    [
        index_dir.join("version_columns.tsv"),
        index_dir.join("taxid_matrix.tsv"),
        index_dir.join("scientific_name_matrix.tsv"),
        index_dir.join("taxid_scientific_name_matrix.tsv"),
        index_dir.join("taxid_any_name_matrix.tsv"),
    ]
}

fn has_required_index(index_dir: &Path) -> bool {
    required_index_files(index_dir).iter().all(|p| p.exists())
}

fn detect_taxonomy(index_dir: &Path, viable_versions: &[String]) -> String {
    if !viable_versions.is_empty() {
        if viable_versions
            .iter()
            .all(|v| v.starts_with("gtdb-r") || v.starts_with("g-r"))
        {
            return "gtdb".to_string();
        }
        if viable_versions
            .iter()
            .all(|v| {
                v.starts_with("new_taxdump_")
                    || v.starts_with("taxdump_")
                    || v.starts_with("taxdmp_")
                    || v.starts_with("n-")
                    || v.starts_with("t-")
            })
        {
            return "ncbi".to_string();
        }
    }
    let d = index_dir.to_string_lossy().to_lowercase();
    if d.contains("gtdb") {
        "gtdb".to_string()
    } else if d.contains("ncbi") || d.ends_with("/ncbi_index") || d == ".taxdet/index/ncbi_index" {
        "ncbi".to_string()
    } else {
        "unknown".to_string()
    }
}

fn default_index_dirs() -> Vec<PathBuf> {
    if let Ok(raw) = env::var("TAXON_INDEX_DIRS") {
        let dirs: Vec<PathBuf> = raw
            .split(|c| c == ',' || c == ';' || c == ':')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect();
        if !dirs.is_empty() {
            return dirs;
        }
    }

    let default_root = env::var("TAXDET_HOME")
        .map(PathBuf::from)
        .or_else(|_| env::var("HOME").map(|h| PathBuf::from(h).join(".taxdet")))
        .unwrap_or_else(|_| PathBuf::from(".taxdet"));

    let default_ncbi = default_root.join("index").join("ncbi_index");
    let default_gtdb = default_root.join("index").join("gtdb_index");

    let ncbi = env::var("NCBI_INDEX_DIR")
        .map(PathBuf::from)
        .unwrap_or(default_ncbi);
    let gtdb = env::var("GTDB_INDEX_DIR")
        .map(PathBuf::from)
        .unwrap_or(default_gtdb);
    vec![ncbi, gtdb]
}

fn unique_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for p in paths {
        let k = p.to_string_lossy().to_string();
        if seen.insert(k) {
            out.push(p);
        }
    }
    out
}

fn better_candidate(a: &EvalResult, b: &EvalResult) -> bool {
    if a.failed_queries != b.failed_queries {
        return a.failed_queries < b.failed_queries;
    }
    let a_class = if a.viable_versions.len() == 1 {
        0
    } else if a.viable_versions.is_empty() {
        2
    } else {
        1
    };
    let b_class = if b.viable_versions.len() == 1 {
        0
    } else if b.viable_versions.is_empty() {
        2
    } else {
        1
    };
    if a_class != b_class {
        return a_class < b_class;
    }
    if a.viable_versions.len() != b.viable_versions.len() {
        return a.viable_versions.len() < b.viable_versions.len();
    }
    if a.used_for_intersection != b.used_for_intersection {
        return a.used_for_intersection > b.used_for_intersection;
    }
    false
}

fn inferred_query_taxonomy(inputs: &QueryInputs) -> &'static str {
    let has_gtdb_prefix = |s: &str| {
        ["d__", "p__", "c__", "o__", "f__", "g__", "s__"]
            .iter()
            .any(|p| s.trim_start().starts_with(p))
    };
    if inputs.req_names.iter().any(|n| has_gtdb_prefix(n)) {
        return "gtdb";
    }
    if inputs
        .req_pair_requests
        .iter()
        .any(|r| has_gtdb_prefix(&r.display_name_txt))
    {
        return "gtdb";
    }
    "ncbi"
}

fn evaluate_index(
    index_dir: &Path,
    inputs: &QueryInputs,
    allow_synonym_fallback: bool,
    ignore_failed: bool,
) -> Result<EvalResult> {
    let req_tax_set: HashSet<u32> = inputs.req_tax_ids.iter().copied().collect();
    let mut req_name_set: HashSet<String> = HashSet::new();
    for name in &inputs.req_names {
        for variant in split_name_variants(name) {
            req_name_set.insert(variant);
        }
    }
    let mut req_pair_set: HashSet<(u32, String)> = HashSet::new();
    for req in &inputs.req_pair_requests {
        for (taxid, name) in &req.alternatives {
            req_pair_set.insert((*taxid, name.clone()));
        }
    }

    let files = required_index_files(index_dir);
    for p in &files {
        if !p.exists() {
            anyhow::bail!("Missing input file: {}", p.display());
        }
    }
    let version_cols_path = &files[0];
    let taxid_matrix_path = &files[1];
    let sci_name_matrix_path = &files[2];
    let taxid_sci_matrix_path = &files[3];
    let taxid_any_matrix_path = &files[4];

    let mut version_cols: Vec<(usize, String)> = Vec::new();
    {
        let mut rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .flexible(true)
            .from_path(version_cols_path)?;
        let headers = rdr.headers()?.clone();
        let c_i = headers
            .iter()
            .position(|h| h == "col_idx" || h.starts_with("col_idx-"))
            .context("missing col_idx")?;
        let v_i = headers
            .iter()
            .position(|h| h == "version_id" || h.starts_with("version_id-"))
            .context("missing version_id")?;
        for rec in rdr.records() {
            let rec = rec?;
            let Some(idx_raw) = rec.get(c_i).map(str::trim) else {
                continue;
            };
            let Some(ver_raw) = rec.get(v_i).map(str::trim) else {
                continue;
            };
            if idx_raw.is_empty() || ver_raw.is_empty() {
                continue;
            }
            let Ok(idx) = idx_raw.parse::<usize>() else {
                continue;
            };
            let ver = ver_raw.to_string();
            version_cols.push((idx, ver));
        }
    }
    version_cols.sort_by_key(|(i, _)| *i);
    let version_cols: Vec<String> = version_cols.into_iter().map(|(_, v)| v).collect();
    let chunk_count = version_cols.len().div_ceil(64);

    let mut taxid_bits: HashMap<u32, Vec<u64>> = HashMap::new();
    let mut sci_name_bits: HashMap<String, Vec<u64>> = HashMap::new();
    let mut pair_sci_bits: HashMap<(u32, String), Vec<u64>> = HashMap::new();
    let mut pair_any_bits: HashMap<(u32, String), Vec<u64>> = HashMap::new();
    let mut name_any_bits: HashMap<String, Vec<u64>> = HashMap::new();

    {
        let mut rdr = ReaderBuilder::new().delimiter(b'\t').from_path(taxid_matrix_path)?;
        let h = rdr.headers()?.clone();
        let tax_i = h.iter().position(|x| x == "tax_id").context("taxid_matrix missing tax_id")?;
        let bits_i = h
            .iter()
            .position(|x| x == "bitset_hex")
            .context("taxid_matrix missing bitset_hex")?;
        for rec in rdr.records() {
            let rec = rec?;
            let taxid: u32 = rec.get(tax_i).unwrap_or_default().parse().unwrap_or(0);
            if req_tax_set.contains(&taxid) {
                taxid_bits.insert(
                    taxid,
                    parse_bitset_hex(rec.get(bits_i).unwrap_or_default(), chunk_count),
                );
            }
        }
    }

    {
        let mut rdr = ReaderBuilder::new().delimiter(b'\t').from_path(sci_name_matrix_path)?;
        let h = rdr.headers()?.clone();
        let n_i = h
            .iter()
            .position(|x| x == "scientific_name")
            .context("scientific_name_matrix missing scientific_name")?;
        let bits_i = h
            .iter()
            .position(|x| x == "bitset_hex")
            .context("scientific_name_matrix missing bitset_hex")?;
        for rec in rdr.records() {
            let rec = rec?;
            let name = rec.get(n_i).unwrap_or_default().to_string();
            if req_name_set.contains(&name) {
                sci_name_bits.insert(
                    name,
                    parse_bitset_hex(rec.get(bits_i).unwrap_or_default(), chunk_count),
                );
            }
        }
    }

    {
        let mut rdr = ReaderBuilder::new().delimiter(b'\t').from_path(taxid_sci_matrix_path)?;
        let h = rdr.headers()?.clone();
        let tax_i = h.iter().position(|x| x == "tax_id").context("taxid_sci missing tax_id")?;
        let n_i = h
            .iter()
            .position(|x| x == "scientific_name")
            .context("taxid_sci missing scientific_name")?;
        let bits_i = h
            .iter()
            .position(|x| x == "bitset_hex")
            .context("taxid_sci missing bitset_hex")?;
        for rec in rdr.records() {
            let rec = rec?;
            let taxid: u32 = rec.get(tax_i).unwrap_or_default().parse().unwrap_or(0);
            let name = rec.get(n_i).unwrap_or_default().to_string();
            let key = (taxid, name);
            if req_pair_set.contains(&key) {
                pair_sci_bits.insert(
                    key,
                    parse_bitset_hex(rec.get(bits_i).unwrap_or_default(), chunk_count),
                );
            }
        }
    }

    {
        let mut rdr = ReaderBuilder::new().delimiter(b'\t').from_path(taxid_any_matrix_path)?;
        let h = rdr.headers()?.clone();
        let tax_i = h.iter().position(|x| x == "tax_id").context("taxid_any missing tax_id")?;
        let n_i = h
            .iter()
            .position(|x| x == "name_txt")
            .context("taxid_any missing name_txt")?;
        let bits_i = h
            .iter()
            .position(|x| x == "bitset_hex")
            .context("taxid_any missing bitset_hex")?;
        for rec in rdr.records() {
            let rec = rec?;
            let taxid: u32 = rec.get(tax_i).unwrap_or_default().parse().unwrap_or(0);
            let name = rec.get(n_i).unwrap_or_default().to_string();
            let bits = parse_bitset_hex(rec.get(bits_i).unwrap_or_default(), chunk_count);
            let key = (taxid, name.clone());
            if req_pair_set.contains(&key) {
                pair_any_bits.insert(key, bits.clone());
            }
            if req_name_set.contains(&name) {
                let entry = name_any_bits.entry(name).or_insert_with(|| vec![0_u64; chunk_count]);
                for (e, b) in entry.iter_mut().zip(bits.iter()) {
                    *e |= *b;
                }
            }
        }
    }

    let mut details: Vec<DetailRow> = Vec::new();
    let mut all_bits: Vec<Vec<u64>> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut failed_queries = 0_usize;

    for taxid in &inputs.req_tax_ids {
        let bits = taxid_bits
            .get(taxid)
            .cloned()
            .unwrap_or_else(|| vec![0_u64; chunk_count]);
        let failed = bitset_is_empty(&bits);
        if failed {
            failed_queries += 1;
        }
        if !failed || !ignore_failed {
            all_bits.push(bits.clone());
        }
        details.push(DetailRow {
            query_type: "tax_id".to_string(),
            tax_id: taxid.to_string(),
            name_txt: String::new(),
            match_source: "tax_id".to_string(),
            bitset: bits,
        });
    }

    for req in &inputs.req_pair_requests {
        let mut sci = vec![0_u64; chunk_count];
        let mut any = vec![0_u64; chunk_count];
        for (taxid, name_txt) in &req.alternatives {
            let key = (*taxid, name_txt.clone());
            if let Some(bits) = pair_sci_bits.get(&key) {
                or_assign(&mut sci, bits);
            }
            if let Some(bits) = pair_any_bits.get(&key) {
                or_assign(&mut any, bits);
            }
        }

        let (bits, match_source) = if !bitset_is_empty(&sci) {
            if req.alternatives.len() > 1 {
                (sci, "scientific_name_alt".to_string())
            } else {
                (sci, "scientific_name".to_string())
            }
        } else if allow_synonym_fallback && !bitset_is_empty(&any) {
            if req.alternatives.len() > 1 {
                (any, "synonym_fallback_alt".to_string())
            } else {
                (any, "synonym_fallback".to_string())
            }
        } else if !bitset_is_empty(&any) {
            warnings.push(format!(
                "(tax_id={}, name_txt={:?}) matched only non-scientific names; rerun with --allow-synonym-fallback to include these versions.",
                req.display_tax_id, req.display_name_txt
            ));
            (vec![0_u64; chunk_count], "none_synonym_only".to_string())
        } else {
            warnings.push(format!(
                "(tax_id={}, name_txt={:?}) had no matches in any name class.",
                req.display_tax_id, req.display_name_txt
            ));
            (vec![0_u64; chunk_count], "none".to_string())
        };

        let failed = bitset_is_empty(&bits);
        if failed {
            failed_queries += 1;
        }
        if !failed || !ignore_failed {
            all_bits.push(bits.clone());
        }
        details.push(DetailRow {
            query_type: "tax_id_name".to_string(),
            tax_id: req.display_tax_id.clone(),
            name_txt: req.display_name_txt.clone(),
            match_source,
            bitset: bits,
        });
    }

    for name_txt in &inputs.req_names {
        let variants = split_name_variants(name_txt);
        let mut sci = vec![0_u64; chunk_count];
        let mut any = vec![0_u64; chunk_count];
        for variant in &variants {
            if let Some(bits) = sci_name_bits.get(variant) {
                or_assign(&mut sci, bits);
            }
            if let Some(bits) = name_any_bits.get(variant) {
                or_assign(&mut any, bits);
            }
        }

        let (bits, match_source) = if !bitset_is_empty(&sci) {
            if variants.len() > 1 {
                (sci, "scientific_name_alt".to_string())
            } else {
                (sci, "scientific_name".to_string())
            }
        } else if allow_synonym_fallback && !bitset_is_empty(&any) {
            if variants.len() > 1 {
                (any, "synonym_fallback_alt".to_string())
            } else {
                (any, "synonym_fallback".to_string())
            }
        } else if !bitset_is_empty(&any) {
            warnings.push(format!(
                "(name_txt={:?}) matched only non-scientific names; rerun with --allow-synonym-fallback to include these versions.",
                name_txt
            ));
            (vec![0_u64; chunk_count], "none_synonym_only".to_string())
        } else {
            warnings.push(format!("(name_txt={:?}) had no matches in any name class.", name_txt));
            (vec![0_u64; chunk_count], "none".to_string())
        };

        let failed = bitset_is_empty(&bits);
        if failed {
            failed_queries += 1;
        }
        if !failed || !ignore_failed {
            all_bits.push(bits.clone());
        }
        details.push(DetailRow {
            query_type: "name_txt".to_string(),
            tax_id: String::new(),
            name_txt: name_txt.clone(),
            match_source,
            bitset: bits,
        });
    }

    let used_for_intersection = all_bits.len();
    let viable_bits = if all_bits.is_empty() {
        vec![0_u64; chunk_count]
    } else {
        let mut it = all_bits.into_iter();
        let mut acc = it.next().unwrap_or_else(|| vec![0_u64; chunk_count]);
        for bits in it {
            and_assign(&mut acc, &bits);
        }
        acc
    };
    let viable_versions = bitset_to_versions(&viable_bits, &version_cols);
    let taxonomy = detect_taxonomy(index_dir, &viable_versions);

    Ok(EvalResult {
        index_dir: index_dir.to_path_buf(),
        taxonomy,
        version_cols,
        details,
        viable_versions,
        failed_queries,
        used_for_intersection,
        warnings,
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.tax_ids.is_none() && args.names.is_none() && args.tax_name_pairs.is_none() {
        anyhow::bail!("Provide --tax-ids and/or --names and/or --tax-name-pairs");
    }

    let (req_pair_requests, mut parse_warnings) = if let Some(p) = &args.tax_name_pairs {
        read_tax_name_pairs(p)?
    } else {
        (Vec::new(), Vec::new())
    };

    let inputs = QueryInputs {
        req_tax_ids: if let Some(p) = &args.tax_ids {
            read_tax_ids(p)?
        } else {
            Vec::new()
        },
        req_names: if let Some(p) = &args.names {
            expand_name_queries(read_names(p)?)
        } else {
            Vec::new()
        },
        req_pair_requests,
    };

    let mut global_warnings: Vec<String> = Vec::new();
    global_warnings.append(&mut parse_warnings);

    let candidate_dirs = if args.index_dirs.is_empty() {
        let mut dirs = Vec::new();
        for d in unique_paths(default_index_dirs()) {
            if has_required_index(&d) {
                dirs.push(d);
            } else {
                global_warnings.push(format!(
                    "skipping default index dir without required files: {}",
                    d.display()
                ));
            }
        }
        dirs
    } else {
        unique_paths(args.index_dirs.clone())
    };

    if candidate_dirs.is_empty() {
        anyhow::bail!(
            "No valid index directories found. Set --index-dir, or set TAXON_INDEX_DIRS/NCBI_INDEX_DIR/GTDB_INDEX_DIR."
        );
    }

    let mut evaluations: Vec<EvalResult> = Vec::new();
    for d in candidate_dirs {
        if !has_required_index(&d) {
            anyhow::bail!("Index dir missing required files: {}", d.display());
        }
        evaluations.push(evaluate_index(
            &d,
            &inputs,
            args.allow_synonym_fallback,
            args.ignore_failed,
        )?);
    }

    let preferred_taxonomy = inferred_query_taxonomy(&inputs);
    let mut best_idx = 0usize;
    for i in 1..evaluations.len() {
        if better_candidate(&evaluations[i], &evaluations[best_idx])
            || (!better_candidate(&evaluations[best_idx], &evaluations[i])
                && evaluations[i].taxonomy == preferred_taxonomy
                && evaluations[best_idx].taxonomy != preferred_taxonomy)
        {
            best_idx = i;
        }
    }
    let selected = &evaluations[best_idx];

    let prefix = args.output_prefix.to_string_lossy().to_string();
    let details_path = PathBuf::from(format!("{prefix}.details.log"));
    let warnings_path = PathBuf::from(format!("{prefix}.warnings.log"));
    let result_path = PathBuf::from(format!("{prefix}.result.log"));
    if let Some(parent) = details_path.parent() {
        fs::create_dir_all(parent)?;
    }

    {
        let mut w = WriterBuilder::new().delimiter(b'\t').from_path(&details_path)?;
        w.write_record([
            "query_type",
            "tax_id",
            "name_txt",
            "match_source",
            "version_count",
            "versions_csv",
        ])?;
        for d in &selected.details {
            let versions = bitset_to_versions(&d.bitset, &selected.version_cols);
            w.write_record([
                d.query_type.as_str(),
                d.tax_id.as_str(),
                d.name_txt.as_str(),
                d.match_source.as_str(),
                &versions.len().to_string(),
                &versions.join(","),
            ])?;
        }
        w.flush()?;
    }

    {
        let mut w = WriterBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .from_path(&warnings_path)?;
        for wmsg in global_warnings.iter().chain(selected.warnings.iter()) {
            w.write_record([wmsg])?;
        }
        w.flush()?;
    }

    {
        let mut result_txt = String::new();
        result_txt.push_str(&format!("Detected taxonomy: {}\n", selected.taxonomy));
        result_txt.push_str(&format!("Selected index: {}\n", selected.index_dir.display()));
        result_txt.push_str(&format!(
            "Queries: total={} failed={} used_for_intersection={}\n",
            selected.details.len(),
            selected.failed_queries,
            selected.used_for_intersection
        ));
        if selected.viable_versions.len() == 1 {
            result_txt.push_str(&format!(
                "Single plausible version: {}\n",
                selected.viable_versions[0]
            ));
        } else if selected.viable_versions.is_empty() {
            result_txt.push_str("No plausible version found from intersection.\n");
        } else {
            result_txt.push_str(&format!(
                "Multiple plausible versions ({}) from intersection.\n",
                selected.viable_versions.len()
            ));
        }
        result_txt.push_str(&format!(
            "Warnings: {}\n",
            global_warnings.len() + selected.warnings.len()
        ));
        result_txt.push_str(&format!(
            "Details log: {}\n",
            details_path.to_string_lossy()
        ));
        result_txt.push_str(&format!(
            "Warnings log: {}\n",
            warnings_path.to_string_lossy()
        ));
        result_txt.push_str("Viable versions:\n");
        for v in &selected.viable_versions {
            result_txt.push_str(v);
            result_txt.push('\n');
        }
        fs::write(&result_path, result_txt)?;
    }
    let stdout_result = fs::read_to_string(&result_path)?;
    print!("{stdout_result}");

    Ok(())
}
