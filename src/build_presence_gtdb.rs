use anyhow::{Context, Result};
use clap::Parser;
use glob::glob;
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(about = "Build compact GTDB taxonomy version indexes (Rust)")]
struct Args {
    #[arg(long, default_value = "data/gtdb")]
    taxonomy_dir: PathBuf,

    #[arg(long, default_value = "gtdb_r*_*.tsv*")]
    input_glob: String,

    #[arg(long)]
    index_dir: Option<PathBuf>,

    #[arg(long, default_value = "data/manifests/gtdb_ingestion_manifest.tsv")]
    manifest_out: PathBuf,

    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct ReleaseResult {
    version_id: String,
    status: String,
    file_count: usize,
    taxon_rows: u64,
    name_rows: u64,
    process_seconds: f64,
    error: String,
}

fn parse_release_from_filename(name: &str) -> Option<u32> {
    let bytes = name.as_bytes();
    let needle = b"gtdb_r";
    let mut i = 0;
    while i + needle.len() < bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let mut j = i + needle.len();
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + needle.len() {
                let s = &name[i + needle.len()..j];
                if let Ok(v) = s.parse::<u32>() {
                    return Some(v);
                }
            }
        }
        i += 1;
    }
    None
}

fn open_line_reader(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    if path
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        let gz = flate2::read::GzDecoder::new(file);
        Ok(Box::new(BufReader::new(gz)))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn intern_name(pool: &mut HashSet<Arc<str>>, raw: &str) -> Arc<str> {
    if let Some(existing) = pool.get(raw) {
        return existing.clone();
    }
    let arc: Arc<str> = Arc::from(raw);
    pool.insert(arc.clone());
    arc
}

fn push_release(vec: &mut Vec<u32>, rel: u32) {
    if vec.last().copied() != Some(rel) {
        vec.push(rel);
    }
}

fn parse_rank(token: &str) -> &'static str {
    if let Some((prefix, _)) = token.split_once("__") {
        match prefix {
            "d" => "domain",
            "p" => "phylum",
            "c" => "class",
            "o" => "order",
            "f" => "family",
            "g" => "genus",
            "s" => "species",
            _ => "",
        }
    } else {
        ""
    }
}

fn name_forms(token: &str) -> Vec<String> {
    let t = token.trim();
    if t.is_empty() {
        return Vec::new();
    }
    let mut out = vec![t.to_string()];
    if let Some((_prefix, rest)) = t.split_once("__") {
        let bare = rest.trim();
        if !bare.is_empty() {
            out.push(bare.to_string());
        }
    }
    out
}

fn collect_inputs(args: &Args) -> Result<Vec<PathBuf>> {
    if !args.taxonomy_dir.exists() {
        anyhow::bail!("taxonomy directory does not exist: {}", args.taxonomy_dir.display());
    }
    let pattern = args.taxonomy_dir.join(&args.input_glob);
    let pattern_s = pattern.to_string_lossy().to_string();

    let mut files = Vec::new();
    for entry in glob(&pattern_s).with_context(|| format!("invalid glob pattern: {pattern_s}"))? {
        let path = entry?;
        if path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    if files.is_empty() {
        anyhow::bail!("no GTDB taxonomy files found");
    }
    Ok(files)
}

fn encode_bitset_hex(
    versions: &[u32],
    col_index: &HashMap<u32, usize>,
    chunk_count: usize,
) -> String {
    let mut chunks = vec![0_u64; chunk_count];
    for rel in versions {
        if let Some(&idx) = col_index.get(rel) {
            chunks[idx / 64] |= 1_u64 << (idx % 64);
        }
    }
    chunks
        .iter()
        .map(|x| format!("{x:016x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn write_manifest(path: &Path, rows: &[ReleaseResult]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut out = csv::WriterBuilder::new().delimiter(b'\t').from_path(path)?;
    out.write_record([
        "version_id",
        "status",
        "file_count",
        "taxon_rows",
        "name_rows",
        "process_seconds",
        "error",
    ])?;
    for r in rows {
        out.write_record([
            r.version_id.as_str(),
            r.status.as_str(),
            &r.file_count.to_string(),
            &r.taxon_rows.to_string(),
            &r.name_rows.to_string(),
            &format!("{:.3}", r.process_seconds),
            r.error.as_str(),
        ])?;
    }
    out.flush()?;
    Ok(())
}

fn write_version_columns(path: &Path, cols: &[u32]) -> Result<()> {
    let mut out = csv::WriterBuilder::new().delimiter(b'\t').from_path(path)?;
    out.write_record(["col_idx", "version_id"])?;
    for (idx, rel) in cols.iter().enumerate() {
        out.write_record([idx.to_string(), format!("gtdb-r{rel}")])?;
    }
    out.flush()?;
    Ok(())
}

fn write_taxid_matrix(
    path: &Path,
    map: &HashMap<u32, Vec<u32>>,
    taxid_rank: &HashMap<u32, String>,
    col_index: &HashMap<u32, usize>,
    chunk_count: usize,
) -> Result<()> {
    let mut items: Vec<_> = map.iter().collect();
    items.sort_by_key(|(k, _)| **k);
    let mut out = csv::WriterBuilder::new().delimiter(b'\t').from_path(path)?;
    out.write_record(["tax_id", "rank", "bitset_hex"])?;
    for (tax_id, versions) in items {
        let rank = taxid_rank.get(tax_id).cloned().unwrap_or_default();
        out.write_record([
            tax_id.to_string(),
            rank,
            encode_bitset_hex(versions, col_index, chunk_count),
        ])?;
    }
    out.flush()?;
    Ok(())
}

fn write_name_matrix(
    path: &Path,
    map: &HashMap<Arc<str>, Vec<u32>>,
    name_col: &str,
    col_index: &HashMap<u32, usize>,
    chunk_count: usize,
) -> Result<()> {
    let mut items: Vec<_> = map.iter().collect();
    items.sort_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));
    let mut out = csv::WriterBuilder::new().delimiter(b'\t').from_path(path)?;
    out.write_record([name_col, "bitset_hex"])?;
    for (name, versions) in items {
        out.write_record([
            name.to_string(),
            encode_bitset_hex(versions, col_index, chunk_count),
        ])?;
    }
    out.flush()?;
    Ok(())
}

fn write_pair_matrix(
    path: &Path,
    map: &HashMap<(u32, Arc<str>), Vec<u32>>,
    second_col: &str,
    col_index: &HashMap<u32, usize>,
    chunk_count: usize,
) -> Result<()> {
    let mut items: Vec<_> = map.iter().collect();
    items.sort_by(|((ta, na), _), ((tb, nb), _)| ta.cmp(tb).then_with(|| na.as_ref().cmp(nb.as_ref())));
    let mut out = csv::WriterBuilder::new().delimiter(b'\t').from_path(path)?;
    out.write_record(["tax_id", second_col, "bitset_hex"])?;
    for ((tax_id, name), versions) in items {
        out.write_record([
            tax_id.to_string(),
            name.to_string(),
            encode_bitset_hex(versions, col_index, chunk_count),
        ])?;
    }
    out.flush()?;
    Ok(())
}

fn default_taxdet_home() -> PathBuf {
    env::var("TAXDET_HOME")
        .map(PathBuf::from)
        .or_else(|_| env::var("HOME").map(|h| PathBuf::from(h).join(".taxdet")))
        .unwrap_or_else(|_| PathBuf::from(".taxdet"))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let index_dir = args
        .index_dir
        .clone()
        .unwrap_or_else(|| default_taxdet_home().join("index").join("gtdb_index"));
    fs::create_dir_all(&index_dir)?;

    let mut files = collect_inputs(&args)?;
    let mut by_release: HashMap<u32, Vec<PathBuf>> = HashMap::new();
    for path in files.drain(..) {
        let name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
        if let Some(rel) = parse_release_from_filename(name) {
            by_release.entry(rel).or_default().push(path);
        }
    }

    let mut releases: Vec<u32> = by_release.keys().copied().collect();
    releases.sort_unstable();
    if let Some(limit) = args.limit {
        releases.truncate(limit);
    }
    if releases.is_empty() {
        anyhow::bail!("no parseable GTDB release files found");
    }

    let mut name_pool: HashSet<Arc<str>> = HashSet::new();
    let mut taxon_to_id: HashMap<Arc<str>, u32> = HashMap::new();
    let mut next_taxid: u32 = 1;

    let mut sci_name_versions: HashMap<Arc<str>, Vec<u32>> = HashMap::new();
    let mut taxid_versions: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut taxid_rank: HashMap<u32, String> = HashMap::new();
    let mut taxid_sci_versions: HashMap<(u32, Arc<str>), Vec<u32>> = HashMap::new();
    let mut taxid_any_versions: HashMap<(u32, Arc<str>), Vec<u32>> = HashMap::new();

    let mut manifest_rows = Vec::new();

    for (idx, rel) in releases.iter().enumerate() {
        let version_id = format!("gtdb-r{rel}");
        let mut row = ReleaseResult {
            version_id: version_id.clone(),
            status: "ok".to_string(),
            file_count: 0,
            taxon_rows: 0,
            name_rows: 0,
            process_seconds: 0.0,
            error: String::new(),
        };

        let start = Instant::now();
        let mut seen_taxid: HashSet<u32> = HashSet::new();
        let mut seen_sci_name: HashSet<Arc<str>> = HashSet::new();
        let mut seen_taxid_sci: HashSet<(u32, Arc<str>)> = HashSet::new();
        let mut seen_taxid_any: HashSet<(u32, Arc<str>)> = HashSet::new();

        let mut rel_files = by_release.remove(rel).unwrap_or_default();
        rel_files.sort();

        for path in &rel_files {
            row.file_count += 1;
            let mut rdr = match open_line_reader(path) {
                Ok(v) => v,
                Err(err) => {
                    row.status = "error".to_string();
                    row.error = format!("{}: {err:#}", path.display());
                    continue;
                }
            };

            let mut line = String::new();
            loop {
                line.clear();
                if rdr.read_line(&mut line)? == 0 {
                    break;
                }
                let t = line.trim();
                if t.is_empty() {
                    continue;
                }
                let mut parts = t.splitn(2, '\t');
                let _genome = parts.next().unwrap_or_default();
                let lineage = parts.next().unwrap_or_default();
                if lineage.is_empty() {
                    continue;
                }

                for token in lineage.split(';').map(str::trim).filter(|x| !x.is_empty()) {
                    let canonical = intern_name(&mut name_pool, token);
                    let taxid = if let Some(existing) = taxon_to_id.get(&canonical) {
                        *existing
                    } else {
                        let id = next_taxid;
                        next_taxid = next_taxid.saturating_add(1);
                        taxon_to_id.insert(canonical.clone(), id);
                        id
                    };

                    seen_taxid.insert(taxid);
                    taxid_rank
                        .entry(taxid)
                        .or_insert_with(|| parse_rank(token).to_string());

                    for n in name_forms(token) {
                        let n_arc = intern_name(&mut name_pool, &n);
                        seen_sci_name.insert(n_arc.clone());
                        seen_taxid_sci.insert((taxid, n_arc.clone()));
                        seen_taxid_any.insert((taxid, n_arc));
                        row.name_rows += 1;
                    }
                }
            }
        }

        if row.status == "ok" {
            row.taxon_rows = seen_taxid.len() as u64;
            for taxid in seen_taxid {
                push_release(taxid_versions.entry(taxid).or_default(), *rel);
            }
            for name in seen_sci_name {
                push_release(sci_name_versions.entry(name).or_default(), *rel);
            }
            for key in seen_taxid_sci {
                push_release(taxid_sci_versions.entry(key).or_default(), *rel);
            }
            for key in seen_taxid_any {
                push_release(taxid_any_versions.entry(key).or_default(), *rel);
            }
        }

        row.process_seconds = start.elapsed().as_secs_f64();
        eprintln!(
            "[{}/{}] {:>8}  {}  files={}  taxa={}  names={}  total={:.2}s{}",
            idx + 1,
            releases.len(),
            row.status,
            row.version_id,
            row.file_count,
            row.taxon_rows,
            row.name_rows,
            row.process_seconds,
            if row.error.is_empty() {
                String::new()
            } else {
                format!("  err={}", row.error.lines().next().unwrap_or_default())
            }
        );
        manifest_rows.push(row);
    }

    write_manifest(&args.manifest_out, &manifest_rows)?;

    let mut version_cols: Vec<u32> = manifest_rows
        .iter()
        .filter(|r| r.status == "ok")
        .filter_map(|r| r.version_id.strip_prefix("gtdb-r"))
        .filter_map(|s| s.parse::<u32>().ok())
        .collect();
    version_cols.sort_unstable();
    version_cols.dedup();

    let col_index: HashMap<u32, usize> = version_cols
        .iter()
        .enumerate()
        .map(|(i, r)| (*r, i))
        .collect();
    let chunk_count = version_cols.len().div_ceil(64);

    let version_cols_path = index_dir.join("version_columns.tsv");
    let taxid_matrix_path = index_dir.join("taxid_matrix.tsv");
    let sci_name_matrix_path = index_dir.join("scientific_name_matrix.tsv");
    let taxid_sci_matrix_path = index_dir.join("taxid_scientific_name_matrix.tsv");
    let taxid_any_matrix_path = index_dir.join("taxid_any_name_matrix.tsv");

    write_version_columns(&version_cols_path, &version_cols)?;
    write_taxid_matrix(&taxid_matrix_path, &taxid_versions, &taxid_rank, &col_index, chunk_count)?;
    write_name_matrix(
        &sci_name_matrix_path,
        &sci_name_versions,
        "scientific_name",
        &col_index,
        chunk_count,
    )?;
    write_pair_matrix(
        &taxid_sci_matrix_path,
        &taxid_sci_versions,
        "scientific_name",
        &col_index,
        chunk_count,
    )?;
    write_pair_matrix(
        &taxid_any_matrix_path,
        &taxid_any_versions,
        "name_txt",
        &col_index,
        chunk_count,
    )?;

    let ok = manifest_rows.iter().filter(|r| r.status == "ok").count();
    println!("Wrote manifest: {}", args.manifest_out.display());
    println!("Wrote: {}", version_cols_path.display());
    println!("Wrote: {}", taxid_matrix_path.display());
    println!("Wrote: {}", sci_name_matrix_path.display());
    println!("Wrote: {}", taxid_sci_matrix_path.display());
    println!("Wrote: {}", taxid_any_matrix_path.display());
    println!("GTDB releases processed: {}, successful: {}", manifest_rows.len(), ok);

    Ok(())
}
