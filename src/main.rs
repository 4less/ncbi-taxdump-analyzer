use anyhow::{Context, Result};
use clap::Parser;
use glob::glob;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use zip::result::ZipError;
use zip::ZipArchive;

#[derive(clap::ValueEnum, Clone, Debug)]
enum TaxonScope {
    All,
    Bacteria,
}

#[derive(Parser, Debug)]
#[command(about = "Build compact taxon/name version indexes (Rust)")]
struct Args {
    #[arg(long, default_value = "data/archives")]
    archives_dir: PathBuf,

    #[arg(long, default_value = "*_*.zip")]
    archives_glob: String,

    #[arg(long, default_value = "data/index")]
    index_dir: PathBuf,

    #[arg(long, default_value = "data/manifests/ingestion_manifest.tsv")]
    manifest_out: PathBuf,

    #[arg(long, value_enum, default_value = "all")]
    taxon_scope: TaxonScope,

    #[arg(long = "root-taxid")]
    root_taxids: Vec<u32>,

    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct ArchiveResult {
    version_id: String,
    filename: String,
    dump_date: String,
    status: String,
    taxon_rows: u64,
    name_rows: u64,
    load_seconds: f64,
    process_seconds: f64,
    total_seconds: f64,
    error: String,
}

fn parse_dump_date(version_id: &str) -> Option<String> {
    let maybe = version_id.rsplit_once('_')?.1;
    if maybe.len() == 10 && &maybe[4..5] == "-" && &maybe[7..8] == "-" {
        Some(maybe.to_string())
    } else {
        None
    }
}

fn parse_dmp_line(raw: &str) -> Vec<&str> {
    let mut stripped = raw.trim_end_matches('\n');
    if stripped.ends_with("\t|") {
        stripped = &stripped[..stripped.len() - 2];
    }
    stripped.split("\t|\t").map(str::trim).collect()
}

fn parse_nodes_tax_parent(raw: &str) -> Option<(u32, u32)> {
    let parts = parse_dmp_line(raw);
    if parts.len() < 2 {
        return None;
    }
    let tax_id = parts[0].parse::<u32>().ok()?;
    let parent_tax_id = parts[1].parse::<u32>().ok()?;
    Some((tax_id, parent_tax_id))
}

fn parse_nodes_tax_parent_rank(raw: &str) -> Option<(u32, u32, String)> {
    let parts = parse_dmp_line(raw);
    if parts.len() < 3 {
        return None;
    }
    let tax_id = parts[0].parse::<u32>().ok()?;
    let parent_tax_id = parts[1].parse::<u32>().ok()?;
    let rank = parts[2].to_string();
    Some((tax_id, parent_tax_id, rank))
}

fn read_zip_entry_bytes(zip_path: &Path, entry_name: &str) -> std::result::Result<Vec<u8>, ZipError> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let mut entry = archive.by_name(entry_name)?;
    let mut buf = Vec::with_capacity(entry.size() as usize);
    entry.read_to_end(&mut buf)?;
    Ok(buf)
}

fn push_version(vec: &mut Vec<String>, version_id: &str) {
    if vec.last().is_some_and(|v| v == version_id) {
        return;
    }
    vec.push(version_id.to_string());
}

fn intern_name(pool: &mut HashSet<Arc<str>>, raw: &str) -> Arc<str> {
    if let Some(existing) = pool.get(raw) {
        return existing.clone();
    }
    let arc: Arc<str> = Arc::from(raw);
    pool.insert(arc.clone());
    arc
}

fn is_descendant_of_roots(
    start: u32,
    roots: &HashSet<u32>,
    parent_of: &HashMap<u32, u32>,
    memo: &mut HashMap<u32, bool>,
) -> bool {
    if roots.contains(&start) {
        memo.insert(start, true);
        return true;
    }
    if let Some(&v) = memo.get(&start) {
        return v;
    }

    let mut path: Vec<u32> = Vec::new();
    let mut cur = start;

    loop {
        if roots.contains(&cur) {
            for t in path {
                memo.insert(t, true);
            }
            memo.insert(cur, true);
            return true;
        }
        if let Some(&known) = memo.get(&cur) {
            for t in path {
                memo.insert(t, known);
            }
            return known;
        }
        path.push(cur);
        let Some(&parent) = parent_of.get(&cur) else {
            for t in path {
                memo.insert(t, false);
            }
            return false;
        };
        if parent == cur {
            for t in path {
                memo.insert(t, false);
            }
            return false;
        }
        cur = parent;
    }
}

fn build_allowed_taxa(nodes_bytes: &[u8], roots: &HashSet<u32>) -> HashSet<u32> {
    let mut parent_of: HashMap<u32, u32> = HashMap::new();
    for line in nodes_bytes.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let s = String::from_utf8_lossy(line);
        if let Some((tax_id, parent_tax_id)) = parse_nodes_tax_parent(&s) {
            parent_of.insert(tax_id, parent_tax_id);
        }
    }

    if roots.is_empty() {
        return parent_of.keys().copied().collect();
    }

    let mut allowed = HashSet::new();
    let mut memo: HashMap<u32, bool> = HashMap::new();
    for tax_id in parent_of.keys().copied() {
        if is_descendant_of_roots(tax_id, roots, &parent_of, &mut memo) {
            allowed.insert(tax_id);
        }
    }
    allowed
}

fn collect_archives(args: &Args) -> Result<Vec<PathBuf>> {
    if !args.archives_dir.exists() {
        anyhow::bail!("Archive directory does not exist: {}", args.archives_dir.display());
    }

    let pattern = args.archives_dir.join(&args.archives_glob);
    let pattern_s = pattern.to_string_lossy().to_string();

    let mut archives = Vec::new();
    for entry in glob(&pattern_s).with_context(|| format!("invalid glob pattern: {pattern_s}"))? {
        let path = entry?;
        if path.is_file() {
            archives.push(path);
        }
    }

    archives.sort();
    if let Some(limit) = args.limit {
        archives.truncate(limit);
    }

    if archives.is_empty() {
        anyhow::bail!("No archives found.");
    }

    Ok(archives)
}

fn write_manifest(path: &Path, rows: &[ArchiveResult]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut wtr = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .from_path(path)
        .with_context(|| format!("create {}", path.display()))?;

    wtr.write_record([
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
    ])?;

    for r in rows {
        wtr.write_record([
            r.version_id.as_str(),
            r.filename.as_str(),
            r.dump_date.as_str(),
            r.status.as_str(),
            &r.taxon_rows.to_string(),
            &r.name_rows.to_string(),
            &format!("{:.3}", r.load_seconds),
            &format!("{:.3}", r.process_seconds),
            &format!("{:.3}", r.total_seconds),
            r.error.as_str(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

fn encode_bitset_hex(
    versions: &[String],
    col_index: &HashMap<String, usize>,
    chunk_count: usize,
) -> String {
    let mut chunks = vec![0_u64; chunk_count];
    for version_id in versions {
        if let Some(&idx) = col_index.get(version_id) {
            chunks[idx / 64] |= 1_u64 << (idx % 64);
        }
    }
    chunks
        .iter()
        .map(|x| format!("{x:016x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn write_version_columns(path: &Path, cols: &[String]) -> Result<()> {
    let mut out = csv::WriterBuilder::new().delimiter(b'\t').from_path(path)?;
    out.write_record(["col_idx", "version_id"])?;
    for (idx, version_id) in cols.iter().enumerate() {
        out.write_record([idx.to_string(), version_id.to_string()])?;
    }
    out.flush()?;
    Ok(())
}

fn write_taxid_matrix(
    path: &Path,
    map: &HashMap<u32, Vec<String>>,
    taxid_rank: &HashMap<u32, String>,
    col_index: &HashMap<String, usize>,
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
    map: &HashMap<Arc<str>, Vec<String>>,
    name_col: &str,
    col_index: &HashMap<String, usize>,
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
    map: &HashMap<(u32, Arc<str>), Vec<String>>,
    second_col: &str,
    col_index: &HashMap<String, usize>,
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

fn main() -> Result<()> {
    let args = Args::parse();
    let archives = collect_archives(&args)?;
    let mut roots: HashSet<u32> = args.root_taxids.iter().copied().collect();
    if matches!(args.taxon_scope, TaxonScope::Bacteria) {
        roots.insert(2);
    }

    fs::create_dir_all(&args.index_dir)
        .with_context(|| format!("create {}", args.index_dir.display()))?;

    let mut name_pool: HashSet<Arc<str>> = HashSet::new();

    // Requested maps:
    // a) scientific name -> versions
    let mut sci_name_versions: HashMap<Arc<str>, Vec<String>> = HashMap::new();
    // b) taxid -> versions
    let mut taxid_versions: HashMap<u32, Vec<String>> = HashMap::new();
    let mut taxid_rank: HashMap<u32, String> = HashMap::new();
    // c) (taxid, scientific name) -> versions
    let mut taxid_sci_versions: HashMap<(u32, Arc<str>), Vec<String>> = HashMap::new();
    // d) (taxid, any name) -> versions
    let mut taxid_any_versions: HashMap<(u32, Arc<str>), Vec<String>> = HashMap::new();

    let mut rows: Vec<ArchiveResult> = Vec::with_capacity(archives.len());

    for (i, archive_path) in archives.iter().enumerate() {
        let version_id = archive_path
            .file_stem()
            .and_then(OsStr::to_str)
            .unwrap_or_default()
            .to_string();
        let filename = archive_path
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or_default()
            .to_string();

        let mut row = ArchiveResult {
            version_id: version_id.clone(),
            filename: filename.clone(),
            dump_date: parse_dump_date(&version_id).unwrap_or_default(),
            status: "ok".to_string(),
            taxon_rows: 0,
            name_rows: 0,
            load_seconds: 0.0,
            process_seconds: 0.0,
            total_seconds: 0.0,
            error: String::new(),
        };

        let total_start = Instant::now();
        let load_start = Instant::now();
        let load_result = (|| -> Result<(Vec<u8>, Vec<u8>)> {
            let nodes = read_zip_entry_bytes(archive_path, "nodes.dmp")
                .with_context(|| format!("{}: nodes.dmp", archive_path.display()))?;
            let names = read_zip_entry_bytes(archive_path, "names.dmp")
                .with_context(|| format!("{}: names.dmp", archive_path.display()))?;
            Ok((nodes, names))
        })();
        row.load_seconds = load_start.elapsed().as_secs_f64();

        match load_result {
            Err(err) => {
                row.status = match err.downcast_ref::<ZipError>() {
                    Some(ZipError::FileNotFound) => "missing_file".to_string(),
                    Some(ZipError::InvalidArchive(_)) | Some(ZipError::UnsupportedArchive(_)) => {
                        "bad_zip".to_string()
                    }
                    _ => "error".to_string(),
                };
                row.error = format!("{err:#}");
                row.total_seconds = total_start.elapsed().as_secs_f64();
                rows.push(row.clone());
                eprintln!(
                    "[{}/{}] {:>12}  {}  taxa=0  names=0  load={:.2}s  process=0.00s  total={:.2}s  err={}",
                    i + 1,
                    archives.len(),
                    row.status,
                    row.filename,
                    row.load_seconds,
                    row.total_seconds,
                    row.error.lines().next().unwrap_or_default(),
                );
                continue;
            }
            Ok((nodes_bytes, names_bytes)) => {
                let process_start = Instant::now();
                let allowed_taxa = build_allowed_taxa(&nodes_bytes, &roots);

                let mut seen_taxid: HashSet<u32> = HashSet::new();
                let mut seen_taxid_rank: HashMap<u32, String> = HashMap::new();
                let mut seen_sci_name: HashSet<Arc<str>> = HashSet::new();
                let mut seen_taxid_sci: HashSet<(u32, Arc<str>)> = HashSet::new();
                let mut seen_taxid_any: HashSet<(u32, Arc<str>)> = HashSet::new();

                for line in nodes_bytes.split(|b| *b == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    let s = String::from_utf8_lossy(line);
                    if let Some((taxid, _parent_taxid, rank)) = parse_nodes_tax_parent_rank(&s) {
                        if !allowed_taxa.contains(&taxid) {
                            continue;
                        }
                        seen_taxid.insert(taxid);
                        seen_taxid_rank.entry(taxid).or_insert(rank);
                        row.taxon_rows += 1;
                    }
                }

                for line in names_bytes.split(|b| *b == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    let s = String::from_utf8_lossy(line);
                    let parts = parse_dmp_line(&s);
                    if parts.len() < 4 {
                        continue;
                    }
                    let taxid = match parts[0].parse::<u32>() {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    if !allowed_taxa.contains(&taxid) {
                        continue;
                    }
                    let name_txt = intern_name(&mut name_pool, parts[1]);
                    let name_class = parts[3];

                    seen_taxid_any.insert((taxid, name_txt.clone()));
                    if name_class == "scientific name" {
                        seen_sci_name.insert(name_txt.clone());
                        seen_taxid_sci.insert((taxid, name_txt));
                    }
                    row.name_rows += 1;
                }

                for taxid in seen_taxid {
                    push_version(taxid_versions.entry(taxid).or_default(), &version_id);
                }
                for (taxid, rank) in seen_taxid_rank {
                    taxid_rank.entry(taxid).or_insert(rank);
                }
                for name in seen_sci_name {
                    push_version(sci_name_versions.entry(name).or_default(), &version_id);
                }
                for key in seen_taxid_sci {
                    push_version(taxid_sci_versions.entry(key).or_default(), &version_id);
                }
                for key in seen_taxid_any {
                    push_version(taxid_any_versions.entry(key).or_default(), &version_id);
                }

                row.process_seconds = process_start.elapsed().as_secs_f64();
                row.total_seconds = total_start.elapsed().as_secs_f64();

                rows.push(row.clone());
                eprintln!(
                    "[{}/{}] {:>12}  {}  taxa={}  names={}  load={:.2}s  process={:.2}s  total={:.2}s",
                    i + 1,
                    archives.len(),
                    row.status,
                    row.filename,
                    row.taxon_rows,
                    row.name_rows,
                    row.load_seconds,
                    row.process_seconds,
                    row.total_seconds,
                );
            }
        }
    }

    write_manifest(&args.manifest_out, &rows)?;

    let mut version_cols: Vec<String> = rows
        .iter()
        .filter(|r| r.status == "ok")
        .map(|r| r.version_id.clone())
        .collect();
    version_cols.sort();
    version_cols.dedup();
    let col_index: HashMap<String, usize> = version_cols
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), i))
        .collect();
    let chunk_count = version_cols.len().div_ceil(64);

    let version_cols_path = args.index_dir.join("version_columns.tsv");
    let taxid_matrix_path = args.index_dir.join("taxid_matrix.tsv");
    let sci_name_matrix_path = args.index_dir.join("scientific_name_matrix.tsv");
    let taxid_sci_matrix_path = args.index_dir.join("taxid_scientific_name_matrix.tsv");
    let taxid_any_matrix_path = args.index_dir.join("taxid_any_name_matrix.tsv");

    write_version_columns(&version_cols_path, &version_cols)?;
    write_taxid_matrix(
        &taxid_matrix_path,
        &taxid_versions,
        &taxid_rank,
        &col_index,
        chunk_count,
    )?;
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

    let ok = rows.iter().filter(|r| r.status == "ok").count();
    println!("Wrote manifest: {}", args.manifest_out.display());
    println!("Wrote: {}", version_cols_path.display());
    println!("Wrote: {}", taxid_matrix_path.display());
    println!("Wrote: {}", sci_name_matrix_path.display());
    println!("Wrote: {}", taxid_sci_matrix_path.display());
    println!("Wrote: {}", taxid_any_matrix_path.display());
    println!("Archives processed: {}, successful: {}", rows.len(), ok);

    Ok(())
}
