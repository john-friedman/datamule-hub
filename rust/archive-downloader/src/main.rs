use anyhow::{anyhow, bail, Context, Result};
use futures::{stream, StreamExt};
use reqwest::{header, Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;

const SGML_BASE_URL: &str = "https://sec-filings-archive.sgml.datamule.xyz";
const TAR_BASE_URL: &str = "https://sec-filings-archive.tar.datamule.xyz";
const TAR_BLOCK_SIZE: u64 = 512;
const TAR_END_SIZE: u64 = 1024;
const ZSTD_MAGIC: &[u8] = b"\x28\xb5\x2f\xfd";

#[derive(Clone, Copy)]
enum Mode {
    Sgml,
    Metadata,
    Range,
    Submission,
}

impl Mode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "sgml" => Ok(Self::Sgml),
            "metadata" => Ok(Self::Metadata),
            "range" => Ok(Self::Range),
            "submission" => Ok(Self::Submission),
            _ => bail!("unknown download mode: {value}"),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Job {
    filing_date: String,
    accession: String,
    filename: Option<String>,
    start: Option<u64>,
    end: Option<u64>,
}

struct DownloadItem {
    logical_path: PathBuf,
    data: Vec<u8>,
}

enum OutputSink {
    Files { output_dir: PathBuf },
    Tar(TarBatchWriter),
}

impl OutputSink {
    fn write(&mut self, item: DownloadItem) -> Result<PathBuf> {
        match self {
            Self::Files { output_dir } => {
                let path = output_dir.join(item.logical_path);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&path, item.data)?;
                Ok(path)
            }
            Self::Tar(writer) => writer.write(item),
        }
    }

    fn finish(&mut self) -> Result<()> {
        if let Self::Tar(writer) = self {
            writer.finish()?;
        }
        Ok(())
    }
}

struct TarBatchWriter {
    output_dir: PathBuf,
    max_size: u64,
    index: usize,
    current_size: u64,
    current_path: Option<PathBuf>,
    builder: Option<tar::Builder<BufWriter<File>>>,
}

impl TarBatchWriter {
    fn new(output_dir: PathBuf, max_size: u64) -> Self {
        Self {
            output_dir,
            max_size,
            index: 0,
            current_size: 0,
            current_path: None,
            builder: None,
        }
    }

    fn write(&mut self, item: DownloadItem) -> Result<PathBuf> {
        let data_size = item.data.len() as u64;
        let padded = data_size.div_ceil(TAR_BLOCK_SIZE) * TAR_BLOCK_SIZE;
        let entry_size = TAR_BLOCK_SIZE + padded;

        if self.builder.is_some()
            && self.current_size > 0
            && self.current_size + entry_size + TAR_END_SIZE > self.max_size
        {
            self.open_next()?;
        }
        if self.builder.is_none() {
            self.open_next()?;
        }

        let mut header = tar::Header::new_gnu();
        header.set_path(&item.logical_path)?;
        header.set_size(data_size);
        header.set_mode(0o644);
        header.set_cksum();
        self.builder
            .as_mut()
            .expect("tar builder was opened")
            .append(&header, item.data.as_slice())?;
        self.current_size += entry_size;
        Ok(self.current_path.clone().expect("tar path was opened"))
    }

    fn open_next(&mut self) -> Result<()> {
        self.finish()?;
        self.index += 1;
        let path = self.output_dir.join(format!("batch_{:06}.tar", self.index));
        let file = File::create(&path)?;
        self.builder = Some(tar::Builder::new(BufWriter::new(file)));
        self.current_path = Some(path);
        self.current_size = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(mut builder) = self.builder.take() {
            builder.finish()?;
        }
        self.current_path = None;
        self.current_size = 0;
        Ok(())
    }
}

fn file_name(value: &str) -> Result<String> {
    Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("invalid archive filename: {value:?}"))
}

fn tar_url(job: &Job) -> String {
    format!("{TAR_BASE_URL}/{}/{}.tar", job.filing_date, job.accession)
}

async fn get_full(client: &Client, url: &str) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("unsuccessful response: {url}"))?;
    Ok(response.bytes().await?.to_vec())
}

async fn get_range(client: &Client, url: &str, start: u64, end: u64) -> Result<Vec<u8>> {
    if end <= start {
        bail!("invalid byte range {start}-{end} for {url}");
    }
    let response = client
        .get(url)
        .header(header::RANGE, format!("bytes={}-{}", start, end - 1))
        .send()
        .await
        .with_context(|| format!("range request failed: {url}"))?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        bail!("expected HTTP 206, got {}: {url}", response.status());
    }
    let content_range = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow!("missing Content-Range: {url}"))?;
    let expected_prefix = format!("bytes {}-{}/", start, end - 1);
    if !content_range.starts_with(&expected_prefix) {
        bail!("unexpected Content-Range {content_range:?}; expected {expected_prefix:?}: {url}");
    }

    let data = response.bytes().await?.to_vec();
    if data.len() as u64 != end - start {
        bail!("range response had the wrong length: {url}");
    }
    Ok(data)
}

fn metadata_size(header: &[u8]) -> Result<u64> {
    if header.len() != TAR_BLOCK_SIZE as usize {
        bail!("expected a 512-byte tar header");
    }
    let raw_name = &header[..100];
    let name_end = raw_name
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(raw_name.len());
    let name = std::str::from_utf8(&raw_name[..name_end])?;
    if file_name(name)? != "metadata.json" {
        bail!("expected metadata.json as the first tar member, got {name:?}");
    }
    let raw_size = std::str::from_utf8(&header[124..136])?
        .trim_matches('\0')
        .trim();
    Ok(u64::from_str_radix(raw_size, 8)?)
}

fn decode_zstd(data: Vec<u8>) -> Result<Vec<u8>> {
    Ok(zstd::stream::decode_all(Cursor::new(data))?)
}

fn decode_sgml(data: Vec<u8>) -> Result<Vec<u8>> {
    if data.starts_with(ZSTD_MAGIC) {
        decode_zstd(data)
    } else {
        Ok(data)
    }
}

async fn run_blocking<T, F>(semaphore: Arc<Semaphore>, function: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let permit = semaphore.acquire_owned().await?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        function()
    })
    .await?
}

fn extract_submission(job: Job, tar_bytes: Vec<u8>, decompress: bool) -> Result<Vec<DownloadItem>> {
    let mut output = Vec::new();
    let mut archive = tar::Archive::new(Cursor::new(tar_bytes));
    for entry in archive.entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let member_path = entry.path()?;
        let Some(member_name) = member_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let member_name = member_name.to_owned();
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;

        let output_name = if member_name == "metadata.json" {
            member_name
        } else if decompress {
            data = decode_zstd(data)?;
            member_name
        } else {
            format!("{member_name}.zst")
        };
        output.push(DownloadItem {
            logical_path: PathBuf::from(&job.filing_date)
                .join(&job.accession)
                .join(output_name),
            data,
        });
    }
    Ok(output)
}

async fn process_job(
    mode: Mode,
    job: Job,
    client: Client,
    decompress: bool,
    cpu: Arc<Semaphore>,
) -> Result<Vec<DownloadItem>> {
    match mode {
        Mode::Sgml => {
            let url = format!(
                "{SGML_BASE_URL}/{}/{}.sgml.zst",
                job.filing_date, job.accession
            );
            let mut data = get_full(&client, &url).await?;
            let output_name = if decompress {
                data = run_blocking(cpu, move || decode_sgml(data)).await?;
                format!("{}.sgml", job.accession)
            } else {
                format!("{}.sgml.zst", job.accession)
            };
            Ok(vec![DownloadItem {
                logical_path: PathBuf::from(job.filing_date).join(output_name),
                data,
            }])
        }
        Mode::Metadata => {
            let url = tar_url(&job);
            let header = get_range(&client, &url, 0, TAR_BLOCK_SIZE).await?;
            let size = metadata_size(&header)?;
            let data = if size == 0 {
                Vec::new()
            } else {
                get_range(&client, &url, TAR_BLOCK_SIZE, TAR_BLOCK_SIZE + size).await?
            };
            serde_json::from_slice::<serde_json::Value>(&data)
                .with_context(|| format!("invalid metadata.json: {url}"))?;
            Ok(vec![DownloadItem {
                logical_path: PathBuf::from(job.filing_date)
                    .join(job.accession)
                    .join("metadata.json"),
                data,
            }])
        }
        Mode::Range => {
            let url = tar_url(&job);
            let start = job
                .start
                .ok_or_else(|| anyhow!("range job is missing start"))?;
            let end = job.end.ok_or_else(|| anyhow!("range job is missing end"))?;
            let source_name = job
                .filename
                .as_deref()
                .ok_or_else(|| anyhow!("range job is missing filename"))?;
            let mut data = get_range(&client, &url, start, end).await?;
            let output_name = if decompress {
                data = run_blocking(cpu, move || decode_zstd(data)).await?;
                file_name(source_name)?
            } else {
                format!("{}.zst", file_name(source_name)?)
            };
            Ok(vec![DownloadItem {
                logical_path: PathBuf::from(job.filing_date)
                    .join(job.accession)
                    .join(output_name),
                data,
            }])
        }
        Mode::Submission => {
            let url = tar_url(&job);
            let data = get_full(&client, &url).await?;
            run_blocking(cpu, move || extract_submission(job, data, decompress)).await
        }
    }
}

fn read_jobs(path: &Path) -> Result<Vec<Job>> {
    let reader = BufReader::new(File::open(path)?);
    reader
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let line = line?;
            serde_json::from_str(&line)
                .with_context(|| format!("invalid manifest record on line {}", index + 1))
        })
        .collect()
}

fn parse_bool(value: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("expected true or false, got {value:?}"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 8 {
        bail!(
            "usage: {} MODE MANIFEST OUTPUT_DIR MAX_WORKERS DECOMP_WORKERS DECOMPRESS TAR_MAX_BYTES",
            args.first().map(String::as_str).unwrap_or("archive-downloader")
        );
    }

    let mode = Mode::parse(&args[1])?;
    let jobs = read_jobs(Path::new(&args[2]))?;
    let output_dir = PathBuf::from(&args[3]);
    let max_workers: usize = args[4].parse()?;
    let decomp_workers: usize = args[5].parse()?;
    let decompress = parse_bool(&args[6])?;
    if max_workers == 0 || decomp_workers == 0 {
        bail!("worker counts must be positive");
    }
    std::fs::create_dir_all(&output_dir)?;

    let mut sink = if args[7] == "none" {
        OutputSink::Files {
            output_dir: output_dir.clone(),
        }
    } else {
        OutputSink::Tar(TarBatchWriter::new(output_dir.clone(), args[7].parse()?))
    };

    let client = Client::builder()
        .pool_max_idle_per_host(max_workers)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .timeout(std::time::Duration::from_secs(300))
        .user_agent("datamule-hub")
        .build()?;
    let cpu = Arc::new(Semaphore::new(decomp_workers));
    let stdout = std::io::stdout();
    let mut output = BufWriter::new(stdout.lock());

    let work = stream::iter(jobs.into_iter().map(|job| {
        let client = client.clone();
        let cpu = cpu.clone();
        async move {
            let context = format!(
                "filing_date={} accession={}",
                job.filing_date, job.accession
            );
            (
                context,
                process_job(mode, job, client, decompress, cpu).await,
            )
        }
    }))
    .buffer_unordered(max_workers);
    futures::pin_mut!(work);

    while let Some((context, result)) = work.next().await {
        match result {
            Ok(items) => {
                let mut paths = Vec::with_capacity(items.len());
                for item in items {
                    paths.push(sink.write(item)?.to_string_lossy().into_owned());
                }
                serde_json::to_writer(
                    &mut output,
                    &json!({
                        "type": "complete",
                        "paths": paths,
                    }),
                )?;
            }
            Err(error) => {
                serde_json::to_writer(
                    &mut output,
                    &json!({
                        "type": "error",
                        "message": format!("{context}: {error:#}"),
                    }),
                )?;
            }
        }
        output.write_all(b"\n")?;
        output.flush()?;
    }

    sink.finish()?;
    Ok(())
}
