use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{bail, Result};
use chrono::{DateTime, Datelike, FixedOffset};
use figment::{
    providers::{Format as _, Yaml},
    Figment,
};
use log::{debug, info};
use nom_exif::{
    EntryValue, Exif, ExifIter, ExifTag, MediaParser, MediaSource, TrackInfo, TrackInfoTag,
};
use serde::Deserialize;
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub existing_paths: Vec<String>,
    pub search_paths: Vec<String>,
    pub extensions: Vec<String>,
    pub target_dir: String,
}

impl Config {
    pub fn try_load(yaml: &str) -> Result<Self> {
        Figment::new()
            .merge(Yaml::file(yaml))
            .extract()
            .map_err(Into::into)
    }
}

pub fn sync_files_to_location(
    existing_paths: &[String],
    search_paths: &[String],
    extensions: &[String],
    target_dir: &str,
) -> Result<()> {
    let extensions = build_extension_set(extensions)?;

    let existing_files = build_existing_files_set(existing_paths, &extensions);
    info!("Found {} existing media files", existing_files.len());

    let media_files = find_media_files(search_paths, &extensions);

    let target_dir = Path::new(target_dir);
    if !target_dir.is_dir() {
        info!("Creating target directory {}", target_dir.display());
        fs::create_dir_all(target_dir)?;
    }

    let mut stats = Statistics::default();

    for file in media_files {
        debug!("Found file {}", file.display());
        stats.found += 1;
        let key = hashed(file.file_name());

        let created = if let Some(existing) = existing_files.get(&key) {
            // We have at least one file with the same filename.
            // In the majority of cases, this is the exact same file.
            // Reading the file size is cheap,
            // reading the exif create date is more expensive via the slow connection.

            // We check first if there is an exact size match and skip the duplicate in this case.
            stats.name_existing += 1;
            let file_size = get_file_size(&file)?;

            if existing.iter().any(|e| e.size == file_size) {
                debug!(
                    "Identified {} as duplicate of an existing file (same name, both {} bytes)",
                    file.display(),
                    file_size
                );
                stats.skipped += 1;
                continue;
            }

            // There is no size match, we have to check the exif date
            // to identify if this is the same media file with differing quality.
            let created = get_exif_date(&file).unwrap_or_default();
            if let Some(existing) = existing.iter().find(|e| e.created == created) {
                debug!(
                    "File {} ({} bytes) is already found at {} ({} bytes)",
                    file.display(),
                    file_size,
                    existing.path.display(),
                    existing.size,
                );

                if file_size <= existing.size {
                    // The new version is of lower or equal quality.
                    debug!(
                        "Skipping duplicate / lower-quality version of {}",
                        file.display()
                    );
                    stats.skipped += 1;
                    continue;
                }
            }

            // The new file matches by exif date and has a larger file size (assumed to be better quality).
            // Copy new file.
            stats.copied_hq += 1;
            created

            // TODO: Move to a subdirectory that makes replacing the lower-quality version easier.
            // TODO: Same day, same name, different exif date?
        } else {
            get_exif_date(&file).unwrap_or_default()
        };

        let date_dir = target_dir.join(format!(
            "{:04}_{:02}_{:02}",
            created.year(),
            created.month(),
            created.day()
        ));
        if !date_dir.is_dir() {
            debug!("Creating date directory {}", target_dir.display());
            fs::create_dir(&date_dir)?;
        }

        let target_file = date_dir.join(file.file_name().unwrap());
        fs::copy(&file, &target_file)?;
        debug!("Copied {} to {}", file.display(), target_file.display());
        stats.copied += 1;
    }

    info!("{:#?}", stats);

    Ok(())
}

fn build_extension_set(extensions: &[String]) -> Result<HashSet<OsString>> {
    let mut exts = HashSet::new();

    for extension in extensions {
        if extension.contains('.') {
            bail!("extensions must not contain '.' but got '{extension}'");
        }
        exts.insert(OsString::from_str(extension)?);
    }

    Ok(exts)
}

fn build_existing_files_set(
    existing_paths: &[String],
    extensions: &HashSet<OsString>,
) -> HashMap<u64, Vec<ExistingFile>> {
    let mut existing_files = HashMap::new();

    for existing in existing_paths.iter().flat_map(|p| {
        WalkDir::new(p)
            .into_iter()
            .filter_map(|x| x.ok())
            .filter(|e| !e.file_type().is_dir())
            .filter_map(|e| match e.path().extension() {
                Some(ext) if extensions.contains(ext) => Some(e.path().to_owned()),
                _ => None,
            })
            .filter_map(|e| ExistingFile::create_from_path(&e).ok())
    }) {
        let key = hashed(existing.path.file_name());
        existing_files
            .entry(key)
            .and_modify(|v: &mut Vec<ExistingFile>| v.push(existing.clone()))
            .or_insert_with(|| vec![existing]);
    }

    existing_files
}

#[derive(Debug, Clone)]
struct ExistingFile {
    created: DateTime<FixedOffset>,
    size: u64,
    path: PathBuf,
}

impl ExistingFile {
    fn create_from_path(path: &Path) -> Result<Self> {
        let created = get_exif_date(path).unwrap_or_default();
        let size = get_file_size(path)?;
        let path = path.to_owned();

        Ok(Self {
            created,
            size,
            path,
        })
    }
}

fn get_file_size(path: &Path) -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(path).map(|m| m.size()).map_err(Into::into)
    }

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::fs::MetadataExt;
        fs::metadata(path)
            .map(|m| m.file_size())
            .map_err(Into::into)
    }
}

fn hashed<T: Hash>(data: T) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

fn find_media_files<'a>(
    search_paths: &'a [String],
    extensions: &'a HashSet<OsString>,
) -> impl Iterator<Item = PathBuf> + 'a {
    search_paths.iter().flat_map(|s| {
        WalkDir::new(s)
            .into_iter()
            .filter_map(|x| x.ok())
            .filter(|e| !e.file_type().is_dir())
            .filter_map(|e| match e.path().extension() {
                Some(ext) if extensions.contains(ext) => Some(e.path().to_owned()),
                _ => None,
            })
    })
}

fn get_exif_date(path: &Path) -> Option<DateTime<FixedOffset>> {
    fn extract_date(value: &EntryValue) -> Option<DateTime<FixedOffset>> {
        if let EntryValue::Time(create_date) = value {
            Some(*create_date)
        } else {
            None
        }
    }

    let mut parser = MediaParser::new();
    let src = MediaSource::file_path(path).ok()?;

    if src.has_exif() {
        let exif: ExifIter = parser.parse(src).ok()?;
        let exif: Exif = exif.into();
        return exif.get(ExifTag::CreateDate).and_then(extract_date);
    } else if src.has_track() {
        let track_info: TrackInfo = parser.parse(src).ok()?;
        return track_info
            .get(TrackInfoTag::CreateDate)
            .and_then(extract_date);
    }

    None
}

#[derive(Debug, Default)]
struct Statistics {
    name_existing: usize,
    found: usize,
    skipped: usize,
    copied_hq: usize,
    copied: usize,
}
