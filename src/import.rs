//! Media file import
//!
//! This module provides a function to import media files.
//! Existing paths can be indexed
//! and duplicates can be skipped.

use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs,
    hash::{DefaultHasher, Hash, Hasher as _},
    path::{Path, PathBuf},
    str::FromStr as _,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike as _, FixedOffset};
use figment::{
    Figment,
    providers::{Format as _, Toml},
};
use log::{debug, warn};
use nom_exif::{
    EntryValue, Exif, ExifIter, ExifTag, MediaParser, MediaSource, TrackInfo, TrackInfoTag,
};
use serde::Deserialize;
use walkdir::WalkDir;

/// Import configuration
#[derive(Debug, Deserialize)]
pub struct Config {
    pub extensions: Vec<String>,
    pub existing_paths: Vec<String>,
    pub search_paths: Vec<String>,
    pub output_path: String,
}

impl Config {
    /// Try loading the configuration from a toml file
    pub fn try_load(toml: &str) -> Result<Self> {
        Figment::new()
            .merge(Toml::file(toml))
            .extract()
            .map_err(Into::into)
    }
}

/// Import media files according to the [Config]
pub fn import_media_files(config: &Config) -> Result<()> {
    let extensions: HashSet<OsString> = build_extension_set(&config.extensions)?;

    // Index existing media files
    let existing = MediaFiles::from_paths(&config.existing_paths, &extensions);

    // Synchronize files from search paths
    sync_media_files(
        &existing,
        &config.search_paths,
        &extensions,
        Path::new(&config.output_path),
    )
}

/// Synchronize files to `output_path` which are not found in `existing`
fn sync_media_files(
    existing: &MediaFiles,
    search_paths: &[String],
    extensions: &HashSet<OsString>,
    output_path: &Path,
) -> Result<()> {
    // Crawl through search paths
    for path in find_media_files(search_paths, extensions) {
        // Check for a match with an existing file
        let key = hashed(path.file_name());
        if let Some(existing) = existing.name_map.get(&key) {
            // We have at least one file with the same filename.
            // In the majority of cases, this is the exact same file.
            // Reading the file size is cheap,
            // reading the exif create date is more expensive via the slow connection.

            // We check first if there is an exact size match and skip the duplicate in this case.
            let file_size = file_size(&path)?;

            if existing.iter().any(|e| e.size == file_size) {
                debug!(
                    "Identified {} as duplicate of an existing file (same name, both {file_size} bytes)",
                    path.display(),
                );
                continue;
            }

            // There is no size match, we have to check the exif date
            // to identify if this is the same media file with differing quality.
            let created = exif_created(&path).unwrap_or_default();
            if let Some(existing) = existing.iter().find(|e| e.created == created) {
                debug!(
                    "File {} ({file_size} bytes) is already found at {} ({} bytes)",
                    path.display(),
                    existing.path.display(),
                    existing.size,
                );

                if file_size <= existing.size {
                    // The new version is of lower or equal quality.
                    debug!(
                        "Skipping duplicate / lower-quality version of {}",
                        path.display()
                    );
                    continue;
                }
            }
        }

        // Copy file to target location
        let created = exif_created(&path).unwrap_or_default();
        let date_path = output_path.join(format!(
            "{:04}_{:02}_{:02}",
            created.year(),
            created.month(),
            created.day()
        ));
        if !date_path.is_dir() {
            debug!("Creating date directory {}", date_path.display());
            fs::create_dir_all(&date_path)?;
        }

        let target_file = date_path.join(path.file_name().unwrap());
        fs::copy(&path, &target_file)?;
        debug!("Copied {} to {}", path.display(), target_file.display());
    }

    Ok(())
}

/// Set of existing [MediaFile]s
struct MediaFiles {
    /// Map from hashed filenames to vectors of indexed files by this name
    name_map: HashMap<u64, Vec<MediaFile>>,
}

impl MediaFiles {
    fn from_paths(paths: &[String], extensions: &HashSet<OsString>) -> Self {
        let mut name_map = HashMap::new();

        for existing in paths.iter().flat_map(|p| {
            WalkDir::new(p)
                .into_iter()
                .filter_map(|x| x.ok())
                .filter(|e| !e.file_type().is_dir())
                .filter_map(|e| match e.path().extension() {
                    Some(ext) if extensions.contains(ext) => Some(e.path().to_owned()),
                    _ => None,
                })
                .filter_map(|p| {
                    MediaFile::try_from_path(&p)
                        .inspect_err(|e| warn!("Failed to parse {}: {e:#}", p.display()))
                        .ok()
                })
        }) {
            let key = hashed(existing.path.file_name());
            name_map
                .entry(key)
                .and_modify(|v: &mut Vec<MediaFile>| v.push(existing.clone()))
                .or_insert_with(|| vec![existing]);
        }

        Self { name_map }
    }
}

/// Indexed media file
#[derive(Debug, Clone)]
struct MediaFile {
    /// Full path to file
    path: PathBuf,
    /// Exif creation timestamp
    created: DateTime<FixedOffset>,
    /// File size in bytes
    size: u64,
}

impl MediaFile {
    /// Try to read a file from the `path`
    fn try_from_path(path: &Path) -> Result<Self> {
        let created = exif_created(path).unwrap_or_default();
        let size =
            file_size(path).with_context(|| format!("failed to get size of {}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            created,
            size,
        })
    }
}

/// Find media files in `search_paths` matching `extensions`
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

/// Build set of extension to crawl for
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

/// Try to extract the exif creation timestamp from the file at `path`
fn exif_created(path: &Path) -> Option<DateTime<FixedOffset>> {
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

/// Try to read the file size of the file at `path`
fn file_size(path: &Path) -> Result<u64> {
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

/// Get the hashed value of `data`
fn hashed<H: Hash>(data: H) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}
