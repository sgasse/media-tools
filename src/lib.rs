use std::{
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
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
        let created = get_exif_date(&file).unwrap_or_default();
        let key = hashed(file.file_name(), &created);

        if let Some(existing) = existing_files.get(&key) {
            // We assume pictures with the same name **and** the same creation date
            // are either duplicates or different-quality versions of the same image.
            // Duplicates are absolutely common when syncing from the same source again.
            // Two versions of the same picture with different quality
            // occurs when one version is the lower-quality copy from Google Photos
            // and the other version is the original.
            stats.existing += 1;
            let file_size = get_file_size(&file)?;
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

            stats.copied_hq += 1;

            // We assume the current file is higher-quality version of the same image.
            // TODO: Move to a subdirectory that makes replacing the lower-quality version easier.
        }

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

    // TODO: No newlines in log
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
) -> HashMap<u64, ExistingFile> {
    existing_paths
        .iter()
        .flat_map(|p| {
            WalkDir::new(p)
                .into_iter()
                .filter_map(|x| x.ok())
                .filter(|e| !e.file_type().is_dir())
                .filter_map(|e| match e.path().extension() {
                    Some(ext) if extensions.contains(ext) => Some(e.path().to_owned()),
                    _ => None,
                })
                .filter_map(|e| ExistingFile::create_from_path(&e).ok())
                .map(|e| (hashed(e.path.file_name(), &e.created), e))
        })
        .collect()
}

#[derive(Debug)]
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

fn hashed(filename: Option<&OsStr>, created: &DateTime<FixedOffset>) -> u64 {
    let mut hasher = DefaultHasher::new();
    if let Some(filename) = filename {
        filename.hash(&mut hasher);
    }
    created.hash(&mut hasher);
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
    existing: usize,
    found: usize,
    skipped: usize,
    copied_hq: usize,
    copied: usize,
}

#[allow(dead_code)]
fn dump_exif_info(path: &Path) {
    let mut parser = MediaParser::new();
    let Ok(src) = MediaSource::file_path(path) else {
        return;
    };

    if src.has_exif() {
        println!("Exif info of foto {}", path.display());

        let Ok(iter) = parser.parse::<_, _, ExifIter>(src) else {
            return;
        };
        for info in iter {
            println!("{info:?}")
        }
    } else if src.has_track() {
        println!("Exif info of video {}", path.display());

        let Ok(track_info) = parser.parse::<_, _, TrackInfo>(src) else {
            return;
        };
        for info in track_info {
            println!("{info:?}")
        }
    }
}
