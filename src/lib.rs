use std::{
    collections::HashSet,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{bail, Result};
use chrono::Datelike;
use log::debug;
use nom_exif::{
    EntryValue, Exif, ExifIter, ExifTag, MediaParser, MediaSource, TrackInfo, TrackInfoTag,
};
use walkdir::WalkDir;

pub fn sync_files_to_location(
    search_paths: &[&str],
    extensions: &[&str],
    target_dir: &str,
) -> Result<()> {
    let extensions = build_extension_set(extensions)?;
    let media_files = find_media_files(search_paths, &extensions);

    let target_dir = Path::new(target_dir);
    if !target_dir.is_dir() {
        debug!("Creating target directory {}", target_dir.display());
        fs::create_dir_all(target_dir)?;
    }

    for file in media_files {
        debug!("Found file {}", file.display());
        let date = get_file_date(&file);

        let date_dir = target_dir.join(format!(
            "{:04}_{:02}_{:02}",
            date.year, date.month, date.day
        ));
        if !date_dir.is_dir() {
            debug!("Creating date directory {}", target_dir.display());
            fs::create_dir(&date_dir)?;
        }
        let target_file = date_dir.join(file.file_name().unwrap());
        fs::copy(&file, &target_file)?;
        debug!("Copied {} to {}", file.display(), target_file.display());
    }

    Ok(())
}

fn build_extension_set(extensions: &[&str]) -> Result<HashSet<OsString>> {
    let mut exts = HashSet::new();

    for extension in extensions {
        if extension.contains('.') {
            bail!("extensions must not contain '.' but got '{extension}'");
        }
        exts.insert(OsString::from_str(&extension)?);
    }

    Ok(exts)
}

fn find_media_files<'a>(
    search_paths: &'a [&str],
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

#[derive(Debug)]
struct Date {
    year: i32,
    month: u32,
    day: u32,
}

fn get_file_date(path: &Path) -> Date {
    if let Some(date) = get_exif_date(path) {
        return date;
    }

    // TODO
    // Fall back to fs creation date

    Date {
        year: 0,
        month: 0,
        day: 0,
    }
}

fn get_exif_date(path: &Path) -> Option<Date> {
    fn extract_date(value: &EntryValue) -> Option<Date> {
        if let EntryValue::Time(create_date) = value {
            Some(Date {
                year: create_date.year(),
                month: create_date.month(),
                day: create_date.day(),
            })
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
