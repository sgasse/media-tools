use anyhow::Result;
use log::info;
use media_tools::{ImportConfig, import_media_files};

fn main() -> Result<()> {
    env_logger::init();
    info!("Starting importer");

    let config = ImportConfig::try_load("config.toml")?;
    import_media_files(&config)?;

    Ok(())
}
