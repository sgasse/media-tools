use anyhow::Result;
use foto_sync::{sync_files_to_location, Config};
use log::info;

fn main() -> Result<()> {
    env_logger::init();
    info!("Starting syncer");

    let config = Config::try_load("config.yml")?;

    sync_files_to_location(
        config.existing_paths.as_slice(),
        config.search_paths.as_slice(),
        config.extensions.as_slice(),
        &config.target_dir,
    )?;

    Ok(())
}
