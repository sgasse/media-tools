use anyhow::Result;
use foto_sync::sync_files_to_location;
use log::info;

fn main() -> Result<()> {
    env_logger::init();
    info!("Starting syncer");

    let search_paths = ["/home/guest/TMP_IMG_CMP"];
    let extensions = ["jpg", "mp4"];
    let target_dir = "/home/guest/TMP_TARGET_DIR";

    sync_files_to_location(&search_paths, &extensions, target_dir)?;

    Ok(())
}
