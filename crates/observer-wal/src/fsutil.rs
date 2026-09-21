use std::{fs::File, path::Path};

use crate::WalError;

pub(crate) fn sync_directory(path: &Path) -> Result<(), WalError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
