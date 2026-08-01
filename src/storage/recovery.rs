//! Recovery control-flow scaffold.

use std::io::Read;

use super::StorageError;
use super::record::{self, Record};

/// Reads logical records in WAL order without applying them to an engine.
///
/// This is scaffolding, not working recovery: [`record::read`] remains learner-owned
/// and currently panics. Engine replay and publication ordering are intentionally
/// outside this module.
///
/// # Errors
///
/// Returns the first record-reader error after record reading is implemented.
///
/// # Panics
///
/// Panics through [`record::read`] until the learner implements record framing.
pub fn recover(reader: &mut impl Read) -> Result<Vec<Record>, StorageError> {
    let mut records = Vec::new();
    while let Some(record) = record::read(reader)? {
        records.push(record);
    }
    Ok(records)
}
