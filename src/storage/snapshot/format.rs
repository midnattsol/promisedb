//! Stable snapshot v1 framing constants.

pub(crate) const MAGIC: [u8; 4] = *b"PDBN";
pub(crate) const VERSION: u8 = 1;
pub(crate) const HEADER_LEN: usize = 128;
pub(crate) const CHECKSUM_LEN: usize = 32;
pub(crate) const TEMP_NAME: &str = "SNAPSHOT.tmp";
pub(crate) const DIRECTORY_NAME: &str = "snapshots";
pub(crate) const EXTENSION: &str = ".snapshot";
