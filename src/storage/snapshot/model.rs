//! Snapshot framing model and configured decoding budgets.

use uuid::Uuid;

use crate::engine::{Engine, EngineSnapshot};

use super::SnapshotError;
use super::format::{CHECKSUM_LEN, HEADER_LEN};

/// Explicit allocation and nesting limits for snapshot files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLimits {
    /// Maximum complete file size, including header and checksum trailer.
    pub max_total_bytes: u64,
    /// Maximum number of entries in any top-level collection.
    pub max_collection_items: u32,
    /// Maximum UTF-8 string length.
    pub max_string_bytes: u32,
    /// Maximum number of entries in any nested collection.
    pub max_nested_items: u32,
}

impl SnapshotLimits {
    /// Validates limits before they are persisted or used for allocation.
    pub fn validate(self) -> Result<Self, SnapshotError> {
        let minimum = u64::try_from(HEADER_LEN + CHECKSUM_LEN)
            .expect("fixed snapshot framing length fits u64");
        let usize_max = u64::try_from(usize::MAX).unwrap_or(u64::MAX);
        if self.max_total_bytes < minimum {
            return Err(SnapshotError::InvalidLimits {
                field: "maximum total bytes",
                value: self.max_total_bytes,
                minimum,
            });
        }
        if usize::try_from(self.max_total_bytes).is_err() {
            return Err(SnapshotError::Limit {
                field: "maximum total bytes",
                value: self.max_total_bytes,
                maximum: usize_max,
            });
        }
        for (field, value) in [
            ("maximum collection items", self.max_collection_items),
            ("maximum string bytes", self.max_string_bytes),
            ("maximum nested items", self.max_nested_items),
        ] {
            if value == 0 {
                return Err(SnapshotError::InvalidLimits {
                    field,
                    value: 0,
                    minimum: 1,
                });
            }
            if usize::try_from(value).is_err() {
                return Err(SnapshotError::Limit {
                    field,
                    value: u64::from(value),
                    maximum: usize_max,
                });
            }
        }
        Ok(self)
    }
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_total_bytes: 512 * 1024 * 1024,
            max_collection_items: 4_000_000,
            max_string_bytes: 16 * 1024 * 1024,
            max_nested_items: 4_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotFile {
    pub(crate) database_uuid: Uuid,
    pub(crate) semantics_version: u32,
    pub(crate) wal_watermark: u64,
    pub(crate) engine: EngineSnapshot,
}

/// A validated snapshot owning its restored, unindexed engine state.
pub(crate) struct DecodedSnapshot {
    /// Last WAL record represented by the engine state.
    pub(crate) wal_watermark: u64,
    /// Restored authoritative engine state awaiting one final index rebuild.
    pub(crate) engine: Engine,
}
