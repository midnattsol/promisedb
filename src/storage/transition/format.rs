//! Stable version-one tags shared by transition encoding and decoding.

pub(super) mod promise_state {
    pub(in crate::storage::transition) const HELD: u8 = 1;
    pub(in crate::storage::transition) const COMMITTED: u8 = 2;
    pub(in crate::storage::transition) const RELEASED: u8 = 3;
    pub(in crate::storage::transition) const EXPIRED: u8 = 4;
}

pub(super) mod response {
    pub(in crate::storage::transition) const SUCCESS: u8 = 1;
    pub(in crate::storage::transition) const DOMAIN_ERROR: u8 = 2;
}

pub(super) mod command_result {
    pub(in crate::storage::transition) const RESOURCE_POOL_CREATED: u8 = 1;
    pub(in crate::storage::transition) const CAPACITY_REVISED: u8 = 2;
    pub(in crate::storage::transition) const HOLD_COMPLETED: u8 = 3;
    pub(in crate::storage::transition) const CHOICE_COMPLETED: u8 = 4;
    pub(in crate::storage::transition) const SLOT_COMPLETED: u8 = 5;
    pub(in crate::storage::transition) const PROMISE_COMMITTED: u8 = 6;
    pub(in crate::storage::transition) const PROMISE_RELEASED: u8 = 7;
    pub(in crate::storage::transition) const PROMISE_REPLACED: u8 = 8;
    pub(in crate::storage::transition) const EXPIRATIONS_PROCESSED: u8 = 9;
}

pub(super) mod hold_outcome {
    pub(in crate::storage::transition) const HELD: u8 = 1;
    pub(in crate::storage::transition) const UNAVAILABLE: u8 = 2;
}

pub(super) mod choice_outcome {
    pub(in crate::storage::transition) const HELD: u8 = 1;
    pub(in crate::storage::transition) const UNAVAILABLE: u8 = 2;
}

pub(super) mod slot_outcome {
    pub(in crate::storage::transition) const HELD: u8 = 1;
    pub(in crate::storage::transition) const UNAVAILABLE: u8 = 2;
}

pub(super) mod replace_outcome {
    pub(in crate::storage::transition) const REPLACED: u8 = 1;
    pub(in crate::storage::transition) const UNAVAILABLE: u8 = 2;
}

pub(super) mod domain_error {
    pub(in crate::storage::transition) const INVALID_INTERVAL: u8 = 1;
    pub(in crate::storage::transition) const UNSORTED_CAPACITY_SEGMENTS: u8 = 2;
    pub(in crate::storage::transition) const OVERLAPPING_CAPACITY_SEGMENTS: u8 = 3;
    pub(in crate::storage::transition) const INVALID_UNIT_NAME: u8 = 4;
    pub(in crate::storage::transition) const INVALID_UNIT_SCALE: u8 = 5;
    pub(in crate::storage::transition) const INVALID_QUANTITY: u8 = 6;
    pub(in crate::storage::transition) const QUANTITY_OUT_OF_RANGE: u8 = 7;
    pub(in crate::storage::transition) const QUANTITY_OVERFLOW: u8 = 8;
    pub(in crate::storage::transition) const INDEX_OVERFLOW: u8 = 9;
    pub(in crate::storage::transition) const INVALID_EXPIRATION: u8 = 10;
    pub(in crate::storage::transition) const EMPTY_BUNDLE: u8 = 11;
    pub(in crate::storage::transition) const EMPTY_RELATIVE_BUNDLE: u8 = 12;
    pub(in crate::storage::transition) const EMPTY_CHOICE: u8 = 13;
    pub(in crate::storage::transition) const INVALID_SEARCH_RANGE: u8 = 14;
    pub(in crate::storage::transition) const INVALID_STEP: u8 = 15;
    pub(in crate::storage::transition) const TIMESTAMP_OVERFLOW: u8 = 16;
    pub(in crate::storage::transition) const RESOURCE_POOL_ALREADY_EXISTS: u8 = 17;
    pub(in crate::storage::transition) const RESOURCE_POOL_NOT_FOUND: u8 = 18;
    pub(in crate::storage::transition) const CAPACITY_EXCEEDED: u8 = 19;
    pub(in crate::storage::transition) const CAPACITY_REVISION_CREATES_DEFICIT: u8 = 20;
    pub(in crate::storage::transition) const PROMISE_ALREADY_EXISTS: u8 = 21;
    pub(in crate::storage::transition) const PROMISE_NOT_FOUND: u8 = 22;
    pub(in crate::storage::transition) const INVALID_PROMISE_STATE: u8 = 23;
    pub(in crate::storage::transition) const IDEMPOTENCY_CONFLICT: u8 = 24;
    pub(in crate::storage::transition) const VERSION_CONFLICT: u8 = 25;
    pub(in crate::storage::transition) const VERSION_OVERFLOW: u8 = 26;
    pub(in crate::storage::transition) const SEQUENCE_OVERFLOW: u8 = 27;
    pub(in crate::storage::transition) const SYSTEM_TIME_OUT_OF_RANGE: u8 = 28;
    pub(in crate::storage::transition) const HOLD_EXPIRED: u8 = 29;
    pub(in crate::storage::transition) const HOLD_NOT_EXPIRED: u8 = 30;
    pub(in crate::storage::transition) const INVALID_PROMISE_HISTORY: u8 = 31;
    pub(in crate::storage::transition) const PUBLICATION_REVISION_OVERFLOW: u8 = 32;
}

pub(super) mod event_kind {
    pub(in crate::storage::transition) const RESOURCE_CREATED: u8 = 1;
    pub(in crate::storage::transition) const CAPACITY_REVISED: u8 = 2;
    pub(in crate::storage::transition) const HOLD_CREATED: u8 = 3;
    pub(in crate::storage::transition) const HOLD_COMMITTED: u8 = 4;
    pub(in crate::storage::transition) const PROMISE_RELEASED: u8 = 5;
    pub(in crate::storage::transition) const PROMISE_REPLACED: u8 = 6;
    pub(in crate::storage::transition) const HOLD_EXPIRED: u8 = 7;
    pub(in crate::storage::transition) const DEFICIT_CREATED: u8 = 8;
    pub(in crate::storage::transition) const DEFICIT_CHANGED: u8 = 9;
    pub(in crate::storage::transition) const DEFICIT_RESOLVED: u8 = 10;
}

pub(super) mod event_data {
    pub(in crate::storage::transition) const RESOURCE_POOL: u8 = 1;
    pub(in crate::storage::transition) const PROMISE: u8 = 2;
    pub(in crate::storage::transition) const DEFICIT: u8 = 3;
}
