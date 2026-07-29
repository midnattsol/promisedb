//! Transactional commitment processing for finite future capacity.
//!
//! PromiseDB models capacity as resource pools and atomically admits bundles of
//! time-bounded claims. The domain module defines validated values, while the
//! engine owns global state and evaluates availability across active promises.

pub mod clock;
pub mod domain;
pub mod engine;
pub mod index;
