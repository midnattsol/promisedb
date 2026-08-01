//! Reconstructible indexes used by the engine's hot paths.
//!
//! Indexes accelerate authoritative decisions but are not sources of truth.
//! They must be rebuildable from resource capacity and active promises.

mod error;
mod slack_timeline;

pub use error::IndexError;
pub use slack_timeline::{Slack, SlackBlock, SlackDeficit, SlackPoint, SlackTimeline};
