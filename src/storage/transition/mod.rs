//! Stable codec facade for durable prepared-transition effects.

pub(crate) mod format;
mod reader;
mod writer;

pub(crate) const TRANSITION_FORMAT_VERSION: u8 = 1;

pub(crate) use reader::{Reader, decode_transition};
#[cfg(test)]
pub(crate) use writer::encode_transition;
pub(crate) use writer::{Writer, encode_transition_into};
