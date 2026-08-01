//! Deterministic commands accepted by the PromiseDB state machine.
//!
//! A command describes what a client requested. It does not describe what
//! happened: successful state changes are represented separately as events.

/// A deterministic request to mutate PromiseDB state.
///
/// Variants are intentionally left for the command-language design step. Every
/// variant must contain all client-provided data needed for replay, while the
/// authoritative timestamp remains an explicit argument to `Engine::apply`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {}

/// The operation-specific response produced by applying a [`Command`].
///
/// Business outcomes such as unavailable capacity belong here rather than in the
/// event stream. Variants will be added alongside the corresponding commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {}
