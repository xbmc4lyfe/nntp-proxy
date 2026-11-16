//! Backbone detection: which configured servers read one article store.
//!
//! Many resellers sell access to the same few backbones. Servers on one
//! backbone share their `430`s (a miss on one skips the rest), so knowing the
//! grouping saves lookups and lets several accounts act as one big backend.
//! Operators can name a server's backbone in the config; this module checks
//! those names against what the servers themselves report, fills in servers
//! left on `auto`, regroups at runtime when the evidence changes, and writes
//! its findings back into the config file as comments.
//!
//! - [`registry`]: the developer-maintained list of known backbones and
//!   brands (`backbones.toml`), used only to name groups.
//! - [`probe`]: reads each server's article-number ranges and quirks.
//! - [`detect`]: pure decision logic over that evidence.
//! - [`annotate`]: edits the config text, touching only its own comments.
//! - [`monitor`]: the background task tying them together.

mod annotate;
mod detect;
mod monitor;
mod probe;
mod registry;

pub use monitor::Detector;
pub(crate) use monitor::{candidate_identities, spawn};
pub(crate) use registry::Registry;
