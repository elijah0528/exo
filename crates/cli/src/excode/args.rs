//! CLI surface for the excode terminal.

use clap::{Args, ValueEnum};
use exoharness::default_docker_image;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum PoolBackend {
    LocalProcess,
    Docker,
}

#[derive(Debug, Clone, Args)]
pub struct ExcodeArgs {
    /// Provider used for pool runtimes.
    #[arg(long, value_enum, default_value_t = PoolBackend::LocalProcess)]
    pub backend: PoolBackend,
    /// Number of warm entries to maintain.
    #[arg(long, default_value_t = 2)]
    pub workers: usize,
    /// Maximum total entries, including leased entries. Defaults to --workers.
    #[arg(long)]
    pub max_workers: Option<usize>,
    /// Model used by the interactive coding agent. With `--harness`, this must
    /// name a registered model binding (see `exo model register`); when unset,
    /// the first registered binding is used.
    #[arg(long)]
    pub model: Option<String>,
    /// Container image used by the Docker backend.
    #[arg(long, default_value_t = default_docker_image())]
    pub image: String,
    /// Run chat turns through an exoharness-backed harness instead of the
    /// built-in pool agent: `codex`, `claude-code`, `cursor`, `pi`, or a
    /// TypeScript harness module path.
    #[arg(long, value_name = "HARNESS")]
    pub harness: Option<String>,
}

impl ExcodeArgs {
    pub fn max_total(&self) -> usize {
        self.max_workers.unwrap_or(self.workers)
    }
}
