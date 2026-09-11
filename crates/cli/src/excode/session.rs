//! Sandbox-pool state and the operations the UI drives.
//!
//! Everything here is terminal-free: it owns the pool, the leases, and the
//! snapshot store, and returns plain data. The app layer turns that data into
//! transcript cells, which keeps pool behavior testable and the views dumb.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use excode::{
    EmptySandboxPoolProvisioner, LocalSandboxPool, LocalSandboxPoolStore, ManagedSandboxLease,
    PoolCapacity, SandboxPoolKey, SandboxPoolSnapshotStore, SandboxPoolSnapshotView,
    SnapshotRetentionPolicy,
};
use exoharness::{
    CliContainerSandboxBackend, LocalProcessSandboxBackend, ManagedSandboxBackend, SandboxCommand,
    SandboxMount, SandboxMountAccess, SandboxNetworkPolicy, SandboxSpec, SnapshotId,
};

use super::args::PoolBackend;

const EXEC_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SNAPSHOTS: usize = 20;
const MAX_SNAPSHOT_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub(crate) const RECIPE_WORKDIR: &str = "/workspace/exo";

/// Outcome of running a command inside the sandbox.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    pub command: String,
    pub output: String,
    pub exit_code: Option<i32>,
}

/// An owned snapshot connection operation that can run independently of the
/// UI task while a progress surface remains interactive.
pub struct PendingConnect {
    pool: Arc<LocalSandboxPool>,
    target: ConnectTarget,
}

enum ConnectTarget {
    Baseline,
    Snapshot {
        owner_id: String,
        snapshot_id: SnapshotId,
        selected: usize,
    },
}

pub struct ConnectedLease {
    pub lease: ManagedSandboxLease,
    message: String,
    selected: Option<usize>,
}

impl PendingConnect {
    pub async fn run(self) -> Result<ConnectedLease> {
        let (lease, message, selected) = match self.target {
            ConnectTarget::Baseline => {
                let lease = self.pool.acquire_any("cli").await?;
                let id = lease.sandbox.id().to_string();
                (lease, format!("connected to recipe baseline on {id}"), None)
            }
            ConnectTarget::Snapshot {
                owner_id,
                snapshot_id,
                selected,
            } => {
                let lease = self
                    .pool
                    .acquire_any_from_snapshot(owner_id, snapshot_id)
                    .await?;
                (
                    lease,
                    format!("connected to snapshot {snapshot_id}"),
                    Some(selected),
                )
            }
        };
        Ok(ConnectedLease {
            lease,
            message,
            selected,
        })
    }
}

pub struct Session {
    pool: Arc<LocalSandboxPool>,
    pool_id: String,
    snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
    snapshots: Vec<SandboxPoolSnapshotView>,
    snapshot_selected: usize,
    active: Option<ManagedSandboxLease>,
    dirty: bool,
}

impl Session {
    /// Build the pool and its snapshot store for `backend`.
    pub async fn start(
        root: &Path,
        backend: PoolBackend,
        image: String,
        warm_size: usize,
        max_total: usize,
    ) -> Result<Self> {
        if warm_size == 0 {
            bail!("--workers must be positive");
        }
        if max_total < warm_size {
            bail!("--max-workers must be at least --workers");
        }
        let (managed, default_workdir, mounts): (
            Arc<dyn ManagedSandboxBackend>,
            String,
            Vec<SandboxMount>,
        ) = match backend {
            PoolBackend::LocalProcess => (
                Arc::new(LocalProcessSandboxBackend::new()),
                std::env::current_dir()
                    .context("determining the local sandbox working directory")?
                    .to_string_lossy()
                    .into_owned(),
                Vec::new(),
            ),
            PoolBackend::Docker => (
                Arc::new(CliContainerSandboxBackend::docker()),
                RECIPE_WORKDIR.to_string(),
                vec![SandboxMount {
                    host_path: std::env::current_dir()
                        .context("determining the Docker workspace mount")?,
                    guest_path: RECIPE_WORKDIR.to_string(),
                    access: SandboxMountAccess::ReadWrite,
                    internal: false,
                }],
            ),
        };
        // Only container backends can checkpoint a filesystem, so the local
        // backend runs without a snapshot store at all.
        let snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>> = match backend {
            PoolBackend::LocalProcess => None,
            PoolBackend::Docker => {
                let store = Arc::new(LocalSandboxPoolStore::new(
                    root.join("sandbox-pool/snapshots"),
                    SnapshotRetentionPolicy {
                        max_snapshots: Some(MAX_SNAPSHOTS),
                        max_bytes: Some(MAX_SNAPSHOT_BYTES),
                        ..Default::default()
                    },
                ));
                Some(store)
            }
        };
        let pool_id = match backend {
            PoolBackend::LocalProcess => "cli-local-process",
            PoolBackend::Docker => "cli-docker",
        };
        let pool = Arc::new(LocalSandboxPool::new(
            SandboxPoolKey {
                pool_id: pool_id.to_string(),
                recipe_id: "empty".to_string(),
                spec: SandboxSpec {
                    image,
                    resources: Default::default(),
                    mounts,
                    durable_file_systems: Vec::new(),
                    network: SandboxNetworkPolicy::Enabled,
                    default_workdir,
                },
            },
            managed,
            PoolCapacity {
                warm_size,
                max_total,
                lease_ttl: Duration::from_secs(300),
                idle_ttl: Duration::from_secs(600),
            },
            Arc::new(EmptySandboxPoolProvisioner),
            snapshot_store.clone(),
        )?);
        pool.reconcile_once()
            .await
            .context("creating initial sandbox pool entries")?;
        Ok(Self {
            pool,
            pool_id: pool_id.to_string(),
            snapshot_store,
            snapshots: Vec::new(),
            snapshot_selected: 0,
            active: None,
            dirty: false,
        })
    }

    pub fn pool(&self) -> Arc<LocalSandboxPool> {
        Arc::clone(&self.pool)
    }

    pub fn active(&self) -> Option<&ManagedSandboxLease> {
        self.active.as_ref()
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn has_snapshot_store(&self) -> bool {
        self.snapshot_store.is_some()
    }

    pub fn snapshots(&self) -> &[SandboxPoolSnapshotView] {
        &self.snapshots
    }

    /// Re-read pool entries and snapshots. Called on a timer by the app loop.
    pub async fn refresh(&mut self) -> Result<()> {
        let Some(store) = &self.snapshot_store else {
            self.snapshots.clear();
            self.snapshot_selected = 0;
            return Ok(());
        };
        let mut snapshots = store
            .list(&self.pool_id, &self.pool.baseline_owner_id())
            .await?;
        snapshots.extend(store.list(&self.pool_id, "cli").await?);
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.last_accessed_at_ms));
        self.snapshot_selected = self
            .snapshot_selected
            .min(snapshots.len().saturating_sub(1));
        self.snapshots = snapshots;
        Ok(())
    }

    /// Connect to a snapshot, creating the recipe baseline on first use.
    pub fn prepare_connect(&self, number: Option<usize>) -> Result<PendingConnect> {
        if self.active.is_some() {
            bail!("disconnect from the current snapshot before connecting to another");
        }
        if self.snapshot_store.is_none() || self.snapshots.is_empty() {
            return Ok(PendingConnect {
                pool: Arc::clone(&self.pool),
                target: ConnectTarget::Baseline,
            });
        }

        let index = number.unwrap_or(self.snapshot_selected + 1);
        if index == 0 || index > self.snapshots.len() {
            bail!(
                "snapshot {index} does not exist; choose 1-{}",
                self.snapshots.len()
            );
        }
        let selected = index - 1;
        let snapshot = &self.snapshots[selected];
        let snapshot_id = snapshot.snapshot_id;
        let owner_id = snapshot.owner_id.clone();
        Ok(PendingConnect {
            pool: Arc::clone(&self.pool),
            target: ConnectTarget::Snapshot {
                owner_id,
                snapshot_id,
                selected,
            },
        })
    }

    pub async fn finish_connect(&mut self, connected: ConnectedLease) -> Result<String> {
        if let Some(selected) = connected.selected {
            self.snapshot_selected = selected;
        }
        let message = connected.message;
        self.active = Some(connected.lease);
        self.dirty = false;
        self.refresh().await?;
        Ok(message)
    }

    /// Disconnect from the current snapshot, checkpointing changes when supported.
    pub async fn disconnect(&mut self) -> Result<Option<SnapshotId>> {
        let Some(active) = self.active.take() else {
            bail!("no sandbox is attached");
        };
        let checkpoint = self.pool.release(&active.lease).await?;
        if let Some(store) = &self.snapshot_store {
            store.prune().await?;
        }
        self.dirty = false;
        self.refresh().await?;
        Ok(checkpoint)
    }

    /// Run `command` in the attached sandbox through a login shell.
    pub async fn exec(&mut self, command: String) -> Result<ExecOutput> {
        let Some(active) = &self.active else {
            bail!("no snapshot is connected; run /c first");
        };
        let result = active
            .sandbox
            .exec(&SandboxCommand {
                argv: vec!["/bin/sh".to_string(), "-lc".to_string(), command.clone()],
                env: Default::default(),
                display_argv: Some(vec![command.clone()]),
                cwd: None,
                timeout: Some(EXEC_TIMEOUT),
            })
            .await?;
        self.dirty = true;
        let mut output = result.stdout.trim_end().to_string();
        let stderr = result.stderr.trim_end();
        if !stderr.is_empty() {
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(stderr);
        }
        Ok(ExecOutput {
            command,
            output,
            exit_code: result.exit_code,
        })
    }

    /// Unified diff of the sandbox working tree, including untracked files.
    pub async fn diff(&mut self) -> Result<String> {
        let output = self
            .exec("git add -N . >/dev/null 2>&1; git --no-pager diff".to_string())
            .await?;
        Ok(output.output)
    }

    /// Keep every lease we hold alive; dropped leases are forgotten.
    pub async fn heartbeat(&mut self) -> Vec<String> {
        let leases = self
            .active
            .as_ref()
            .map(|lease| vec![lease.lease.clone()])
            .unwrap_or_default();
        let mut failures = Vec::new();
        for lease in leases {
            let Err(error) = self.pool.heartbeat(&lease).await else {
                continue;
            };
            failures.push(format!("lease heartbeat failed: {error:#}"));
            if self
                .active
                .as_ref()
                .is_some_and(|active| active.lease.entry_id == lease.entry_id)
            {
                self.active = None;
                self.dirty = false;
            }
        }
        failures
    }
}

/// Trim backend prefixes and long hashes so ids fit a table column.
pub fn short_id(id: &str) -> String {
    id.strip_prefix("local:")
        .or_else(|| id.strip_prefix("docker:"))
        .unwrap_or(id)
        .chars()
        .take(16)
        .collect()
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
