//! Sandbox-pool state and the operations the UI drives.
//!
//! Everything here is terminal-free: it owns the pool, the leases, and the
//! snapshot store, and returns plain data. The app layer turns that data into
//! transcript cells, which keeps pool behavior testable and the views dumb.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use excode::{
    EmptySandboxPoolProvisioner, LocalSandboxPool, LocalSandboxPoolStore, ManagedSandboxLease,
    PoolCapacity, PoolEntryState, SandboxPoolEntryView, SandboxPoolKey, SandboxPoolSnapshotStore,
    SandboxPoolSnapshotView, SnapshotRetentionPolicy,
};
use exoharness::{
    CliContainerSandboxBackend, LocalProcessSandboxBackend, ManagedSandboxBackend, SandboxCommand,
    SandboxNetworkPolicy, SandboxSpec, SnapshotId,
};

use super::args::PoolBackend;

const EXEC_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SNAPSHOTS: usize = 20;
const MAX_SNAPSHOT_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Outcome of running a command inside the sandbox.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    pub command: String,
    pub output: String,
    pub exit_code: Option<i32>,
}

/// A single row of the sandbox table, already flattened for rendering.
#[derive(Debug, Clone)]
pub struct EntryRow {
    pub index: usize,
    pub sandbox_id: String,
    pub state: PoolEntryState,
    pub dirty: bool,
    pub owner: Option<String>,
    pub idle_secs: u64,
    pub snapshot: Option<String>,
}

/// How much of the pool is in use right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Utilization {
    pub ready: usize,
    pub leased: usize,
    pub starting: usize,
    pub retiring: usize,
    pub total: usize,
    pub warm_size: usize,
    pub max_total: usize,
}

pub struct Session {
    pool: Arc<LocalSandboxPool>,
    pool_id: String,
    snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
    entries: Vec<SandboxPoolEntryView>,
    snapshots: Vec<SandboxPoolSnapshotView>,
    snapshot_selected: usize,
    active: Option<ManagedSandboxLease>,
    detached: HashMap<String, ManagedSandboxLease>,
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
        let (managed, default_workdir): (Arc<dyn ManagedSandboxBackend>, String) = match backend {
            PoolBackend::LocalProcess => (
                Arc::new(LocalProcessSandboxBackend::new()),
                std::env::current_dir()
                    .context("determining the local sandbox working directory")?
                    .to_string_lossy()
                    .into_owned(),
            ),
            PoolBackend::Docker => (
                Arc::new(CliContainerSandboxBackend::docker()),
                "/".to_string(),
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
                store.clear().await?;
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
                    mounts: Vec::new(),
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
            entries: Vec::new(),
            snapshots: Vec::new(),
            snapshot_selected: 0,
            active: None,
            detached: HashMap::new(),
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

    pub fn detached_count(&self) -> usize {
        self.detached.len()
    }

    /// Re-read pool entries and snapshots. Called on a timer by the app loop.
    pub async fn refresh(&mut self) -> Result<()> {
        self.entries = self.pool.entries().await;
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

    pub fn utilization(&self) -> Utilization {
        let count = |state: PoolEntryState| {
            self.entries
                .iter()
                .filter(|entry| entry.state == state)
                .count()
        };
        let capacity = self.pool.capacity();
        let ready = count(PoolEntryState::Ready);
        let leased = count(PoolEntryState::Leased);
        let retiring = count(PoolEntryState::Retiring);
        Utilization {
            ready,
            leased,
            retiring,
            starting: self.entries.len() - ready - leased - retiring,
            total: self.entries.len(),
            warm_size: capacity.warm_size,
            max_total: capacity.max_total,
        }
    }

    pub fn rows(&self) -> Vec<EntryRow> {
        self.entries
            .iter()
            .enumerate()
            .map(|(index, entry)| EntryRow {
                index: index + 1,
                sandbox_id: entry
                    .sandbox_id
                    .as_deref()
                    .map(short_id)
                    .unwrap_or_else(|| "starting".to_string()),
                state: entry.state,
                dirty: entry.dirty,
                owner: entry.lease_owner.clone(),
                idle_secs: entry.idle_for.as_secs(),
                snapshot: entry.snapshot_id.map(|id| self.snapshot_label(id)),
            })
            .collect()
    }

    fn snapshot_label(&self, snapshot_id: SnapshotId) -> String {
        self.snapshots
            .iter()
            .position(|snapshot| snapshot.snapshot_id == snapshot_id)
            .map(|index| (index + 1).to_string())
            .unwrap_or_else(|| short_id(&snapshot_id.to_string()))
    }

    /// Lease any warm sandbox. Returns the sandbox id that was attached.
    pub async fn acquire(&mut self) -> Result<String> {
        if self.active.is_some() {
            bail!("detach the active sandbox before acquiring another");
        }
        let lease = self.pool.acquire_any("cli".to_string()).await?;
        let id = lease.sandbox.id().to_string();
        self.dirty = false;
        self.active = Some(lease);
        self.refresh().await?;
        Ok(id)
    }

    /// Stop using the lease without releasing it; it keeps being heartbeated.
    pub fn detach(&mut self) -> Result<String> {
        let Some(lease) = self.active.take() else {
            bail!("no sandbox is attached");
        };
        let entry_id = lease.lease.entry_id.clone();
        self.dirty = false;
        self.detached.insert(entry_id.clone(), lease);
        Ok(entry_id)
    }

    /// Release the lease, checkpointing the filesystem when supported.
    pub async fn release(&mut self) -> Result<Option<SnapshotId>> {
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

    /// Attach a sandbox restored from snapshot `number` (1-based).
    pub async fn restore(&mut self, number: Option<usize>) -> Result<String> {
        if self.active.is_some() {
            bail!("release the active sandbox before restoring a snapshot");
        }
        if self.snapshot_store.is_none() {
            let id = self.acquire().await?;
            return Ok(format!("recipe baseline restored on {id}"));
        }
        let index = number.unwrap_or(self.snapshot_selected + 1);
        if index == 0 || index > self.snapshots.len() {
            bail!(
                "snapshot {index} does not exist; choose 1-{}",
                self.snapshots.len()
            );
        }
        self.snapshot_selected = index - 1;
        let snapshot = &self.snapshots[self.snapshot_selected];
        let snapshot_id = snapshot.snapshot_id;
        let owner_id = snapshot.owner_id.clone();
        let lease = self
            .pool
            .acquire_any_from_snapshot(owner_id, snapshot_id)
            .await?;
        self.dirty = false;
        self.active = Some(lease);
        self.refresh().await?;
        Ok(format!("restored snapshot {snapshot_id}"))
    }

    /// Run `command` in the attached sandbox through a login shell.
    pub async fn exec(&mut self, command: String) -> Result<ExecOutput> {
        let Some(active) = &self.active else {
            bail!("no sandbox is attached; run /acquire first");
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
        let mut leases = self
            .detached
            .values()
            .map(|lease| lease.lease.clone())
            .collect::<Vec<_>>();
        if let Some(active) = &self.active {
            leases.push(active.lease.clone());
        }
        let mut failures = Vec::new();
        for lease in leases {
            let Err(error) = self.pool.heartbeat(&lease).await else {
                continue;
            };
            failures.push(format!("lease heartbeat failed: {error:#}"));
            self.detached.remove(&lease.entry_id);
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
