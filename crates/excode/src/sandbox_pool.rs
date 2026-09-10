//! A single-process pool of interchangeable, disposable sandbox runtimes.
//!
//! Start one reconciler alongside callers:
//! ```ignore
//! let pool = Arc::new(LocalSandboxPool::new(
//!     key,
//!     backend,
//!     capacity,
//!     Arc::new(EmptySandboxPoolProvisioner),
//!     None,
//! )?);
//! let (shutdown, receiver) = tokio::sync::watch::channel(false);
//! let task = tokio::spawn({
//!     let pool = Arc::clone(&pool);
//!     async move { pool.run_reconciler(receiver).await }
//! });
//! let acquired = tokio::time::timeout(
//!     Duration::from_secs(150), pool.acquire_any("conversation:123"),
//! ).await??;
//! let output = acquired.sandbox.exec(&command).await?;
//! pool.heartbeat(&acquired.lease).await?;
//! // Release resets the runtime to its recipe baseline before making it
//! // available to another worker.
//! pool.release(&acquired.lease).await?;
//! shutdown.send(true)?;
//! task.await?;
//! ```
//!
//! Shutdown stops replenishment and rejects new acquisitions; it does not destroy
//! outstanding leases or warm runtimes. Release owned leases before shutdown.
//! Dropped leases expire and are retired by reconciliation. This API does not
//! persist pool ownership across process restarts.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, ensure};
use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, RwLock, watch};
use tokio::time::{self, MissedTickBehavior};

use exoharness::{
    ManagedSandboxBackend, ManagedSandboxHandle, Result, SandboxCommand, SandboxRequest,
    SandboxSpec, SnapshotId, SnapshotPayload, Uuid7,
};

use crate::SandboxPoolSnapshotStore;

/// The immutable sandbox configuration shared by entries in one pool.
///
/// A pool can only hand out entries that are interchangeable for the request
/// represented by this key. Pool policy such as capacity and lease duration is
/// kept separately in [`PoolCapacity`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SandboxPoolKey {
    pub pool_id: String,
    /// Stable identity of the recipe that produced the pool baseline.
    pub recipe_id: String,
    pub spec: SandboxSpec,
}

/// Identity of one runtime entry owned by a [`LocalSandboxPool`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolSandboxKey {
    pub pool_id: String,
    pub entry_id: String,
}

impl From<PoolSandboxKey> for exoharness::SandboxKey {
    fn from(key: PoolSandboxKey) -> Self {
        exoharness::SandboxKey::AgentSandbox {
            agent_id: format!("__excode_pool__:{}", key.pool_id),
            sandbox_id: key.entry_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolCapacity {
    pub warm_size: usize,
    pub max_total: usize,
    pub lease_ttl: Duration,
    pub idle_ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct PoolPolicy {
    pub provider_timeout: Duration,
    pub health_check_command: Vec<String>,
    pub health_check_timeout: Duration,
    pub health_check_interval: Duration,
    pub reconcile_interval: Duration,
    pub command_timeout: Duration,
}

impl Default for PoolPolicy {
    fn default() -> Self {
        Self {
            provider_timeout: Duration::from_secs(120),
            health_check_command: vec!["true".to_string()],
            health_check_timeout: Duration::from_secs(5),
            health_check_interval: Duration::from_secs(30),
            reconcile_interval: Duration::from_secs(10),
            command_timeout: Duration::from_secs(300),
        }
    }
}

/// Materializes a clean runtime for a newly-created pool entry.
///
/// A recipe owns snapshot resolution, credentials, setup steps, and cleanup
/// when materialization fails. The pool owns only capacity and lease lifecycle.
#[async_trait]
pub trait SandboxPoolProvisioner: Send + Sync {
    async fn acquire(
        &self,
        backend: &dyn ManagedSandboxBackend,
        request: SandboxRequest,
    ) -> Result<Arc<dyn ManagedSandboxHandle>>;
}

/// A recipe that provisions an empty runtime from the configured provider.
pub struct EmptySandboxPoolProvisioner;

#[async_trait]
impl SandboxPoolProvisioner for EmptySandboxPoolProvisioner {
    async fn acquire(
        &self,
        backend: &dyn ManagedSandboxBackend,
        request: SandboxRequest,
    ) -> Result<Arc<dyn ManagedSandboxHandle>> {
        backend.acquire(request).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolEntryState {
    Creating,
    Checking,
    Ready,
    Leased,
    /// The filesystem is being checkpointed/reset and must not be leased.
    Resetting,
    /// The entry must not be leased and will be destroyed by reconciliation.
    Retiring,
}

enum ReconcileTask {
    HealthCheck(String),
    Retire(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxLease {
    pub entry_id: String,
    pub worker_id: String,
    pub fencing_token: String,
    pub expires_at: Instant,
}

#[derive(Debug, Clone)]
pub struct SandboxPoolEntryView {
    pub entry_id: String,
    pub sandbox_id: Option<String>,
    pub state: PoolEntryState,
    pub lease_owner: Option<String>,
    pub idle_for: Duration,
    pub dirty: bool,
    pub snapshot_id: Option<SnapshotId>,
}

struct PoolEntry {
    id: String,
    request: SandboxRequest,
    /// Live provider access; absent until creation succeeds.
    handle: Option<Arc<dyn ManagedSandboxHandle>>,
    state: PoolEntryState,
    lease: Option<SandboxLease>,
    last_used_at: Instant,
    dirty: bool,
    snapshot_id: Option<SnapshotId>,
    last_health_check_at: Option<Instant>,
    lifecycle: Arc<RwLock<()>>,
}

impl PoolEntry {
    fn reconciliation_task(
        &mut self,
        now: Instant,
        health_check_interval: Duration,
    ) -> Option<ReconcileTask> {
        if self.state == PoolEntryState::Leased
            && self
                .lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at <= now)
        {
            self.state = PoolEntryState::Retiring;
        }
        match self.state {
            PoolEntryState::Ready
                if self
                    .last_health_check_at
                    .is_none_or(|last| now.duration_since(last) >= health_check_interval) =>
            {
                Some(ReconcileTask::HealthCheck(self.id.clone()))
            }
            PoolEntryState::Retiring => Some(ReconcileTask::Retire(self.id.clone())),
            _ => None,
        }
    }
}

/// Owns warm runtime capacity and fences access through leases.
pub struct LocalSandboxPool {
    key: SandboxPoolKey,
    backend: Arc<dyn ManagedSandboxBackend>,
    provisioner: Arc<dyn SandboxPoolProvisioner>,
    snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
    /// Every entry of a pool is materialized from this one baseline snapshot,
    /// so entries handed out by the pool are interchangeable. It is written
    /// once when the first entry is created and re-written only when the
    /// stored baseline turns out to be unusable.
    baseline_snapshot: RwLock<Option<SnapshotId>>,
    policy: PoolPolicy,
    entries: Arc<Mutex<HashMap<String, PoolEntry>>>,
    capacity: PoolCapacity,
    notify: Arc<Notify>,
    changed: Arc<Notify>,
    reconcile: Mutex<()>,
    closed: AtomicBool,
}

pub const RECIPE_BASELINE_OWNER: &str = "__recipe_baseline__";

/// Operations available through an active pool lease.
#[async_trait]
pub trait ManagedSandboxCapability: Send + Sync {
    fn id(&self) -> &str;
    async fn exec(&self, command: &SandboxCommand) -> Result<exoharness::SandboxCommandOutput>;
}

/// A lease and its fenced sandbox capability.
pub struct ManagedSandboxLease {
    pub lease: SandboxLease,
    pub sandbox: Arc<dyn ManagedSandboxCapability>,
}

/// Common lifecycle operations for sandbox-pool implementations.
///
/// Acquisitions return a fenced capability rather than a raw provider handle,
/// so every implementation preserves lease validity during sandbox access.
#[async_trait]
pub trait ManagedSandboxPool: Send + Sync {
    async fn acquire_any(&self, worker_id: String) -> Result<ManagedSandboxLease>;
    async fn heartbeat(&self, lease: &SandboxLease) -> Result<()>;
    async fn release(&self, lease: &SandboxLease) -> Result<Option<SnapshotId>>;
    async fn reset(&self, lease: &SandboxLease) -> Result<()>;
    async fn drain(&self) -> Result<()>;
}

/// Kubernetes-backed pool placeholder. The Kubernetes controller and API
/// integration will be added without changing [`ManagedSandboxPool`].
pub struct KubernetesSandboxPool;

#[async_trait]
impl ManagedSandboxPool for KubernetesSandboxPool {
    async fn acquire_any(&self, _worker_id: String) -> Result<ManagedSandboxLease> {
        bail!("KubernetesSandboxPool is not implemented")
    }

    async fn heartbeat(&self, _lease: &SandboxLease) -> Result<()> {
        bail!("KubernetesSandboxPool is not implemented")
    }

    async fn release(&self, _lease: &SandboxLease) -> Result<Option<SnapshotId>> {
        bail!("KubernetesSandboxPool is not implemented")
    }

    async fn reset(&self, _lease: &SandboxLease) -> Result<()> {
        bail!("KubernetesSandboxPool is not implemented")
    }

    async fn drain(&self) -> Result<()> {
        bail!("KubernetesSandboxPool is not implemented")
    }
}

#[cfg(test)]
impl PoolEntry {
    fn new(
        id: String,
        request: SandboxRequest,
        handle: Option<Arc<dyn ManagedSandboxHandle>>,
    ) -> Self {
        Self {
            id,
            request,
            handle,
            state: PoolEntryState::Ready,
            lease: None,
            last_used_at: Instant::now(),
            dirty: false,
            snapshot_id: None,
            last_health_check_at: None,
            lifecycle: Arc::new(RwLock::new(())),
        }
    }
}

impl LocalSandboxPool {
    pub fn new(
        key: SandboxPoolKey,
        backend: Arc<dyn ManagedSandboxBackend>,
        capacity: PoolCapacity,
        provisioner: Arc<dyn SandboxPoolProvisioner>,
        snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
    ) -> Result<Self> {
        Self::new_with_policy(
            key,
            backend,
            capacity,
            provisioner,
            snapshot_store,
            PoolPolicy::default(),
        )
    }

    pub fn new_with_policy(
        key: SandboxPoolKey,
        backend: Arc<dyn ManagedSandboxBackend>,
        capacity: PoolCapacity,
        provisioner: Arc<dyn SandboxPoolProvisioner>,
        snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
        policy: PoolPolicy,
    ) -> Result<Self> {
        validate_key(&key)?;
        validate_capacity(&capacity)?;
        validate_policy(&policy)?;
        Ok(Self {
            key,
            backend,
            provisioner,
            snapshot_store,
            baseline_snapshot: RwLock::new(None),
            policy,
            entries: Arc::new(Mutex::new(HashMap::new())),
            capacity,
            notify: Arc::new(Notify::new()),
            changed: Arc::new(Notify::new()),
            reconcile: Mutex::new(()),
            closed: AtomicBool::new(false),
        })
    }

    pub async fn entry_count(&self) -> usize {
        self.entries.lock().await.len()
    }

    pub fn capacity(&self) -> PoolCapacity {
        self.capacity
    }

    pub fn baseline_owner_id(&self) -> String {
        format!("{}:{}", RECIPE_BASELINE_OWNER, self.key.recipe_id)
    }

    pub async fn entries(&self) -> Vec<SandboxPoolEntryView> {
        let now = Instant::now();
        let entries = self.entries.lock().await;
        let mut views = entries
            .values()
            .map(|entry| SandboxPoolEntryView {
                entry_id: entry.id.clone(),
                sandbox_id: entry.handle.as_ref().map(|handle| handle.id().to_string()),
                state: entry.state,
                lease_owner: entry.lease.as_ref().map(|lease| lease.worker_id.clone()),
                idle_for: now.saturating_duration_since(entry.last_used_at),
                dirty: entry.dirty,
                snapshot_id: entry.snapshot_id,
            })
            .collect::<Vec<_>>();
        views.sort_by(|left, right| left.entry_id.cmp(&right.entry_id));
        views
    }

    #[cfg(test)]
    async fn insert_entry(&self, entry: PoolEntry) -> Result<()> {
        let mut entries = self.entries.lock().await;
        if entries.contains_key(&entry.id) {
            bail!("sandbox pool entry already exists: {}", entry.id);
        }
        entries.insert(entry.id.clone(), entry);
        self.notify.notify_one();
        self.changed.notify_waiters();
        Ok(())
    }

    /// Lease a ready sandbox. If the entry has no live handle, acquire one
    /// from the provider using the request persisted on the entry.
    async fn try_acquire_with_snapshot(
        &self,
        worker_id: impl Into<String>,
        snapshot: Option<SnapshotPayload>,
        snapshot_id: Option<SnapshotId>,
        requested_entry_id: Option<&str>,
    ) -> Result<(SandboxLease, LeasedSandbox)> {
        if self.closed.load(Ordering::Acquire) {
            bail!("sandbox pool is closed");
        }
        let worker_id = worker_id.into();
        let (entry_id, lease, request, live_handle, lifecycle) = {
            let mut entries = self.entries.lock().await;
            let entry = entries
                .values_mut()
                .filter(|entry| {
                    entry.state == PoolEntryState::Ready
                        && requested_entry_id.is_none_or(|id| entry.id == id)
                })
                .min_by_key(|entry| entry.last_used_at)
                .ok_or_else(|| anyhow::Error::new(NoReadyCapacity))?;
            // Create lease and mark in entry table
            let lease = SandboxLease {
                entry_id: entry.id.clone(),
                worker_id,
                fencing_token: Uuid7::now().to_string(),
                expires_at: Instant::now() + self.capacity.lease_ttl,
            };
            entry.state = PoolEntryState::Leased;
            entry.lease = Some(lease.clone());
            entry.last_used_at = Instant::now();
            (
                entry.id.clone(),
                lease,
                entry.request.clone(),
                entry.handle.clone(),
                Arc::clone(&entry.lifecycle),
            )
        };

        let _operation = lifecycle.read().await;
        let handle = match self.prepare_handle(request, live_handle, snapshot).await {
            Ok(handle) => handle,
            Err(error) => {
                self.mark_retiring(&entry_id, &lease).await;
                return Err(error);
            }
        };

        let mut entries = self.entries.lock().await;
        let entry = entry_mut(&mut entries, &entry_id)?;
        if validate_lease(entry, &lease).is_err() {
            // A concurrent release/reset invalidated this acquisition. Do not
            // return a handle whose ownership is no longer represented by the
            // pool.
            drop(entries);
            handle
                .stop()
                .await
                .map_err(|error| anyhow!("failed stopping invalidated sandbox lease: {error}"))?;
            bail!("sandbox lease was invalidated while acquiring capacity");
        }
        // Record the live provider handle for this pool entry.
        entry.handle = Some(Arc::clone(&handle));
        if let Some(snapshot_id) = snapshot_id {
            entry.snapshot_id = Some(snapshot_id);
            entry.dirty = false;
        }
        self.notify.notify_one();
        self.changed.notify_waiters();
        let leased = LeasedSandbox {
            lease: lease.clone(),
            handle,
            command_timeout: self.policy.command_timeout,
            entries: Arc::clone(&self.entries),
            lifecycle: Arc::clone(&lifecycle),
            notify: Arc::clone(&self.notify),
            changed: Arc::clone(&self.changed),
        };
        Ok((lease, leased))
    }

    /// Wait for clean capacity. Run `run_reconciler` concurrently.
    /// Dropping this future cancels the wait; callers can use tokio::time::timeout.
    pub async fn acquire_any(&self, worker_id: impl Into<String>) -> Result<ManagedSandboxLease> {
        self.acquire(worker_id.into(), None, None, None).await
    }

    /// Acquire a ready runtime restored from an owner-scoped checkpoint.
    /// The snapshot store is responsible for enforcing ownership and
    /// retaining the checkpoint durably.
    pub async fn acquire_any_from_snapshot(
        &self,
        owner_id: impl Into<String>,
        snapshot_id: SnapshotId,
    ) -> Result<ManagedSandboxLease> {
        let owner_id = owner_id.into();
        let store = self
            .snapshot_store
            .as_ref()
            .ok_or_else(|| anyhow!("sandbox pool has no snapshot store"))?;
        let snapshot = store
            .load(&self.key.pool_id, &owner_id, snapshot_id)
            .await?;
        self.acquire(owner_id, Some(snapshot), Some(snapshot_id), None)
            .await
    }

    pub async fn acquire_entry(
        &self,
        entry_id: impl Into<String>,
        worker_id: impl Into<String>,
    ) -> Result<ManagedSandboxLease> {
        self.acquire(worker_id.into(), None, None, Some(entry_id.into()))
            .await
    }

    async fn acquire(
        &self,
        worker_id: String,
        snapshot: Option<SnapshotPayload>,
        snapshot_id: Option<SnapshotId>,
        requested_entry_id: Option<String>,
    ) -> Result<ManagedSandboxLease> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self
                .try_acquire_with_snapshot(
                    worker_id.clone(),
                    snapshot.clone(),
                    snapshot_id,
                    requested_entry_id.as_deref(),
                )
                .await
            {
                Ok((lease, sandbox)) => {
                    return Ok(ManagedSandboxLease {
                        lease,
                        sandbox: Arc::new(sandbox),
                    });
                }
                Err(error) if error.is::<NoReadyCapacity>() => {}
                Err(error) => return Err(error),
            }
            self.notify.notify_one();
            changed.await;
        }
    }

    #[cfg(test)]
    async fn try_acquire(
        &self,
        worker_id: impl Into<String>,
    ) -> Result<(SandboxLease, LeasedSandbox)> {
        self.try_acquire_with_snapshot(worker_id, None, None, None)
            .await
    }

    pub async fn heartbeat(&self, lease: &SandboxLease) -> Result<()> {
        let mut entries = self.entries.lock().await;
        let entry = leased_entry_mut(&mut entries, lease)?;
        let current_lease = entry
            .lease
            .as_mut()
            .ok_or_else(|| anyhow!("sandbox lease disappeared during heartbeat"))?;
        current_lease.expires_at = Instant::now() + self.capacity.lease_ttl;
        Ok(())
    }

    /// Rebuild the entry from its recipe baseline before reuse.
    /// Use [`Self::reset`] to remove the entry without replenishing it.
    // This operation should remain behind the pool manager's authorization
    // boundary when the pool is exposed to remote workers.
    pub async fn release(&self, lease: &SandboxLease) -> Result<Option<SnapshotId>> {
        let lifecycle = self.entry_lifecycle(&lease.entry_id).await?;
        let _operation = lifecycle.write().await;
        let (entry_id, request, handle, dirty) = {
            let mut entries = self.entries.lock().await;
            let entry = leased_entry_mut(&mut entries, lease)?;
            let handle = entry.handle.clone().ok_or_else(|| {
                anyhow!("sandbox lease is still acquiring: {}", lease.fencing_token)
            })?;
            entry.state = PoolEntryState::Resetting;
            entry.lease = None;
            (entry.id.clone(), entry.request.clone(), handle, entry.dirty)
        };

        let result = async {
            let snapshot_id = if dirty {
                self.checkpoint(&lease.worker_id, handle).await?
            } else {
                None
            };
            self.reset_runtime(&entry_id, request).await?;
            Ok::<Option<SnapshotId>, anyhow::Error>(snapshot_id)
        }
        .await;
        let snapshot_id = match result {
            Ok(snapshot_id) => snapshot_id,
            Err(error) => {
                self.quarantine(&entry_id).await;
                return Err(error);
            }
        };

        self.notify.notify_one();
        self.changed.notify_waiters();
        Ok(snapshot_id)
    }

    // This operation should remain behind the pool manager's authorization
    // boundary when the pool is exposed to remote workers.
    pub async fn reset(&self, lease: &SandboxLease) -> Result<()> {
        let lifecycle = self.entry_lifecycle(&lease.entry_id).await?;
        let _operation = lifecycle.write().await;
        let (entry_id, request) = {
            let mut entries = self.entries.lock().await;
            let entry = leased_entry_mut(&mut entries, lease)?;
            entry.state = PoolEntryState::Retiring;
            (entry.id.clone(), entry.request.clone())
        };

        if let Err(error) = self.terminate_with_provider(request).await {
            self.quarantine(&entry_id).await;
            return Err(error);
        }

        self.entries.lock().await.remove(&entry_id);
        self.notify.notify_one();
        self.changed.notify_waiters();
        Ok(())
    }

    /// Stop every runtime in the pool and reject subsequent acquisitions.
    /// Call this after durable workspace state is saved and active users have
    /// released their leases.
    pub async fn drain(&self) -> Result<()> {
        let _reconcile = self.reconcile.lock().await;
        self.closed.store(true, Ordering::Release);
        let entry_ids = {
            let mut entries = self.entries.lock().await;
            entries
                .values_mut()
                .map(|entry| {
                    entry.state = PoolEntryState::Retiring;
                    entry.lease = None;
                    entry.id.clone()
                })
                .collect::<Vec<_>>()
        };
        self.changed.notify_waiters();

        for entry_id in entry_ids {
            self.retire_entry(&entry_id).await?;
        }
        Ok(())
    }

    async fn health_check(&self, entry_id: &str) -> Result<()> {
        let lifecycle = self.entry_lifecycle(entry_id).await?;
        let _operation = lifecycle.write().await;
        let handle = {
            let mut entries = self.entries.lock().await;
            let entry = entry_mut(&mut entries, entry_id)?;
            if entry.state != PoolEntryState::Ready {
                bail!("sandbox pool entry is not ready: {entry_id}");
            }
            let handle = entry
                .handle
                .clone()
                .ok_or_else(|| anyhow!("sandbox pool entry has no live handle: {entry_id}"))?;
            entry.state = PoolEntryState::Checking;
            handle
        };

        let command = SandboxCommand {
            argv: self.policy.health_check_command.clone(),
            env: HashMap::new(),
            display_argv: Some(self.policy.health_check_command.clone()),
            cwd: None,
            timeout: Some(self.policy.health_check_timeout),
        };
        let result = time::timeout(self.policy.health_check_timeout, handle.exec(&command))
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result)
            .and_then(|output| {
                if output.ok {
                    Ok(())
                } else {
                    bail!("health command failed")
                }
            });
        if let Err(error) = result {
            let mut entries = self.entries.lock().await;
            if let Some(entry) = entries.get_mut(entry_id) {
                entry.state = PoolEntryState::Retiring;
                entry.last_health_check_at = Some(Instant::now());
            }
            self.notify.notify_one();
            return Err(error);
        }

        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(entry_id) {
            entry.state = PoolEntryState::Ready;
            entry.last_health_check_at = Some(Instant::now());
        }
        self.changed.notify_waiters();
        Ok(())
    }

    /// Run one reconciliation pass. Provider calls never run while the entry
    /// table mutex is held.
    pub async fn reconcile_once(&self) -> Result<()> {
        let _reconcile = self.reconcile.lock().await;
        self.evict_idle().await?;
        let tasks = {
            let mut entries = self.entries.lock().await;
            let now = Instant::now();
            entries
                .values_mut()
                .filter_map(|entry| {
                    entry.reconciliation_task(now, self.policy.health_check_interval)
                })
                .collect::<Vec<_>>()
        };

        for task in tasks {
            let (result, entry_id, operation) = match task {
                ReconcileTask::HealthCheck(entry_id) => {
                    (self.health_check(&entry_id).await, entry_id, "health check")
                }
                ReconcileTask::Retire(entry_id) => {
                    (self.retire_entry(&entry_id).await, entry_id, "retirement")
                }
            };
            if let Err(error) = result {
                tracing::warn!(%error, %entry_id, operation, "sandbox pool reconciliation task failed");
            }
        }

        self.ensure_capacity().await
    }

    /// Run the event-driven pool reconciler until the shutdown watch is set.
    /// The timer is only a safety sweep; normal changes wake the loop through
    /// `Notify`.
    pub async fn run_reconciler(&self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = time::interval(self.policy.reconcile_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            if *shutdown.borrow() {
                self.closed.store(true, Ordering::Release);
                self.changed.notify_waiters();
                return;
            }
            tokio::select! {
                _ = self.notify.notified() => {
                    if let Err(error) = self.reconcile_once().await {
                        tracing::warn!(%error, "sandbox pool reconciliation failed");
                    }
                }
                _ = interval.tick() => {
                    if let Err(error) = self.reconcile_once().await {
                        tracing::warn!(%error, "sandbox pool reconciliation failed");
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.closed.store(true, Ordering::Release);
                        self.changed.notify_waiters();
                        return;
                    }
                }
            }
        }
    }

    async fn evict_idle(&self) -> Result<()> {
        if self.capacity.idle_ttl.is_zero() {
            return Ok(());
        }

        let candidates = {
            let mut entries = self.entries.lock().await;
            let now = Instant::now();
            let ready_count = entries
                .values()
                .filter(|entry| entry.state == PoolEntryState::Ready)
                .count();
            let removable_count = ready_count.saturating_sub(self.capacity.warm_size);
            let mut candidates = entries
                .values_mut()
                .filter(|entry| {
                    entry.state == PoolEntryState::Ready
                        && entry.lease.is_none()
                        && now.duration_since(entry.last_used_at) >= self.capacity.idle_ttl
                })
                .collect::<Vec<_>>();
            candidates.sort_unstable_by_key(|entry| entry.last_used_at);
            candidates
                .into_iter()
                .take(removable_count)
                .map(|entry| {
                    entry.state = PoolEntryState::Retiring;
                    entry.id.clone()
                })
                .collect::<Vec<_>>()
        };

        for entry_id in candidates {
            self.retire_entry(&entry_id).await?;
        }
        Ok(())
    }

    async fn ensure_capacity(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        loop {
            let mut entries = self.entries.lock().await;
            let ready_or_creating = entries
                .values()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        PoolEntryState::Ready | PoolEntryState::Creating
                    )
                })
                .count();
            if ready_or_creating >= self.capacity.warm_size
                || entries.len() >= self.capacity.max_total
            {
                return Ok(());
            }

            let entry_id = format!("pool-entry-{}", Uuid7::now());
            let request = self.pool_request(&entry_id);
            {
                entries.insert(
                    entry_id.clone(),
                    PoolEntry {
                        id: entry_id.clone(),
                        request: request.clone(),
                        handle: None,
                        state: PoolEntryState::Creating,
                        lease: None,
                        last_used_at: Instant::now(),
                        dirty: false,
                        snapshot_id: None,
                        last_health_check_at: None,
                        lifecycle: Arc::new(RwLock::new(())),
                    },
                );
            }

            drop(entries);
            match self.acquire_from_recipe(request).await {
                Ok(handle) => {
                    let baseline_snapshot = *self.baseline_snapshot.read().await;
                    let mut entries = self.entries.lock().await;
                    if let Some(entry) = entries.get_mut(&entry_id) {
                        entry.request.provider_state = handle.provider_state();
                        entry.handle = Some(handle);
                        entry.snapshot_id = baseline_snapshot;
                        entry.state = PoolEntryState::Ready;
                        entry.last_used_at = Instant::now();
                    }
                }
                Err(error) => {
                    let mut entries = self.entries.lock().await;
                    if let Some(entry) = entries.get_mut(&entry_id) {
                        entry.state = PoolEntryState::Retiring;
                    }
                    return Err(error);
                }
            }
            self.notify.notify_one();
            self.changed.notify_waiters();
        }
    }

    async fn entry_lifecycle(&self, entry_id: &str) -> Result<Arc<RwLock<()>>> {
        self.entries
            .lock()
            .await
            .get(entry_id)
            .map(|entry| Arc::clone(&entry.lifecycle))
            .ok_or_else(|| anyhow!("sandbox pool entry not found: {entry_id}"))
    }

    async fn retire_entry(&self, entry_id: &str) -> Result<()> {
        let lifecycle = self.entry_lifecycle(entry_id).await?;
        let _operation = lifecycle.write().await;
        let (request, has_runtime, unsaved) = {
            let mut entries = self.entries.lock().await;
            let entry = entry_mut(&mut entries, entry_id)?;
            if entry.state != PoolEntryState::Retiring {
                return Ok(());
            }
            let unsaved = entry.dirty.then(|| {
                entry
                    .lease
                    .as_ref()
                    .map(|lease| (lease.worker_id.clone(), entry.handle.clone()))
            });
            (
                entry.request.clone(),
                entry.handle.is_some() || entry.request.provider_state.is_some(),
                unsaved.flatten(),
            )
        };

        // An entry can be retired while its worker still has unsaved work, for
        // example when its lease expires. Keep that work reachable through the
        // snapshot store instead of destroying it with the runtime.
        if let Some((owner_id, Some(handle))) = unsaved
            && let Err(error) = self.checkpoint(&owner_id, handle).await
        {
            tracing::warn!(%error, %entry_id, %owner_id, "failed checkpointing sandbox before retirement");
        }

        if has_runtime && let Err(error) = self.terminate_with_provider(request).await {
            return Err(error);
        }
        self.entries.lock().await.remove(entry_id);
        self.notify.notify_one();
        self.changed.notify_waiters();
        Ok(())
    }

    fn pool_request(&self, entry_id: &str) -> SandboxRequest {
        SandboxRequest {
            key: PoolSandboxKey {
                pool_id: self.key.pool_id.clone(),
                entry_id: entry_id.to_string(),
            }
            .into(),
            spec: self.key.spec.clone(),
            lifecycle: exoharness::SandboxLifecycleConfig {
                idle_ttl: (!self.capacity.idle_ttl.is_zero()).then_some(self.capacity.idle_ttl),
            },
            provider_state: None,
        }
    }

    async fn acquire_from_recipe(
        &self,
        request: SandboxRequest,
    ) -> Result<Arc<dyn ManagedSandboxHandle>> {
        // Serialize baseline restore/fallback/save so concurrent replacements
        // cannot create competing baselines. Readers of an established
        // baseline take the read lock and never wait on each other.
        let mut baseline = self.baseline_snapshot.write().await;
        if let Some(store) = &self.snapshot_store {
            if baseline.is_none() {
                *baseline = store
                    .list(&self.key.pool_id, &self.baseline_owner_id())
                    .await?
                    .first()
                    .map(|snapshot| snapshot.snapshot_id);
            }
            let snapshot_id = *baseline;
            if let Some(snapshot_id) = snapshot_id {
                let restored = async {
                    let snapshot = store
                        .load(&self.key.pool_id, &self.baseline_owner_id(), snapshot_id)
                        .await?;
                    self.acquire_from_snapshot(request.clone(), snapshot).await
                }
                .await;
                match restored {
                    Ok(handle) => return Ok(handle),
                    Err(error) => {
                        tracing::warn!(%error, %snapshot_id, "pool baseline restore failed; recreating from recipe");
                        if let Err(delete_error) = store
                            .delete(&self.key.pool_id, &self.baseline_owner_id(), snapshot_id)
                            .await
                        {
                            tracing::warn!(%delete_error, %snapshot_id, "failed deleting unusable pool baseline");
                        }
                        *baseline = None;
                    }
                }
            }
        }
        let handle = time::timeout(
            self.policy.provider_timeout,
            self.provisioner.acquire(self.backend.as_ref(), request),
        )
        .await??;
        if let Some(store) = &self.snapshot_store {
            let saved = async {
                let payload =
                    time::timeout(self.policy.provider_timeout, handle.snapshot()).await??;
                store
                    .save(&self.key.pool_id, &self.baseline_owner_id(), payload)
                    .await
            }
            .await;
            match saved {
                Ok(snapshot_id) => *baseline = Some(snapshot_id),
                Err(error) => {
                    tracing::debug!(%error, "recipe baseline snapshot could not be saved");
                }
            }
        }
        Ok(handle)
    }

    async fn acquire_from_snapshot(
        &self,
        request: SandboxRequest,
        snapshot: SnapshotPayload,
    ) -> Result<Arc<dyn ManagedSandboxHandle>> {
        time::timeout(
            self.policy.provider_timeout,
            self.backend.acquire_from_snapshot(request, snapshot),
        )
        .await?
    }

    async fn prepare_handle(
        &self,
        request: SandboxRequest,
        live_handle: Option<Arc<dyn ManagedSandboxHandle>>,
        snapshot: Option<SnapshotPayload>,
    ) -> Result<Arc<dyn ManagedSandboxHandle>> {
        let Some(snapshot) = snapshot else {
            return match live_handle {
                Some(handle) => Ok(handle),
                None => self.acquire_from_recipe(request).await,
            };
        };
        if live_handle.is_some() {
            self.terminate_with_provider(request.clone()).await?;
        }
        self.acquire_from_snapshot(request, snapshot).await
    }

    async fn terminate_with_provider(&self, request: SandboxRequest) -> Result<()> {
        time::timeout(
            self.policy.provider_timeout,
            self.backend.terminate(request),
        )
        .await?
    }

    async fn checkpoint(
        &self,
        owner_id: &str,
        handle: Arc<dyn ManagedSandboxHandle>,
    ) -> Result<Option<SnapshotId>> {
        let Some(store) = &self.snapshot_store else {
            return Ok(None);
        };
        let payload = time::timeout(self.policy.provider_timeout, handle.snapshot())
            .await
            .map_err(anyhow::Error::from)??;
        store
            .save(&self.key.pool_id, owner_id, payload)
            .await
            .map(Some)
    }

    async fn reset_runtime(&self, entry_id: &str, request: SandboxRequest) -> Result<()> {
        self.terminate_with_provider(request.clone()).await?;
        let handle = self.acquire_from_recipe(request.clone()).await?;
        let baseline_snapshot = *self.baseline_snapshot.read().await;
        let mut entries = self.entries.lock().await;
        let entry = entry_mut(&mut entries, entry_id)?;
        entry.request.provider_state = handle.provider_state();
        entry.handle = Some(handle);
        entry.snapshot_id = baseline_snapshot;
        entry.dirty = false;
        entry.state = PoolEntryState::Ready;
        entry.last_used_at = Instant::now();
        entry.last_health_check_at = None;
        Ok(())
    }

    async fn quarantine(&self, entry_id: &str) {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(entry_id) {
            quarantine_entry(entry);
        }
        self.notify.notify_one();
        self.changed.notify_waiters();
    }

    async fn mark_retiring(&self, entry_id: &str, lease: &SandboxLease) {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(entry_id)
            && entry.lease.as_ref() == Some(lease)
        {
            entry.state = PoolEntryState::Retiring;
            entry.lease = None;
            entry.handle = None;
            self.notify.notify_one();
            self.changed.notify_waiters();
        }
    }
}

#[async_trait]
impl ManagedSandboxPool for LocalSandboxPool {
    async fn acquire_any(&self, worker_id: String) -> Result<ManagedSandboxLease> {
        LocalSandboxPool::acquire_any(self, worker_id).await
    }

    async fn heartbeat(&self, lease: &SandboxLease) -> Result<()> {
        LocalSandboxPool::heartbeat(self, lease).await
    }

    async fn release(&self, lease: &SandboxLease) -> Result<Option<SnapshotId>> {
        LocalSandboxPool::release(self, lease).await
    }

    async fn reset(&self, lease: &SandboxLease) -> Result<()> {
        LocalSandboxPool::reset(self, lease).await
    }

    async fn drain(&self) -> Result<()> {
        LocalSandboxPool::drain(self).await
    }
}

/// A capability valid only while its pool lease is active.
/// Runtime lifecycle stays with the pool. Release invalidates this capability
/// before the pool rebuilds the entry for its next lease.
struct LeasedSandbox {
    lease: SandboxLease,
    handle: Arc<dyn ManagedSandboxHandle>,
    command_timeout: Duration,
    entries: Arc<Mutex<HashMap<String, PoolEntry>>>,
    lifecycle: Arc<RwLock<()>>,
    notify: Arc<Notify>,
    changed: Arc<Notify>,
}

impl LeasedSandbox {
    fn id(&self) -> &str {
        self.handle.id()
    }

    async fn exec(&self, command: &SandboxCommand) -> Result<exoharness::SandboxCommandOutput> {
        let operation = self.lifecycle.read().await;
        {
            let entries = self.entries.lock().await;
            let entry = entries
                .get(&self.lease.entry_id)
                .ok_or_else(|| anyhow!("lease entry removed"))?;
            validate_lease(entry, &self.lease)?;
        }
        let timeout = command.timeout.unwrap_or(self.command_timeout);
        let result = time::timeout(timeout, self.handle.exec(command))
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result);
        drop(operation);

        if result.is_err() {
            self.quarantine().await;
        } else {
            let mut entries = self.entries.lock().await;
            if let Some(entry) = entries.get_mut(&self.lease.entry_id)
                && lease_matches(entry.lease.as_ref(), &self.lease)
                && entry.state == PoolEntryState::Leased
            {
                entry.dirty = true;
            }
        }
        result
    }

    async fn quarantine(&self) {
        let quarantined = {
            let mut entries = self.entries.lock().await;
            let Some(entry) = entries.get_mut(&self.lease.entry_id) else {
                return;
            };
            if !lease_matches(entry.lease.as_ref(), &self.lease)
                || entry.state != PoolEntryState::Leased
            {
                return;
            }
            quarantine_entry(entry);
            true
        };
        if quarantined {
            self.notify.notify_one();
            self.changed.notify_waiters();
        }
    }
}

#[async_trait]
impl ManagedSandboxCapability for LeasedSandbox {
    fn id(&self) -> &str {
        LeasedSandbox::id(self)
    }

    async fn exec(&self, command: &SandboxCommand) -> Result<exoharness::SandboxCommandOutput> {
        LeasedSandbox::exec(self, command).await
    }
}

#[derive(Debug)]
struct NoReadyCapacity;
impl std::fmt::Display for NoReadyCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sandbox pool has no ready capacity")
    }
}
impl std::error::Error for NoReadyCapacity {}

fn validate_key(key: &SandboxPoolKey) -> Result<()> {
    ensure!(!key.pool_id.trim().is_empty(), "pool id must not be empty");
    ensure!(
        !key.recipe_id.trim().is_empty(),
        "recipe id must not be empty"
    );
    Ok(())
}

fn validate_capacity(capacity: &PoolCapacity) -> Result<()> {
    ensure!(capacity.warm_size > 0, "warm size must be positive");
    ensure!(
        capacity.warm_size <= capacity.max_total,
        "warm size must not exceed the maximum pool size"
    );
    ensure!(!capacity.lease_ttl.is_zero(), "lease ttl must be positive");
    Ok(())
}

fn validate_policy(policy: &PoolPolicy) -> Result<()> {
    ensure!(
        policy
            .health_check_command
            .first()
            .is_some_and(|command| !command.trim().is_empty()),
        "health check command must not be empty"
    );
    for (name, duration) in [
        ("provider timeout", policy.provider_timeout),
        ("health check timeout", policy.health_check_timeout),
        ("health check interval", policy.health_check_interval),
        ("reconcile interval", policy.reconcile_interval),
        ("command timeout", policy.command_timeout),
    ] {
        ensure!(!duration.is_zero(), "{name} must be positive");
    }
    Ok(())
}

fn entry_mut<'a>(
    entries: &'a mut HashMap<String, PoolEntry>,
    entry_id: &str,
) -> Result<&'a mut PoolEntry> {
    entries
        .get_mut(entry_id)
        .ok_or_else(|| anyhow!("sandbox pool entry not found: {entry_id}"))
}

/// Look up the entry a lease refers to and reject stale or fenced-out leases.
fn leased_entry_mut<'a>(
    entries: &'a mut HashMap<String, PoolEntry>,
    lease: &SandboxLease,
) -> Result<&'a mut PoolEntry> {
    let entry = entry_mut(entries, &lease.entry_id)?;
    validate_lease(entry, lease)?;
    Ok(entry)
}

fn quarantine_entry(entry: &mut PoolEntry) {
    entry.state = PoolEntryState::Retiring;
    entry.lease = None;
}

fn validate_lease(entry: &PoolEntry, lease: &SandboxLease) -> Result<()> {
    let Some(current) = entry.lease.as_ref() else {
        bail!("sandbox lease is not valid for entry {}", entry.id);
    };
    if current.fencing_token != lease.fencing_token || current.worker_id != lease.worker_id {
        bail!("sandbox lease is not valid for entry {}", entry.id);
    }
    if entry.state != PoolEntryState::Leased {
        bail!("sandbox pool entry is not leased: {}", entry.id);
    }
    if current.expires_at <= Instant::now() {
        bail!("sandbox lease has expired: {}", lease.fencing_token);
    }
    Ok(())
}

fn lease_matches(current: Option<&SandboxLease>, lease: &SandboxLease) -> bool {
    current.is_some_and(|current| {
        current.fencing_token == lease.fencing_token && current.worker_id == lease.worker_id
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use anyhow::bail;
    use async_trait::async_trait;

    use super::*;
    use crate::{LocalSandboxPoolStore, SandboxPoolSnapshotStore, SnapshotRetentionPolicy};
    use exoharness::{
        SandboxAttachment, SandboxCommandOutput, SandboxLifecycleConfig, SandboxProcessParts,
        SnapshotFormat, SnapshotPayload,
    };

    struct FakeBackend {
        fail_acquire: AtomicBool,
        fail_terminate: AtomicBool,
        acquire_count: AtomicUsize,
        terminate_count: AtomicUsize,
        snapshot_acquire_count: AtomicUsize,
        healthy: Arc<AtomicBool>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                fail_acquire: AtomicBool::new(false),
                fail_terminate: AtomicBool::new(false),
                acquire_count: AtomicUsize::new(0),
                terminate_count: AtomicUsize::new(0),
                snapshot_acquire_count: AtomicUsize::new(0),
                healthy: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    #[async_trait]
    impl ManagedSandboxBackend for FakeBackend {
        fn is_local(&self) -> bool {
            true
        }

        fn consumable_snapshot_formats(&self) -> &[SnapshotFormat] {
            &[]
        }

        async fn acquire(&self, _request: SandboxRequest) -> Result<Arc<dyn ManagedSandboxHandle>> {
            if self.fail_acquire.load(Ordering::SeqCst) {
                bail!("fake acquire failed");
            }
            let sequence = self.acquire_count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Arc::new(FakeHandle {
                id: format!("fake-sandbox-{sequence}"),
                healthy: Arc::clone(&self.healthy),
            }))
        }

        async fn attach(
            &self,
            _request: SandboxRequest,
            _attachment: SandboxAttachment,
        ) -> Result<Arc<dyn ManagedSandboxHandle>> {
            bail!("fake backend does not support attach")
        }

        async fn acquire_from_snapshot(
            &self,
            request: SandboxRequest,
            _payload: SnapshotPayload,
        ) -> Result<Arc<dyn ManagedSandboxHandle>> {
            self.snapshot_acquire_count.fetch_add(1, Ordering::SeqCst);
            self.acquire(request).await
        }

        async fn terminate(&self, _request: SandboxRequest) -> Result<()> {
            if self.fail_terminate.load(Ordering::SeqCst) {
                bail!("fake terminate failed");
            }
            self.terminate_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FakeHandle {
        id: String,
        healthy: Arc<AtomicBool>,
    }

    struct FakeRecipe {
        seed_count: AtomicUsize,
        fail: AtomicBool,
        snapshot: Option<SnapshotPayload>,
    }

    impl FakeRecipe {
        fn new() -> Self {
            Self {
                seed_count: AtomicUsize::new(0),
                fail: AtomicBool::new(false),
                snapshot: None,
            }
        }

        fn from_snapshot(snapshot: SnapshotPayload) -> Self {
            Self {
                seed_count: AtomicUsize::new(0),
                fail: AtomicBool::new(false),
                snapshot: Some(snapshot),
            }
        }
    }

    #[async_trait]
    impl SandboxPoolProvisioner for FakeRecipe {
        async fn acquire(
            &self,
            backend: &dyn ManagedSandboxBackend,
            request: SandboxRequest,
        ) -> Result<Arc<dyn ManagedSandboxHandle>> {
            self.seed_count.fetch_add(1, Ordering::SeqCst);
            let handle = match &self.snapshot {
                Some(snapshot) => {
                    backend
                        .acquire_from_snapshot(request.clone(), snapshot.clone())
                        .await?
                }
                None => backend.acquire(request.clone()).await?,
            };
            if self.fail.load(Ordering::SeqCst) {
                backend.terminate(request).await?;
                bail!("fake recipe failed for {}", handle.id());
            }
            Ok(handle)
        }
    }

    #[async_trait]
    impl ManagedSandboxHandle for FakeHandle {
        fn id(&self) -> &str {
            &self.id
        }

        async fn exec(&self, _command: &SandboxCommand) -> Result<SandboxCommandOutput> {
            if !self.healthy.load(Ordering::SeqCst) {
                bail!("fake health check failed");
            }
            Ok(SandboxCommandOutput {
                ok: true,
                exit_code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
                command: vec!["true".to_string()],
                cwd: "/".to_string(),
            })
        }

        async fn start_process(&self, _command: &SandboxCommand) -> Result<SandboxProcessParts> {
            bail!("fake handle does not support processes")
        }

        async fn stop(&self) -> Result<()> {
            Ok(())
        }

        async fn detach(&self) -> Result<SandboxAttachment> {
            bail!("fake handle does not support detach")
        }

        async fn snapshot(&self) -> Result<SnapshotPayload> {
            Ok(SnapshotPayload {
                format: SnapshotFormat::WorkspaceChunksV1,
                bytes: bytes::Bytes::from_static(b"fake workspace"),
            })
        }
    }

    struct SlowHandle;

    #[async_trait]
    impl ManagedSandboxHandle for SlowHandle {
        fn id(&self) -> &str {
            "slow-sandbox"
        }

        async fn exec(&self, _command: &SandboxCommand) -> Result<SandboxCommandOutput> {
            time::sleep(Duration::from_secs(1)).await;
            unreachable!("the lease wrapper should time out this call")
        }

        async fn start_process(&self, _command: &SandboxCommand) -> Result<SandboxProcessParts> {
            bail!("slow handle does not support processes")
        }

        async fn stop(&self) -> Result<()> {
            Ok(())
        }

        async fn detach(&self) -> Result<SandboxAttachment> {
            bail!("slow handle does not support detach")
        }

        async fn snapshot(&self) -> Result<SnapshotPayload> {
            bail!("slow handle does not support snapshots")
        }
    }

    fn request(entry_id: &str) -> SandboxRequest {
        SandboxRequest {
            key: PoolSandboxKey {
                pool_id: "pool".to_string(),
                entry_id: entry_id.to_string(),
            }
            .into(),
            spec: SandboxSpec {
                image: "fake-image".to_string(),
                resources: Default::default(),
                mounts: Vec::new(),
                durable_file_systems: Vec::new(),
                network: exoharness::SandboxNetworkPolicy::Disabled,
                default_workdir: "/".to_string(),
            },
            lifecycle: SandboxLifecycleConfig::default(),
            provider_state: None,
        }
    }

    fn pool(backend: Arc<FakeBackend>) -> LocalSandboxPool {
        pool_with_store(backend, None)
    }

    fn pool_with_store(
        backend: Arc<FakeBackend>,
        snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
    ) -> LocalSandboxPool {
        let spec = request("entry").spec;
        LocalSandboxPool::new(
            SandboxPoolKey {
                pool_id: "pool".to_string(),
                recipe_id: "test".to_string(),
                spec,
            },
            backend,
            PoolCapacity {
                warm_size: 1,
                max_total: 1,
                lease_ttl: Duration::from_secs(60),
                idle_ttl: Duration::from_secs(300),
            },
            Arc::new(EmptySandboxPoolProvisioner),
            snapshot_store,
        )
        .unwrap()
    }

    async fn state(pool: &LocalSandboxPool, entry_id: &str) -> PoolEntryState {
        pool.entries
            .lock()
            .await
            .get(entry_id)
            .expect("pool entry should exist")
            .state
    }

    #[tokio::test]
    async fn acquire_and_release_transitions_entry() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        let (lease, handle) = pool.try_acquire("worker-a").await.unwrap();
        assert_eq!(handle.id(), "fake-sandbox-1");
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Leased);
        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 1);

        pool.release(&lease).await.unwrap();
        assert_eq!(pool.entry_count().await, 1);
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Ready);
        let (_, reused) = pool.try_acquire("worker-b").await.unwrap();
        assert_ne!(reused.id(), handle.id());
        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn release_restores_the_recipe_baseline() {
        let backend = Arc::new(FakeBackend::new());
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalSandboxPoolStore::new(
            directory.path(),
            SnapshotRetentionPolicy::default(),
        ));
        let pool = pool_with_store(Arc::clone(&backend), Some(store.clone()));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        let (lease, handle) = pool.try_acquire("worker-a").await.unwrap();
        handle.exec(&command()).await.unwrap();
        let checkpoint_id = pool.release(&lease).await.unwrap().unwrap();
        let checkpoint = store.load("pool", "worker-a", checkpoint_id).await.unwrap();
        let baseline_id = store
            .list("pool", &pool.baseline_owner_id())
            .await
            .unwrap()
            .first()
            .unwrap()
            .snapshot_id;
        let snapshot = store
            .load("pool", &pool.baseline_owner_id(), baseline_id)
            .await
            .unwrap();

        assert_eq!(
            checkpoint.bytes,
            bytes::Bytes::from_static(b"fake workspace")
        );
        assert_eq!(snapshot.bytes, bytes::Bytes::from_static(b"fake workspace"));
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Ready);
        assert_eq!(pool.entries().await[0].snapshot_id, Some(baseline_id));
    }

    #[tokio::test]
    async fn restored_snapshot_becomes_the_entries_current_base() {
        let backend = Arc::new(FakeBackend::new());
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalSandboxPoolStore::new(
            directory.path(),
            SnapshotRetentionPolicy::default(),
        ));
        let pool = pool_with_store(backend, Some(store.clone()));
        pool.reconcile_once().await.unwrap();
        let snapshot_id = store
            .save(
                "pool",
                "worker",
                SnapshotPayload {
                    format: SnapshotFormat::WorkspaceChunksV1,
                    bytes: bytes::Bytes::from_static(b"checkpoint"),
                },
            )
            .await
            .unwrap();

        let acquired = pool
            .acquire_any_from_snapshot("worker", snapshot_id)
            .await
            .unwrap();

        assert_eq!(pool.entries().await[0].snapshot_id, Some(snapshot_id));
        pool.reset(&acquired.lease).await.unwrap();
    }

    #[tokio::test]
    async fn acquire_skips_leased_entries() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        let _lease = pool.try_acquire("worker-a").await.unwrap();
        assert!(pool.try_acquire("worker-b").await.is_err());
    }

    #[tokio::test]
    async fn stale_lease_cannot_release_entry() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        let (lease, _) = pool.try_acquire("worker-a").await.unwrap();
        let mut stale = lease.clone();
        stale.fencing_token = "stale-token".to_string();
        assert!(pool.release(&stale).await.is_err());
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Leased);
        pool.release(&lease).await.unwrap();
    }

    #[tokio::test]
    async fn acquire_failure_marks_entry_retiring() {
        let backend = Arc::new(FakeBackend::new());
        backend.fail_acquire.store(true, Ordering::SeqCst);
        let pool = pool(Arc::clone(&backend));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        assert!(pool.try_acquire("worker-a").await.is_err());
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Retiring);
    }

    #[tokio::test]
    async fn reset_terminates_and_removes_entry() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        let (lease, _) = pool.try_acquire("worker-a").await.unwrap();
        pool.reset(&lease).await.unwrap();
        assert_eq!(pool.entry_count().await, 0);
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reset_failure_keeps_entry_retiring() {
        let backend = Arc::new(FakeBackend::new());
        backend.fail_terminate.store(true, Ordering::SeqCst);
        let pool = pool(Arc::clone(&backend));
        pool.insert_entry(PoolEntry::new("entry".to_string(), request("entry"), None))
            .await
            .unwrap();

        let (lease, _) = pool.try_acquire("worker-a").await.unwrap();
        assert!(pool.reset(&lease).await.is_err());
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Retiring);
    }

    #[tokio::test]
    async fn health_check_marks_failed_entry_retiring() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        let handle: Arc<dyn ManagedSandboxHandle> = Arc::new(FakeHandle {
            id: "fake-sandbox".to_string(),
            healthy: Arc::clone(&backend.healthy),
        });
        pool.insert_entry(PoolEntry::new(
            "entry".to_string(),
            request("entry"),
            Some(handle),
        ))
        .await
        .unwrap();

        pool.health_check("entry").await.unwrap();
        backend.healthy.store(false, Ordering::SeqCst);
        assert!(pool.health_check("entry").await.is_err());
        assert_eq!(state(&pool, "entry").await, PoolEntryState::Retiring);
    }

    #[tokio::test]
    async fn reconcile_once_replenishes_warm_capacity() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));

        pool.reconcile_once().await.unwrap();

        assert_eq!(pool.entry_count().await, 1);
        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 1);
        let entries = pool.entries.lock().await;
        assert!(
            entries
                .values()
                .all(|entry| entry.state == PoolEntryState::Ready)
        );
    }

    #[tokio::test]
    async fn snapshot_seed_initializes_each_fresh_pool_entry() {
        let backend = Arc::new(FakeBackend::new());
        let pool = LocalSandboxPool::new(
            SandboxPoolKey {
                pool_id: "seeded-pool".to_string(),
                recipe_id: "test".to_string(),
                spec: request("entry").spec,
            },
            Arc::clone(&backend) as Arc<dyn ManagedSandboxBackend>,
            PoolCapacity {
                warm_size: 1,
                max_total: 1,
                lease_ttl: Duration::from_secs(60),
                idle_ttl: Duration::from_secs(300),
            },
            Arc::new(FakeRecipe::from_snapshot(SnapshotPayload {
                format: SnapshotFormat::WorkspaceChunksV1,
                bytes: bytes::Bytes::from_static(b"seeded codebase"),
            })),
            None,
        )
        .unwrap();

        pool.reconcile_once().await.unwrap();

        assert_eq!(backend.snapshot_acquire_count.load(Ordering::SeqCst), 1);
        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn recipe_seed_initializes_each_fresh_pool_entry_before_it_is_ready() {
        let backend = Arc::new(FakeBackend::new());
        let recipe = Arc::new(FakeRecipe::from_snapshot(SnapshotPayload {
            format: SnapshotFormat::WorkspaceChunksV1,
            bytes: bytes::Bytes::from_static(b"base codebase"),
        }));
        let pool = LocalSandboxPool::new(
            SandboxPoolKey {
                pool_id: "recipe-seeded-pool".to_string(),
                recipe_id: "test".to_string(),
                spec: request("entry").spec,
            },
            Arc::clone(&backend) as Arc<dyn ManagedSandboxBackend>,
            PoolCapacity {
                warm_size: 1,
                max_total: 1,
                lease_ttl: Duration::from_secs(60),
                idle_ttl: Duration::from_secs(300),
            },
            recipe.clone(),
            None,
        )
        .unwrap();

        pool.reconcile_once().await.unwrap();

        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 1);
        assert_eq!(backend.snapshot_acquire_count.load(Ordering::SeqCst), 1);
        assert_eq!(recipe.seed_count.load(Ordering::SeqCst), 1);
        assert!(
            pool.entries
                .lock()
                .await
                .values()
                .all(|entry| entry.state == PoolEntryState::Ready)
        );
    }

    #[tokio::test]
    async fn recipe_seed_failure_terminates_the_unusable_runtime() {
        let backend = Arc::new(FakeBackend::new());
        let recipe = Arc::new(FakeRecipe::new());
        recipe.fail.store(true, Ordering::SeqCst);
        let pool = LocalSandboxPool::new(
            SandboxPoolKey {
                pool_id: "failing-recipe-seeded-pool".to_string(),
                recipe_id: "test".to_string(),
                spec: request("entry").spec,
            },
            Arc::clone(&backend) as Arc<dyn ManagedSandboxBackend>,
            PoolCapacity {
                warm_size: 1,
                max_total: 1,
                lease_ttl: Duration::from_secs(60),
                idle_ttl: Duration::from_secs(300),
            },
            recipe.clone(),
            None,
        )
        .unwrap();

        assert!(pool.reconcile_once().await.is_err());

        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 1);
        assert_eq!(recipe.seed_count.load(Ordering::SeqCst), 1);
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 1);
        assert!(
            pool.entries
                .lock()
                .await
                .values()
                .all(|entry| entry.state == PoolEntryState::Retiring)
        );
        assert!(pool.try_acquire("worker-a").await.is_err());
    }

    #[tokio::test]
    async fn acquire_any_uses_least_recently_used_ready_entry() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        let old_handle: Arc<dyn ManagedSandboxHandle> = Arc::new(FakeHandle {
            id: "old-sandbox".to_string(),
            healthy: Arc::clone(&backend.healthy),
        });
        let new_handle: Arc<dyn ManagedSandboxHandle> = Arc::new(FakeHandle {
            id: "new-sandbox".to_string(),
            healthy: Arc::clone(&backend.healthy),
        });
        pool.insert_entry(PoolEntry::new(
            "old".to_string(),
            request("old"),
            Some(old_handle),
        ))
        .await
        .unwrap();
        pool.insert_entry(PoolEntry::new(
            "new".to_string(),
            request("new"),
            Some(new_handle),
        ))
        .await
        .unwrap();
        {
            let mut entries = pool.entries.lock().await;
            entries.get_mut("old").unwrap().last_used_at =
                Instant::now().checked_sub(Duration::from_secs(10)).unwrap();
        }

        let (_, handle) = pool.try_acquire("worker-a").await.unwrap();
        assert_eq!(handle.id(), "old-sandbox");
    }

    #[tokio::test]
    async fn reconcile_does_not_churn_idle_warm_capacity() {
        let backend = Arc::new(FakeBackend::new());
        let mut pool = pool(Arc::clone(&backend));
        pool.capacity = PoolCapacity {
            warm_size: 1,
            max_total: 1,
            lease_ttl: Duration::from_secs(60),
            idle_ttl: Duration::from_secs(1),
        };
        let mut entry = PoolEntry::new(
            "entry".to_string(),
            request("entry"),
            Some(Arc::new(FakeHandle {
                id: "sandbox".to_string(),
                healthy: Arc::clone(&backend.healthy),
            })),
        );
        entry.last_used_at = Instant::now().checked_sub(Duration::from_secs(10)).unwrap();
        pool.insert_entry(entry).await.unwrap();

        pool.reconcile_once().await.unwrap();

        assert_eq!(pool.entry_count().await, 1);
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reconcile_scales_to_warm_size_but_not_past_maximum() {
        let backend = Arc::new(FakeBackend::new());
        let pool = LocalSandboxPool::new(
            SandboxPoolKey {
                pool_id: "pool".to_string(),
                recipe_id: "test".to_string(),
                spec: request("entry").spec,
            },
            backend,
            PoolCapacity {
                warm_size: 2,
                max_total: 3,
                lease_ttl: Duration::from_secs(60),
                idle_ttl: Duration::from_secs(300),
            },
            Arc::new(EmptySandboxPoolProvisioner),
            None,
        )
        .unwrap();

        pool.reconcile_once().await.unwrap();
        assert_eq!(pool.entry_count().await, 2);

        let first = pool.try_acquire("worker-a").await.unwrap().0;
        let second = pool.try_acquire("worker-b").await.unwrap().0;
        pool.reconcile_once().await.unwrap();

        assert_eq!(pool.entry_count().await, 3);
        assert!(pool.try_acquire("worker-c").await.is_ok());
        assert!(pool.try_acquire("worker-d").await.is_err());

        pool.release(&first).await.unwrap();
        pool.release(&second).await.unwrap();
    }

    fn command() -> SandboxCommand {
        SandboxCommand {
            argv: vec!["true".into()],
            env: HashMap::new(),
            display_argv: None,
            cwd: None,
            timeout: Some(Duration::from_secs(1)),
        }
    }

    #[tokio::test]
    async fn end_to_end_wait_release_replace_and_shutdown() {
        let backend = Arc::new(FakeBackend::new());
        let pool = Arc::new(pool(Arc::clone(&backend)));
        let (shutdown, receiver) = watch::channel(false);
        let reconciler = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.run_reconciler(receiver).await }
        });
        let ManagedSandboxLease {
            lease: first,
            sandbox: old_handle,
        } = time::timeout(Duration::from_secs(2), pool.acquire_any("first"))
            .await
            .unwrap()
            .unwrap();
        assert!(old_handle.exec(&command()).await.unwrap().ok);
        pool.heartbeat(&first).await.unwrap();
        assert!(
            time::timeout(Duration::from_millis(20), pool.acquire_any("cancelled"))
                .await
                .is_err()
        );
        let waiter = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.acquire_any("second").await }
        });
        pool.release(&first).await.unwrap();
        let ManagedSandboxLease {
            lease: second,
            sandbox: new_handle,
        } = time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_ne!(old_handle.id(), new_handle.id());
        assert!(pool.release(&first).await.is_err());
        assert!(new_handle.exec(&command()).await.unwrap().ok);
        pool.release(&second).await.unwrap();
        shutdown.send(true).unwrap();
        time::timeout(Duration::from_secs(2), reconciler)
            .await
            .unwrap()
            .unwrap();
        assert!(pool.acquire_any("after shutdown").await.is_err());
    }

    #[tokio::test]
    async fn concurrent_reconciliation_respects_capacity() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        let (a, b, c) = tokio::join!(
            pool.reconcile_once(),
            pool.reconcile_once(),
            pool.reconcile_once()
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        assert_eq!(pool.entry_count().await, 1);
        assert_eq!(backend.acquire_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_creation_is_removed_before_replacement() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        backend.fail_acquire.store(true, Ordering::SeqCst);
        assert!(pool.reconcile_once().await.is_err());
        assert_eq!(pool.entry_count().await, 1);
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 0);
        backend.fail_acquire.store(false, Ordering::SeqCst);
        pool.reconcile_once().await.unwrap();
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 0);
        assert!(pool.try_acquire("recovered").await.is_ok());
    }

    #[tokio::test]
    async fn health_checks_do_not_extend_idle_lifetime() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(backend);
        pool.reconcile_once().await.unwrap();
        let (id, last_used) = {
            let entries = pool.entries.lock().await;
            let entry = entries.values().next().unwrap();
            (entry.id.clone(), entry.last_used_at)
        };
        pool.health_check(&id).await.unwrap();
        assert_eq!(pool.entries.lock().await[&id].last_used_at, last_used);
    }

    #[tokio::test]
    async fn idle_eviction_is_lru_and_preserves_warm_size() {
        let backend = Arc::new(FakeBackend::new());
        let mut pool = pool(Arc::clone(&backend));
        pool.capacity.warm_size = 1;
        pool.capacity.max_total = 2;
        for entry_id in ["old", "new"] {
            pool.insert_entry(PoolEntry::new(
                entry_id.to_string(),
                request(entry_id),
                Some(Arc::new(FakeHandle {
                    id: format!("{entry_id}-sandbox"),
                    healthy: Arc::clone(&backend.healthy),
                })),
            ))
            .await
            .unwrap();
        }
        let old_id = {
            let mut entries = pool.entries.lock().await;
            entries.get_mut("old").unwrap().last_used_at =
                Instant::now() - Duration::from_secs(600);
            entries.get_mut("new").unwrap().last_used_at =
                Instant::now() - Duration::from_secs(400);
            "old".to_string()
        };
        pool.evict_idle().await.unwrap();
        assert_eq!(pool.entry_count().await, 1);
        assert!(!pool.entries.lock().await.contains_key(&old_id));
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn health_check_cannot_change_active_lease() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(backend);
        pool.reconcile_once().await.unwrap();
        let (lease, handle) = pool.try_acquire("active").await.unwrap();
        assert!(pool.health_check(&lease.entry_id).await.is_err());
        assert!(handle.exec(&command()).await.unwrap().ok);
        assert_eq!(state(&pool, &lease.entry_id).await, PoolEntryState::Leased);
    }

    #[tokio::test]
    async fn expired_handles_are_fenced_and_replaced() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        pool.reconcile_once().await.unwrap();
        let (lease, handle) = pool.try_acquire("expired").await.unwrap();
        pool.entries
            .lock()
            .await
            .get_mut(&lease.entry_id)
            .unwrap()
            .lease
            .as_mut()
            .unwrap()
            .expires_at = Instant::now();
        assert!(handle.exec(&command()).await.is_err());
        assert!(pool.heartbeat(&lease).await.is_err());
        pool.reconcile_once().await.unwrap();
        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 1);
        let (_, replacement) = pool.try_acquire("replacement").await.unwrap();
        assert_ne!(handle.id(), replacement.id());
    }

    #[tokio::test]
    async fn expired_lease_is_checkpointed_before_the_entry_is_retired() {
        let backend = Arc::new(FakeBackend::new());
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalSandboxPoolStore::new(
            directory.path(),
            SnapshotRetentionPolicy::default(),
        ));
        let pool = pool_with_store(backend, Some(store.clone()));
        pool.reconcile_once().await.unwrap();
        let (lease, handle) = pool.try_acquire("expired").await.unwrap();
        handle.exec(&command()).await.unwrap();
        pool.entries
            .lock()
            .await
            .get_mut(&lease.entry_id)
            .unwrap()
            .lease
            .as_mut()
            .unwrap()
            .expires_at = Instant::now();

        pool.reconcile_once().await.unwrap();

        let checkpoints = store.list("pool", "expired").await.unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            store
                .load("pool", "expired", checkpoints[0].snapshot_id)
                .await
                .unwrap()
                .bytes,
            bytes::Bytes::from_static(b"fake workspace")
        );
    }

    #[tokio::test]
    async fn provider_failure_quarantines_and_replaces_a_leased_runtime() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(Arc::clone(&backend));
        pool.reconcile_once().await.unwrap();
        let (lease, handle) = pool.try_acquire("worker").await.unwrap();

        backend.healthy.store(false, Ordering::SeqCst);
        assert!(handle.exec(&command()).await.is_err());
        assert_eq!(
            state(&pool, &lease.entry_id).await,
            PoolEntryState::Retiring
        );
        assert!(pool.heartbeat(&lease).await.is_err());

        backend.healthy.store(true, Ordering::SeqCst);
        pool.reconcile_once().await.unwrap();

        assert_eq!(backend.terminate_count.load(Ordering::SeqCst), 1);
        let (_, replacement) = pool.try_acquire("replacement").await.unwrap();
        assert_ne!(handle.id(), replacement.id());
        assert!(replacement.exec(&command()).await.unwrap().ok);
    }

    #[tokio::test]
    async fn command_timeout_quarantines_a_leased_runtime() {
        let backend = Arc::new(FakeBackend::new());
        let pool = pool(backend);
        pool.insert_entry(PoolEntry::new(
            "slow".to_string(),
            request("slow"),
            Some(Arc::new(SlowHandle)),
        ))
        .await
        .unwrap();
        let (lease, sandbox) = pool.try_acquire("worker").await.unwrap();
        let mut timed = command();
        timed.timeout = Some(Duration::from_millis(1));

        assert!(sandbox.exec(&timed).await.is_err());
        assert_eq!(
            state(&pool, &lease.entry_id).await,
            PoolEntryState::Retiring
        );
        assert!(pool.release(&lease).await.is_err());
    }
}
