use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::bail;
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::Mutex;

use exoharness::{Result, SnapshotFormat, SnapshotId, SnapshotPayload, Uuid7};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotRetentionPolicy {
    pub idle_ttl: Option<Duration>,
    pub max_bytes: Option<u64>,
    pub max_snapshots: Option<usize>,
}

/// Owner-scoped snapshot persistence used by a sandbox pool.
#[async_trait]
pub trait SandboxPoolSnapshotStore: Send + Sync {
    async fn save(
        &self,
        pool_id: &str,
        owner_id: &str,
        payload: SnapshotPayload,
    ) -> Result<SnapshotId>;

    async fn load(
        &self,
        pool_id: &str,
        owner_id: &str,
        snapshot_id: SnapshotId,
    ) -> Result<SnapshotPayload>;

    async fn prune(&self) -> Result<()>;
}

/// A single-process, filesystem-backed snapshot cache.
///
/// Snapshots are scoped by pool and owner. Pruning removes least-recently-used
/// snapshots until every configured retention limit is satisfied.
#[derive(Debug, Clone)]
pub struct LocalSandboxPoolStore {
    root: PathBuf,
    retention: SnapshotRetentionPolicy,
    operation: Arc<Mutex<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    snapshot_id: SnapshotId,
    format: SnapshotFormat,
    size_bytes: u64,
    last_accessed_at_ms: u64,
}

struct SnapshotRecord {
    directory: PathBuf,
    manifest: SnapshotManifest,
}

impl LocalSandboxPoolStore {
    pub fn new(root: impl Into<PathBuf>, retention: SnapshotRetentionPolicy) -> Self {
        Self {
            root: root.into(),
            retention,
            operation: Arc::new(Mutex::new(())),
        }
    }

    fn owner_directory(&self, pool_id: &str, owner_id: &str) -> PathBuf {
        self.root
            .join(path_component(pool_id))
            .join(path_component(owner_id))
    }

    fn snapshot_directory(
        &self,
        pool_id: &str,
        owner_id: &str,
        snapshot_id: SnapshotId,
    ) -> PathBuf {
        self.owner_directory(pool_id, owner_id)
            .join(snapshot_id.to_string())
    }

    async fn records(&self) -> Result<Vec<SnapshotRecord>> {
        if !fs::try_exists(&self.root).await? {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for pool in directories(&self.root).await? {
            for owner in directories(&pool).await? {
                for directory in directories(&owner).await? {
                    records.push(SnapshotRecord {
                        manifest: read_manifest(&directory.join("manifest.json")).await?,
                        directory,
                    });
                }
            }
        }
        Ok(records)
    }
}

#[async_trait]
impl SandboxPoolSnapshotStore for LocalSandboxPoolStore {
    async fn save(
        &self,
        pool_id: &str,
        owner_id: &str,
        payload: SnapshotPayload,
    ) -> Result<SnapshotId> {
        let _operation = self.operation.lock().await;
        let snapshot_id = Uuid7::now();
        let owner_directory = self.owner_directory(pool_id, owner_id);
        let temporary = owner_directory.join(format!(".tmp-{snapshot_id}"));
        let directory = owner_directory.join(snapshot_id.to_string());
        fs::create_dir_all(&owner_directory).await?;
        fs::create_dir(&temporary).await?;

        let manifest = SnapshotManifest {
            snapshot_id,
            format: payload.format,
            size_bytes: payload.bytes.len() as u64,
            last_accessed_at_ms: now_ms(),
        };
        let result = async {
            write_manifest(&temporary.join("manifest.json"), &manifest).await?;
            fs::write(temporary.join("payload.bin"), &payload.bytes).await?;
            fs::rename(&temporary, directory).await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            if let Err(cleanup_error) = fs::remove_dir_all(&temporary).await {
                tracing::warn!(
                    %cleanup_error,
                    path = %temporary.display(),
                    "failed cleaning up incomplete local sandbox snapshot"
                );
            }
            return Err(error);
        }
        Ok(snapshot_id)
    }

    async fn load(
        &self,
        pool_id: &str,
        owner_id: &str,
        snapshot_id: SnapshotId,
    ) -> Result<SnapshotPayload> {
        let _operation = self.operation.lock().await;
        let directory = self.snapshot_directory(pool_id, owner_id, snapshot_id);
        let manifest_path = directory.join("manifest.json");
        let mut manifest = read_manifest(&manifest_path).await?;
        if manifest.snapshot_id != snapshot_id {
            bail!("snapshot manifest id does not match requested snapshot");
        }

        let bytes = Bytes::from(fs::read(directory.join("payload.bin")).await?);
        if bytes.len() as u64 != manifest.size_bytes {
            bail!("snapshot payload size does not match its manifest");
        }
        manifest.last_accessed_at_ms = now_ms();
        write_manifest(&manifest_path, &manifest).await?;
        Ok(SnapshotPayload {
            format: manifest.format,
            bytes,
        })
    }

    async fn prune(&self) -> Result<()> {
        let _operation = self.operation.lock().await;
        let now = now_ms();
        let mut records = self.records().await?;
        records.sort_by_key(|record| record.manifest.last_accessed_at_ms);

        let mut remaining_count = records.len();
        let mut remaining_bytes = records
            .iter()
            .map(|record| record.manifest.size_bytes)
            .sum::<u64>();
        for record in records {
            let expired = self.retention.idle_ttl.is_some_and(|ttl| {
                now.saturating_sub(record.manifest.last_accessed_at_ms) >= ttl.as_millis() as u64
            });
            let over_count = self
                .retention
                .max_snapshots
                .is_some_and(|max| remaining_count > max);
            let over_bytes = self
                .retention
                .max_bytes
                .is_some_and(|max| remaining_bytes > max);
            if expired || over_count || over_bytes {
                fs::remove_dir_all(record.directory).await?;
                remaining_count -= 1;
                remaining_bytes = remaining_bytes.saturating_sub(record.manifest.size_bytes);
            }
        }
        Ok(())
    }
}

fn path_component(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value.as_bytes())
}

/// Ignore unpublished temporary directories and never follow symlinks.
async fn directories(path: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    let mut entries = fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir()
            && !entry.file_name().to_string_lossy().starts_with('.')
        {
            result.push(entry.path());
        }
    }
    Ok(result)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn read_manifest(path: &Path) -> Result<SnapshotManifest> {
    Ok(serde_json::from_slice(&fs::read(path).await?)?)
}

async fn write_manifest(path: &Path, manifest: &SnapshotManifest) -> Result<()> {
    fs::write(path, serde_json::to_vec(manifest)?).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pruning_keeps_the_most_recently_used_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalSandboxPoolStore::new(
            directory.path(),
            SnapshotRetentionPolicy {
                max_snapshots: Some(1),
                ..Default::default()
            },
        );
        let first = store
            .save(
                "pool",
                "workspace",
                SnapshotPayload {
                    format: SnapshotFormat::WorkspaceChunksV1,
                    bytes: Bytes::from_static(b"first"),
                },
            )
            .await
            .unwrap();
        let second = store
            .save(
                "pool",
                "workspace",
                SnapshotPayload {
                    format: SnapshotFormat::WorkspaceChunksV1,
                    bytes: Bytes::from_static(b"second"),
                },
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(2)).await;
        store.load("pool", "workspace", first).await.unwrap();
        store.prune().await.unwrap();

        assert!(store.load("pool", "workspace", first).await.is_ok());
        assert!(store.load("pool", "workspace", second).await.is_err());
    }
}
