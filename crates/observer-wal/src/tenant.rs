use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use observer_protocol::{AcceptedBatch, AppendError, IngestSink};
use tokio::task::JoinSet;

use crate::{AsyncWal, WalError, WalWriterConfig};

const TENANTS_DIR_NAME: &str = "tenants";

/// Eagerly opened, independently queued WAL writer for every configured tenant.
pub struct TenantWalRouter {
    tenants: BTreeMap<String, AsyncWal>,
}

impl TenantWalRouter {
    /// Open one complete WAL under `tenants/<tenant>` for every unique tenant.
    pub fn open<I, S>(config: WalWriterConfig, tenants: I) -> Result<Self, WalError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let root = config.wal.directory.clone();
        let tenant_ids: BTreeSet<String> = tenants.into_iter().map(Into::into).collect();
        if tenant_ids.is_empty() {
            return Err(WalError::InvalidConfig(
                "at least one WAL tenant must be configured",
            ));
        }

        let mut opened = BTreeMap::new();
        for tenant_id in tenant_ids {
            let mut tenant_config = config.clone();
            tenant_config.wal.directory = tenant_wal_directory(&root, &tenant_id);
            opened.insert(tenant_id, AsyncWal::open(tenant_config)?);
        }
        Ok(Self { tenants: opened })
    }

    #[must_use]
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// True if any configured tenant writer has entered a failed state.
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.tenants.values().any(AsyncWal::is_failed)
    }

    /// Stop every tenant writer concurrently and wait for every thread.
    pub async fn shutdown(&self) -> Result<(), WalError> {
        let mut joins = JoinSet::new();
        for wal in self.tenants.values() {
            let wal = wal.clone();
            joins.spawn(async move { wal.shutdown().await });
        }

        let mut failed = false;
        while let Some(result) = joins.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => failed = true,
            }
        }
        if failed {
            Err(WalError::Failed)
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl IngestSink for TenantWalRouter {
    async fn append(&self, batch: AcceptedBatch) -> Result<(), AppendError> {
        let wal = self.tenants.get(&batch.tenant_id).ok_or_else(|| {
            AppendError::internal("authenticated tenant has no configured WAL writer")
        })?;
        wal.append(batch).await
    }
}

/// Root directory containing the existing single-lane WAL for `tenant_id`.
#[must_use]
pub fn tenant_wal_directory(root: impl AsRef<Path>, tenant_id: &str) -> PathBuf {
    root.as_ref().join(TENANTS_DIR_NAME).join(tenant_id)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};

    use super::*;
    use crate::{
        SEGMENT_HEADER_SIZE, WalCheckpoint, WalReader, encoded_frame_size, retain_committed,
        tenant_wal_directory,
    };

    fn batch(tenant_id: &str, payload: &'static [u8]) -> AcceptedBatch {
        AcceptedBatch {
            tenant_id: tenant_id.to_owned(),
            signal: Signal::Logs,
            received_at_unix_nanos: 1,
            payload: Bytes::from_static(payload),
        }
    }

    fn read_payloads(root: &Path, tenant_id: &str) -> Vec<Bytes> {
        let directory = tenant_wal_directory(root, tenant_id);
        let mut reader = WalReader::open(directory).expect("reader");
        let mut payloads = Vec::new();
        while let Some(record) = reader.next_record().expect("next") {
            payloads.push(record.frame.payload);
        }
        payloads
    }

    #[tokio::test]
    async fn routes_tenants_to_independent_wals_and_sequences() {
        let dir = tempfile::tempdir().expect("tempdir");
        let router = TenantWalRouter::open(
            WalWriterConfig::new(dir.path()),
            ["tenant-b", "tenant-a", "tenant-a"],
        )
        .expect("router");
        assert_eq!(router.tenant_count(), 2);

        router
            .append(batch("tenant-a", b"a-one"))
            .await
            .expect("a one");
        router
            .append(batch("tenant-b", b"b-one"))
            .await
            .expect("b one");
        router
            .append(batch("tenant-a", b"a-two"))
            .await
            .expect("a two");
        router.shutdown().await.expect("shutdown");

        assert_eq!(
            read_payloads(dir.path(), "tenant-a"),
            [Bytes::from_static(b"a-one"), Bytes::from_static(b"a-two")]
        );
        assert_eq!(
            read_payloads(dir.path(), "tenant-b"),
            [Bytes::from_static(b"b-one")]
        );

        let mut a =
            WalReader::open(tenant_wal_directory(dir.path(), "tenant-a")).expect("a reader");
        let mut b =
            WalReader::open(tenant_wal_directory(dir.path(), "tenant-b")).expect("b reader");
        assert_eq!(a.next_record().unwrap().unwrap().frame.sequence, 0);
        assert_eq!(b.next_record().unwrap().unwrap().frame.sequence, 0);
    }

    #[tokio::test]
    async fn rejects_unknown_tenant_without_creating_a_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let router =
            TenantWalRouter::open(WalWriterConfig::new(dir.path()), ["tenant-a"]).expect("router");

        let error = router
            .append(batch("tenant-b", b"payload"))
            .await
            .expect_err("unknown");
        assert_eq!(error.kind(), observer_protocol::AppendErrorKind::Internal);
        assert!(!dir.path().join("tenants/tenant-b").exists());
        router.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn checkpoint_and_retention_are_independent_per_tenant() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = WalWriterConfig::new(dir.path());
        let frame_size =
            u64::try_from(encoded_frame_size("tenant-a".len(), b"one".len()).expect("size"))
                .expect("u64");
        config.wal.target_segment_bytes =
            u64::try_from(SEGMENT_HEADER_SIZE).expect("header") + frame_size + 1;
        let router = TenantWalRouter::open(config, ["tenant-a", "tenant-b"]).expect("router");
        for tenant in ["tenant-a", "tenant-b"] {
            router.append(batch(tenant, b"one")).await.expect("first");
            router.append(batch(tenant, b"two")).await.expect("second");
        }
        router.shutdown().await.expect("shutdown");

        let a_dir = tenant_wal_directory(dir.path(), "tenant-a");
        let b_dir = tenant_wal_directory(dir.path(), "tenant-b");
        let mut a_reader = WalReader::open(&a_dir).expect("a reader");
        let a_cursor = a_reader
            .next_record()
            .expect("a next")
            .expect("a record")
            .next_cursor;
        let mut a_checkpoint = WalCheckpoint::load(&a_dir).expect("a checkpoint");
        a_checkpoint.commit(a_cursor).expect("a commit");

        let a_report = retain_committed(&a_dir).expect("a retain");
        let b_report = retain_committed(&b_dir).expect("b retain");
        assert_eq!(a_report.deleted_segments, [0]);
        assert!(a_report.bytes_reclaimed > 0);
        assert!(b_report.deleted_segments.is_empty());
        assert!(!a_dir.join("lane-0000/00000000000000000000.wal").exists());
        assert!(b_dir.join("lane-0000/00000000000000000000.wal").exists());
    }
}
