//! Atomic, content-addressed filesystem Paddock backend.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use pit_artifact::ArtifactManifest;
use pit_paddock_core::{
    ArtifactDigest, ArtifactName, BlobDigest, BlobPut, BlobSize, CasConflict, NamespaceId,
    ObjectKey, ObjectMetadata, ObjectRef, ObjectVersion, ObjectWriter, PaddockBackend,
    PaddockCapabilities, PaddockObjectBackend, PaddockRef, PaddockRefWire, RefCondition,
    ResolvedRef,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

#[derive(Debug, Clone)]
pub struct FilesystemPaddock {
    root: PathBuf,
    object_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FilesystemPaddock {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            object_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn blob_path(&self, digest: &ArtifactDigest) -> PathBuf {
        self.root
            .join("blobs/sha256")
            .join(&digest.hex()[..2])
            .join(format!("{}.wasm", digest.hex()))
    }

    fn manifest_path(&self, digest: &ArtifactDigest) -> PathBuf {
        self.root
            .join("manifests/sha256")
            .join(format!("{}.json", digest.hex()))
    }

    fn ref_path(&self, reference: &PaddockRef) -> PathBuf {
        self.root
            .join("refs")
            .join(reference.name.as_str())
            .join(format!("{}.json", reference.tag))
    }

    fn generic_blob_path(&self, digest: &BlobDigest) -> PathBuf {
        self.root
            .join("blobs/sha256")
            .join(&digest.hex()[..2])
            .join(format!("{}.blob", digest.hex()))
    }

    fn object_dir(&self, namespace: &NamespaceId, key: &ObjectKey) -> PathBuf {
        let mut path = self.root.join("objects");
        for part in namespace.as_str().split('/').chain(key.as_str().split('/')) {
            path.push(part);
        }
        path
    }

    fn object_current_path(&self, namespace: &NamespaceId, key: &ObjectKey) -> PathBuf {
        self.object_dir(namespace, key).join("current.json")
    }

    fn object_lock_path(&self, namespace: &NamespaceId, key: &ObjectKey) -> PathBuf {
        let mut path = self.root.join("locks/objects");
        for part in namespace.as_str().split('/').chain(key.as_str().split('/')) {
            path.push(part);
        }
        path.push(".lock");
        path
    }

    fn object_version_path(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
        version: ObjectVersion,
    ) -> PathBuf {
        self.object_dir(namespace, key)
            .join("versions")
            .join(format!("{}.json", version.get()))
    }

    async fn ensure_no_symlink(path: &Path) -> Result<()> {
        let mut current = PathBuf::new();
        for component in path.components() {
            current.push(component);
            if let Ok(metadata) = tokio::fs::symlink_metadata(&current).await
                && metadata.file_type().is_symlink()
            {
                bail!("Paddock path contains a symlink: {}", current.display());
            }
        }
        Ok(())
    }

    async fn acquire_process_lock(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
    ) -> Result<ProcessLock> {
        let path = self.object_lock_path(namespace, key);
        let parent = path.parent().context("object lock has no parent")?;
        Self::ensure_no_symlink(parent).await?;
        tokio::fs::create_dir_all(parent).await?;
        Self::ensure_no_symlink(parent).await?;
        let lock = tokio::task::spawn_blocking(move || -> Result<ProcessLock> {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .with_context(|| format!("open object lock {}", path.display()))?;
            // flock is kernel-managed: a process crash releases the lock, so
            // no stale PID cleanup protocol is needed.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if result != 0 {
                bail!(
                    "acquire object lock {}: {}",
                    path.display(),
                    std::io::Error::last_os_error()
                );
            }
            Ok(ProcessLock { file })
        })
        .await
        .context("object lock task failed")??;
        Ok(lock)
    }

    async fn read_object_record(&self, path: &Path) -> Result<ObjectRef> {
        let bytes = tokio::fs::read(path)
            .await
            .context("object ref is unavailable")?;
        serde_json::from_slice(&bytes).context("malformed Paddock object ref")
    }

    async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
        let parent = path.parent().context("Paddock path has no parent")?;
        Self::ensure_no_symlink(parent).await?;
        tokio::fs::create_dir_all(parent).await?;
        Self::ensure_no_symlink(parent).await?;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let temp = parent.join(format!(
            ".{}.{}.{}.tmp",
            path.file_name().unwrap().to_string_lossy(),
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .await?;
            file.write_all(bytes).await?;
            fault_point("after_temp_write");
            file.sync_all().await?;
            tokio::fs::rename(&temp, path).await?;
            let file_name = path.file_name().and_then(|name| name.to_str());
            if file_name == Some("current.json") {
                fault_point("after_current_ref_rename");
            } else if path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                == Some("versions")
            {
                fault_point("after_version_rename");
            }
            fault_point("before_directory_fsync");
            Self::sync_directory(parent).await?;
            if file_name == Some("current.json") {
                fault_point("after_directory_fsync");
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temp).await;
        }
        result
    }

    async fn atomic_write_from_file(temp: &Path, path: &Path) -> Result<()> {
        let parent = path.parent().context("Paddock path has no parent")?;
        Self::ensure_no_symlink(parent).await?;
        tokio::fs::create_dir_all(parent).await?;
        Self::ensure_no_symlink(parent).await?;
        tokio::fs::rename(temp, path).await?;
        fault_point("after_blob_rename");
        Self::sync_directory(parent).await?;
        Ok(())
    }

    async fn sync_directory(path: &Path) -> Result<()> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let directory = std::fs::File::open(&path)
                .with_context(|| format!("open directory {} for sync", path.display()))?;
            directory
                .sync_all()
                .with_context(|| format!("sync directory {}", path.display()))
        })
        .await
        .context("directory sync task failed")??;
        Ok(())
    }
}

#[cfg(feature = "fault-injection")]
fn fault_point(name: &str) {
    if std::env::var("PITFAST_FS_FAULT_POINT").ok().as_deref() == Some(name) {
        eprintln!("fault injection: {name}");
        std::process::exit(137);
    }
}

#[cfg(not(feature = "fault-injection"))]
fn fault_point(_name: &str) {}

struct ProcessLock {
    file: std::fs::File,
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

struct FilesystemObjectWriter {
    backend: FilesystemPaddock,
    namespace: NamespaceId,
    key: ObjectKey,
    metadata: ObjectMetadata,
    condition: RefCondition,
    temp: PathBuf,
    file: Option<tokio::fs::File>,
    hasher: Sha256,
    size: u64,
}

impl Drop for FilesystemObjectWriter {
    fn drop(&mut self) {
        // Best-effort synchronous cleanup for cancellation paths. Explicit
        // abort remains the reliable asynchronous cleanup operation.
        let _ = std::fs::remove_file(&self.temp);
    }
}

#[async_trait]
impl ObjectWriter for FilesystemObjectWriter {
    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let new_size = self
            .size
            .checked_add(bytes.len() as u64)
            .context("object size overflow")?;
        self.file
            .as_mut()
            .context("object writer is already finalized")?
            .write_all(bytes)
            .await?;
        self.hasher.update(bytes);
        self.size = new_size;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<ObjectRef> {
        let _guard = self.backend.object_lock.lock().await;
        let file = self
            .file
            .take()
            .context("object writer is already finalized")?;
        fault_point("after_temp_write");
        file.sync_all().await?;
        drop(file);
        let _process_lock = self
            .backend
            .acquire_process_lock(&self.namespace, &self.key)
            .await?;

        let digest = BlobDigest::from_sha256_digest(self.hasher.clone().finalize().into());
        let expected_size = BlobSize::new(self.size);
        let blob_path = self.backend.generic_blob_path(&digest);
        FilesystemPaddock::ensure_no_symlink(&blob_path).await?;
        if tokio::fs::try_exists(&blob_path).await? {
            let existing = tokio::fs::read(&blob_path).await?;
            if BlobDigest::from_bytes(&existing) != digest || existing.len() as u64 != self.size {
                bail!("immutable blob {} is corrupt", digest);
            }
            tokio::fs::remove_file(&self.temp).await?;
        } else {
            FilesystemPaddock::atomic_write_from_file(&self.temp, &blob_path).await?;
        }

        let current_path = self.backend.object_current_path(&self.namespace, &self.key);
        FilesystemPaddock::ensure_no_symlink(&current_path).await?;
        let current = if tokio::fs::try_exists(&current_path).await? {
            Some(self.backend.read_object_record(&current_path).await?)
        } else {
            None
        };
        match self.condition {
            RefCondition::Unconditional => {}
            RefCondition::Absent if current.is_some() => {
                return Err(CasConflict {
                    expected: None,
                    actual: current.as_ref().map(|value| value.version),
                }
                .into());
            }
            RefCondition::Absent => {}
            RefCondition::Version(expected) => {
                if Some(expected) != current.as_ref().map(|value| value.version) {
                    return Err(CasConflict {
                        expected: Some(expected),
                        actual: current.as_ref().map(|value| value.version),
                    }
                    .into());
                }
            }
        }
        let version = match current.as_ref() {
            Some(value) => value
                .version
                .get()
                .checked_add(1)
                .context("object version overflow")?,
            None => 1,
        };
        let record = ObjectRef {
            namespace: self.namespace.clone(),
            key: self.key.clone(),
            version: ObjectVersion::new(version),
            digest,
            size: expected_size,
            metadata: self.metadata.clone(),
            deleted: false,
        };
        let encoded = serde_json::to_vec_pretty(&record)?;
        FilesystemPaddock::atomic_write(
            &self
                .backend
                .object_version_path(&self.namespace, &self.key, record.version),
            &encoded,
        )
        .await?;
        FilesystemPaddock::atomic_write(&current_path, &encoded).await?;
        Ok(record)
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.file.take();
        let _ = tokio::fs::remove_file(&self.temp).await;
        Ok(())
    }
}

#[async_trait]
impl PaddockBackend for FilesystemPaddock {
    async fn has_blob(&self, digest: &ArtifactDigest) -> Result<bool> {
        let path = self.blob_path(digest);
        Self::ensure_no_symlink(&path).await?;
        Ok(tokio::fs::try_exists(path).await?)
    }

    async fn put_blob(&self, digest: &ArtifactDigest, bytes: &[u8]) -> Result<BlobPut> {
        if ArtifactDigest::from_wasm(bytes) != *digest {
            bail!("blob bytes do not match digest {}", digest);
        }
        let path = self.blob_path(digest);
        Self::ensure_no_symlink(&path).await?;
        if tokio::fs::try_exists(&path).await? {
            let existing = tokio::fs::read(&path).await?;
            if existing != bytes {
                bail!("immutable blob {} has different bytes", digest);
            }
            return Ok(BlobPut::AlreadyPresent);
        }
        Self::atomic_write(&path, bytes).await?;
        Ok(BlobPut::Uploaded)
    }

    async fn get_blob(&self, digest: &ArtifactDigest) -> Result<Vec<u8>> {
        let path = self.blob_path(digest);
        Self::ensure_no_symlink(&path).await?;
        tokio::fs::read(path)
            .await
            .with_context(|| format!("blob {} is unavailable", digest))
    }

    async fn put_manifest(
        &self,
        digest: &ArtifactDigest,
        manifest: &ArtifactManifest,
    ) -> Result<()> {
        if manifest.artifact.sha256 != digest.hex() {
            bail!("manifest digest does not match {}", digest);
        }
        let contents = manifest.to_json()?;
        let path = self.manifest_path(digest);
        Self::ensure_no_symlink(&path).await?;
        if tokio::fs::try_exists(&path).await? {
            if tokio::fs::read(&path).await? != contents.as_bytes() {
                bail!("immutable manifest {} has different metadata", digest);
            }
            return Ok(());
        }
        Self::atomic_write(&path, contents.as_bytes()).await
    }

    async fn get_manifest(&self, digest: &ArtifactDigest) -> Result<ArtifactManifest> {
        let path = self.manifest_path(digest);
        Self::ensure_no_symlink(&path).await?;
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("manifest for {} is unavailable", digest))?;
        let manifest = serde_json::from_slice(&bytes).context("malformed Paddock manifest")?;
        Ok(manifest)
    }

    async fn set_ref(&self, reference: &PaddockRef, digest: &ArtifactDigest) -> Result<()> {
        if !tokio::fs::try_exists(self.blob_path(digest)).await?
            || !tokio::fs::try_exists(self.manifest_path(digest)).await?
        {
            bail!("cannot publish ref {} before blob and manifest", reference);
        }
        Self::atomic_write(
            &self.ref_path(reference),
            serde_json::to_vec_pretty(&ResolvedRef {
                reference: PaddockRefWire::from(reference),
                digest: digest.clone(),
            })?
            .as_slice(),
        )
        .await
    }

    async fn resolve_ref(&self, reference: &PaddockRef) -> Result<ArtifactDigest> {
        let path = self.ref_path(reference);
        Self::ensure_no_symlink(&path).await?;
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("ref {} is unavailable", reference))?;
        let resolved: ResolvedRef =
            serde_json::from_slice(&bytes).context("malformed Paddock ref")?;
        let parsed = PaddockRef::try_from(resolved.reference)?;
        if parsed != *reference {
            bail!("Paddock ref metadata mismatch for {}", reference);
        }
        Ok(resolved.digest)
    }

    async fn list_refs(&self, name: Option<&ArtifactName>) -> Result<Vec<ResolvedRef>> {
        let root = self
            .root
            .join("refs")
            .join(name.map_or_else(PathBuf::new, |value| PathBuf::from(value.as_str())));
        let mut output: Vec<ResolvedRef> = Vec::new();
        let mut dirs = vec![root];
        while let Some(dir) = dirs.pop() {
            let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
                continue;
            };
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if entry.file_type().await?.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if path.extension().and_then(|value| value.to_str()) != Some("json") {
                    continue;
                }
                if let Ok(bytes) = tokio::fs::read(path).await
                    && let Ok(value) = serde_json::from_slice(&bytes)
                {
                    output.push(value);
                }
            }
        }
        output.sort_by(|a, b| {
            (a.reference.name.clone(), a.reference.tag.clone())
                .cmp(&(b.reference.name.clone(), b.reference.tag.clone()))
        });
        Ok(output)
    }
}

#[async_trait]
impl PaddockObjectBackend for FilesystemPaddock {
    fn capabilities(&self) -> PaddockCapabilities {
        PaddockCapabilities {
            range_read: true,
            streaming_write: true,
            atomic_ref_replace: true,
            conditional_ref_update: true,
            durable_sync: true,
        }
    }

    async fn put_blob_bytes(&self, bytes: &[u8]) -> Result<(BlobDigest, BlobSize)> {
        let digest = BlobDigest::from_bytes(bytes);
        let path = self.generic_blob_path(&digest);
        Self::ensure_no_symlink(&path).await?;
        if tokio::fs::try_exists(&path).await? {
            let existing = tokio::fs::read(&path).await?;
            if BlobDigest::from_bytes(&existing) != digest {
                bail!("immutable blob {} is corrupt", digest);
            }
        } else {
            Self::atomic_write(&path, bytes).await?;
        }
        Ok((digest, BlobSize::new(bytes.len() as u64)))
    }

    async fn get_blob_bytes(&self, digest: &BlobDigest) -> Result<Vec<u8>> {
        let path = self.generic_blob_path(digest);
        Self::ensure_no_symlink(&path).await?;
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("blob {} is unavailable", digest))?;
        if BlobDigest::from_bytes(&bytes) != *digest {
            bail!("blob {} failed integrity verification", digest);
        }
        Ok(bytes)
    }

    async fn read_blob_range(
        &self,
        digest: &BlobDigest,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let path = self.generic_blob_path(digest);
        Self::ensure_no_symlink(&path).await?;
        let mut file = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("blob {} is unavailable", digest))?;
        let size = file.metadata().await?.len();
        if offset > size {
            bail!("range offset {offset} exceeds blob size {size}");
        }
        let available = size - offset;
        let wanted = length.min(available);
        let capacity = usize::try_from(wanted).context("range is too large for this host")?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut bytes = vec![0; capacity];
        file.read_exact(&mut bytes).await?;
        Ok(bytes)
    }

    async fn begin_object_write(
        &self,
        namespace: NamespaceId,
        key: ObjectKey,
        metadata: ObjectMetadata,
        expected_version: Option<ObjectVersion>,
    ) -> Result<Box<dyn ObjectWriter>> {
        self.begin_object_write_with_condition(
            namespace,
            key,
            metadata,
            expected_version.map_or(RefCondition::Unconditional, RefCondition::Version),
        )
        .await
    }

    async fn begin_conditional_object_write(
        &self,
        namespace: NamespaceId,
        key: ObjectKey,
        metadata: ObjectMetadata,
        condition: RefCondition,
    ) -> Result<Box<dyn ObjectWriter>> {
        self.begin_object_write_with_condition(namespace, key, metadata, condition)
            .await
    }
    async fn get_object(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
        version: Option<ObjectVersion>,
    ) -> Result<ObjectRef> {
        let path = match version {
            Some(version) => self.object_version_path(namespace, key, version),
            None => self.object_current_path(namespace, key),
        };
        Self::ensure_no_symlink(&path).await?;
        let record = self.read_object_record(&path).await?;
        if record.namespace != *namespace || record.key != *key {
            bail!("object metadata does not match requested object");
        }
        if record.deleted && version.is_none() {
            bail!("object {namespace}/{key} is deleted");
        }
        Ok(record)
    }

    async fn delete_object(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
        expected_version: Option<ObjectVersion>,
    ) -> Result<()> {
        let _guard = self.object_lock.lock().await;
        let _process_lock = self.acquire_process_lock(namespace, key).await?;
        let current = self.get_object(namespace, key, None).await?;
        if let Some(expected) = expected_version
            && current.version != expected
        {
            return Err(CasConflict {
                expected: Some(expected),
                actual: Some(current.version),
            }
            .into());
        }
        let tombstone = ObjectRef {
            deleted: true,
            version: ObjectVersion::new(
                current
                    .version
                    .get()
                    .checked_add(1)
                    .context("object version overflow")?,
            ),
            ..current
        };
        let encoded = serde_json::to_vec_pretty(&tombstone)?;
        Self::atomic_write(
            &self.object_version_path(namespace, key, tombstone.version),
            &encoded,
        )
        .await?;
        Self::atomic_write(&self.object_current_path(namespace, key), &encoded).await
    }

    async fn list_objects(
        &self,
        namespace: &NamespaceId,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectRef>> {
        if let Some(prefix) = prefix {
            ObjectKey::validate_prefix(prefix)?;
        }
        let root = self.root.join("objects");
        let namespace_root = namespace
            .as_str()
            .split('/')
            .fold(root, |path, part| path.join(part));
        let mut dirs = vec![namespace_root];
        let mut objects = Vec::new();
        while let Some(dir) = dirs.pop() {
            let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
                continue;
            };
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if entry.file_type().await?.is_dir() {
                    if path.file_name().and_then(|value| value.to_str()) != Some("versions") {
                        dirs.push(path);
                    }
                    continue;
                }
                if path.file_name().and_then(|value| value.to_str()) != Some("current.json") {
                    continue;
                }
                Self::ensure_no_symlink(&path).await?;
                let value = self.read_object_record(&path).await?;
                if value.deleted || !value.namespace.eq(namespace) {
                    continue;
                }
                if prefix.is_none_or(|wanted| value.key.as_str().starts_with(wanted)) {
                    objects.push(value);
                }
            }
        }
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(objects)
    }
}

impl FilesystemPaddock {
    async fn begin_object_write_with_condition(
        &self,
        namespace: NamespaceId,
        key: ObjectKey,
        metadata: ObjectMetadata,
        condition: RefCondition,
    ) -> Result<Box<dyn ObjectWriter>> {
        let dir = self.object_dir(&namespace, &key);
        Self::ensure_no_symlink(&dir).await?;
        tokio::fs::create_dir_all(&dir).await?;
        Self::ensure_no_symlink(&dir).await?;
        let temp_dir = self.root.join("objects/.tmp");
        Self::ensure_no_symlink(&temp_dir).await?;
        tokio::fs::create_dir_all(&temp_dir).await?;
        Self::ensure_no_symlink(&temp_dir).await?;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let temp = temp_dir.join(format!(
            "object-{}-{}.tmp",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .await?;
        Ok(Box::new(FilesystemObjectWriter {
            backend: self.clone(),
            namespace,
            key,
            metadata,
            condition,
            temp,
            file: Some(file),
            hasher: Sha256::new(),
            size: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pit_artifact::{
        ArtifactFormat, ArtifactManifest, ArtifactSpec, BuildProfile, BuildSpec, Entrypoint,
        ExecutionDefaults, RuntimeAbi, RuntimeSpec, SCHEMA_VERSION,
    };
    use pit_paddock_core::{
        NamespaceId, ObjectKey, ObjectMetadata, ObjectVersion, PaddockBackend,
        PaddockObjectBackend, RefCondition, pull, push, put_object, put_object_if,
    };

    fn manifest(bytes: &[u8]) -> ArtifactManifest {
        ArtifactManifest {
            schema_version: SCHEMA_VERSION,
            artifact: ArtifactSpec {
                name: "demo".into(),
                path: "build/demo.wasm".into(),
                sha256: ArtifactDigest::from_wasm(bytes).hex().into(),
                size_bytes: bytes.len() as u64,
            },
            build: BuildSpec {
                language: "rust".into(),
                target: "wasm32-wasip1".into(),
                profile: BuildProfile::Release,
                fingerprint: "a".repeat(64),
                toolchain: None,
                toolchain_version: None,
                application_interface: None,
                adapter: None,
                adapter_digest: None,
            },
            runtime: RuntimeSpec {
                abi: RuntimeAbi::wasi_preview1(),
                entrypoint: Entrypoint::wasi_preview1(),
                format: ArtifactFormat::CoreModule,
                world: None,
            },
            execution: ExecutionDefaults::default(),
            capabilities: vec![],
        }
    }

    #[tokio::test]
    async fn stores_deduplicated_blobs_and_updates_refs() {
        let temp = tempfile::tempdir().unwrap();
        let store = FilesystemPaddock::new(temp.path());
        let bytes = b"\0asm\x01\0\0\0";
        let manifest = manifest(bytes);
        let reference: PaddockRef = "demo:v1".parse().unwrap();
        let (stored, first) = push(&store, &reference, manifest.clone(), bytes)
            .await
            .unwrap();
        assert_eq!(first, BlobPut::Uploaded);
        let (_, second) = push(&store, &"demo:latest".parse().unwrap(), manifest, bytes)
            .await
            .unwrap();
        assert_eq!(second, BlobPut::AlreadyPresent);
        assert_eq!(
            pull(&store, Some(&reference), None).await.unwrap().digest,
            stored.digest
        );
        assert_eq!(store.list_refs(None).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn streams_versions_ranges_and_tombstones_without_buffered_backend_api() {
        let temp = tempfile::tempdir().unwrap();
        let store = FilesystemPaddock::new(temp.path());
        let namespace = NamespaceId::new("tenant-a").unwrap();
        let key = ObjectKey::new("files/日本語.bin").unwrap();
        let metadata = ObjectMetadata {
            content_type: Some("application/octet-stream".into()),
            ..Default::default()
        };
        let mut writer = store
            .begin_object_write(namespace.clone(), key.clone(), metadata.clone(), None)
            .await
            .unwrap();
        writer.write_chunk(b"0123").await.unwrap();
        writer.write_chunk(b"456789").await.unwrap();
        let first = writer.commit().await.unwrap();
        assert_eq!(first.version.get(), 1);
        assert_eq!(first.size.get(), 10);
        assert_eq!(
            store.read_blob_range(&first.digest, 3, 4).await.unwrap(),
            b"3456"
        );

        let second = put_object(
            &store,
            namespace.clone(),
            key.clone(),
            metadata,
            b"replacement",
        )
        .await
        .unwrap();
        assert_eq!(second.version.get(), 2);
        assert_ne!(first.digest, second.digest);
        assert_eq!(
            store.get_object(&namespace, &key, None).await.unwrap(),
            second
        );
        assert_eq!(store.list_objects(&namespace, None).await.unwrap().len(), 1);

        store
            .delete_object(&namespace, &key, Some(second.version))
            .await
            .unwrap();
        assert!(store.get_object(&namespace, &key, None).await.is_err());
        assert!(
            store
                .list_objects(&namespace, None)
                .await
                .unwrap()
                .is_empty()
        );
        let historical = store
            .get_object(&namespace, &key, Some(first.version))
            .await
            .unwrap();
        assert!(!historical.deleted);
    }

    #[tokio::test]
    async fn failed_conditional_write_does_not_publish_a_new_version() {
        let temp = tempfile::tempdir().unwrap();
        let store = FilesystemPaddock::new(temp.path());
        let namespace = NamespaceId::new("tenant-a").unwrap();
        let key = ObjectKey::new("value").unwrap();
        let first = put_object(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            b"one",
        )
        .await
        .unwrap();
        let mut writer = store
            .begin_object_write(
                namespace.clone(),
                key.clone(),
                ObjectMetadata::default(),
                Some(ObjectVersion::new(99)),
            )
            .await
            .unwrap();
        writer.write_chunk(b"two").await.unwrap();
        assert!(writer.commit().await.is_err());
        assert_eq!(
            store.get_object(&namespace, &key, None).await.unwrap(),
            first
        );
    }

    #[tokio::test]
    async fn explicit_authority_conditions_cover_create_update_and_tombstone() {
        let temp = tempfile::tempdir().unwrap();
        let store = FilesystemPaddock::new(temp.path());
        let namespace = NamespaceId::new("tenant-a").unwrap();
        let key = ObjectKey::new("authority").unwrap();

        let first = put_object_if(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            RefCondition::Absent,
            b"one",
        )
        .await
        .unwrap();
        let absent_conflict = put_object_if(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            RefCondition::Absent,
            b"other",
        )
        .await;
        assert!(
            absent_conflict
                .unwrap_err()
                .downcast_ref::<pit_paddock_core::CasConflict>()
                .is_some()
        );

        let second = put_object_if(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            RefCondition::Version(first.version),
            b"two",
        )
        .await
        .unwrap();
        let stale = put_object_if(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            RefCondition::Version(first.version),
            b"stale",
        )
        .await;
        assert!(
            stale
                .unwrap_err()
                .downcast_ref::<pit_paddock_core::CasConflict>()
                .is_some()
        );

        store
            .delete_object_if(&namespace, &key, RefCondition::Version(second.version))
            .await
            .unwrap();
        let stale_delete = store
            .delete_object_if(&namespace, &key, RefCondition::Version(second.version))
            .await;
        assert!(stale_delete.is_err());
    }

    #[tokio::test]
    async fn aborted_write_is_not_visible() {
        let temp = tempfile::tempdir().unwrap();
        let store = FilesystemPaddock::new(temp.path());
        let namespace = NamespaceId::new("tenant-a").unwrap();
        let key = ObjectKey::new("partial").unwrap();
        let mut writer = store
            .begin_object_write(
                namespace.clone(),
                key.clone(),
                ObjectMetadata::default(),
                None,
            )
            .await
            .unwrap();
        writer.write_chunk(b"incomplete").await.unwrap();
        writer.abort().await.unwrap();
        assert!(store.get_object(&namespace, &key, None).await.is_err());
    }

    #[tokio::test]
    async fn object_state_survives_backend_restart_and_detects_corruption() {
        let temp = tempfile::tempdir().unwrap();
        let namespace = NamespaceId::new("tenant-a").unwrap();
        let key = ObjectKey::new("binary.dat").unwrap();
        let digest;
        {
            let store = FilesystemPaddock::new(temp.path());
            let object = put_object(
                &store,
                namespace.clone(),
                key.clone(),
                ObjectMetadata::default(),
                &[0, 1, 2, 255],
            )
            .await
            .unwrap();
            digest = object.digest;
        }

        let restarted = FilesystemPaddock::new(temp.path());
        let object = restarted.get_object(&namespace, &key, None).await.unwrap();
        assert_eq!(object.digest, digest);
        assert_eq!(
            restarted.get_blob_bytes(&digest).await.unwrap(),
            [0, 1, 2, 255]
        );
        assert_eq!(
            restarted
                .read_blob_range(&digest, 1, u64::MAX)
                .await
                .unwrap(),
            [1, 2, 255]
        );

        tokio::fs::write(restarted.generic_blob_path(&digest), b"corrupt")
            .await
            .unwrap();
        assert!(restarted.get_blob_bytes(&digest).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn object_paths_reject_symlink_escape_before_writing() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(root.path().join("objects/tenant-a"))
            .await
            .unwrap();
        symlink(outside.path(), root.path().join("objects/tenant-a/escape")).unwrap();
        let store = FilesystemPaddock::new(root.path());
        let result = store
            .begin_object_write(
                NamespaceId::new("tenant-a").unwrap(),
                ObjectKey::new("escape/object").unwrap(),
                ObjectMetadata::default(),
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(!outside.path().join("object").exists());
    }
}
