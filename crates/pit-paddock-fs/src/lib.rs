//! Atomic, content-addressed filesystem Paddock backend.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use pit_artifact::ArtifactManifest;
use pit_paddock_core::{
    ArtifactDigest, ArtifactName, BlobPut, PaddockBackend, PaddockRef, PaddockRefWire, ResolvedRef,
};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct FilesystemPaddock {
    root: PathBuf,
}

impl FilesystemPaddock {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
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

    async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
        let parent = path.parent().context("Paddock path has no parent")?;
        tokio::fs::create_dir_all(parent).await?;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let temp = parent.join(format!(
            ".{}.{}.tmp",
            path.file_name().unwrap().to_string_lossy(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .await?;
            file.write_all(bytes).await?;
            file.sync_all().await?;
            tokio::fs::rename(&temp, path).await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temp).await;
        }
        result
    }
}

#[async_trait]
impl PaddockBackend for FilesystemPaddock {
    async fn has_blob(&self, digest: &ArtifactDigest) -> Result<bool> {
        Ok(tokio::fs::try_exists(self.blob_path(digest)).await?)
    }

    async fn put_blob(&self, digest: &ArtifactDigest, bytes: &[u8]) -> Result<BlobPut> {
        if ArtifactDigest::from_wasm(bytes) != *digest {
            bail!("blob bytes do not match digest {}", digest);
        }
        let path = self.blob_path(digest);
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
        tokio::fs::read(self.blob_path(digest))
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
        if tokio::fs::try_exists(&path).await? {
            if tokio::fs::read(&path).await? != contents.as_bytes() {
                bail!("immutable manifest {} has different metadata", digest);
            }
            return Ok(());
        }
        Self::atomic_write(&path, contents.as_bytes()).await
    }

    async fn get_manifest(&self, digest: &ArtifactDigest) -> Result<ArtifactManifest> {
        let bytes = tokio::fs::read(self.manifest_path(digest))
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
        let bytes = tokio::fs::read(self.ref_path(reference))
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

#[cfg(test)]
mod tests {
    use super::*;
    use pit_artifact::{
        ArtifactFormat, ArtifactManifest, ArtifactSpec, BuildProfile, BuildSpec, Entrypoint,
        ExecutionDefaults, RuntimeAbi, RuntimeSpec, SCHEMA_VERSION,
    };
    use pit_paddock_core::{PaddockBackend, pull, push};

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
}
