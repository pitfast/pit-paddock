//! Vendor-neutral S3-compatible Paddock backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;
use pit_artifact::ArtifactManifest;
use pit_paddock_core::{
    ArtifactDigest, ArtifactName, BackendCapabilityUnsupported, BlobDigest, BlobPut, BlobSize,
    CasConflict, NamespaceId, ObjectKey, ObjectMetadata, ObjectRef, ObjectVersion, ObjectWriter,
    PaddockBackend, PaddockCapabilities, PaddockObjectBackend, PaddockRef, PaddockRefWire,
    RefCondition, ResolvedRef,
};
use s3::{AddressingStyle, Auth, Client, Credentials};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct S3PaddockConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_env: String,
    pub secret_key_env: String,
    pub session_token_env: Option<String>,
    /// Unknown S3-compatible endpoints default to false. Enabling this is an
    /// explicit provider profile declaration that If-Match/If-None-Match on
    /// the ref object is supported atomically.
    pub conditional_ref_update: bool,
}

#[derive(Clone)]
pub struct S3Paddock {
    client: Arc<Client>,
    bucket: String,
    object_lock: Arc<tokio::sync::Mutex<()>>,
    conditional_ref_update: bool,
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<s3::Error>().is_some_and(
            |remote| matches!(remote, s3::Error::Api { status, .. } if status.as_u16() == 404),
        )
    })
}

impl S3Paddock {
    pub fn from_config(config: S3PaddockConfig) -> Result<Self> {
        let access = std::env::var(&config.access_key_env).with_context(|| {
            format!(
                "S3 access key environment variable {} is not set",
                config.access_key_env
            )
        })?;
        let secret = std::env::var(&config.secret_key_env).with_context(|| {
            format!(
                "S3 secret key environment variable {} is not set",
                config.secret_key_env
            )
        })?;
        let token = config
            .session_token_env
            .as_ref()
            .and_then(|name| std::env::var(name).ok());
        let mut credentials = Credentials::new(&access, &secret)
            .map_err(|error| anyhow!("invalid S3 credentials: {error}"))?;
        if let Some(token) = token {
            credentials = credentials
                .with_session_token(token)
                .map_err(|error| anyhow!("invalid S3 session token: {error}"))?;
        }
        let client = Client::builder(&config.endpoint)?
            .region(config.region)
            .auth(Auth::Static(credentials))
            .addressing_style(AddressingStyle::Path)
            .build()?;
        Ok(Self {
            client: Arc::new(client),
            bucket: config.bucket,
            object_lock: Arc::new(tokio::sync::Mutex::new(())),
            conditional_ref_update: config.conditional_ref_update,
        })
    }

    fn blob_key(digest: &ArtifactDigest) -> String {
        format!("blobs/sha256/{}/{}.wasm", &digest.hex()[..2], digest.hex())
    }
    fn manifest_key(digest: &ArtifactDigest) -> String {
        format!("manifests/sha256/{}.json", digest.hex())
    }
    fn ref_key(reference: &PaddockRef) -> String {
        format!("refs/{}/{}.json", reference.name, reference.tag)
    }

    fn generic_blob_key(digest: &BlobDigest) -> String {
        format!("blobs/sha256/{}/{}.blob", &digest.hex()[..2], digest.hex())
    }

    fn object_key(namespace: &NamespaceId, key: &ObjectKey, suffix: &str) -> String {
        format!("objects/{namespace}/{key}/{suffix}")
    }

    async fn read_object_record_with_etag(
        &self,
        key: String,
    ) -> Result<(ObjectRef, Option<String>)> {
        let output = self.client.objects().get(&self.bucket, key).send().await?;
        let etag = output.etag.clone();
        let bytes = output.bytes().await?;
        Ok((
            serde_json::from_slice(&bytes).context("malformed remote Paddock object ref")?,
            etag,
        ))
    }

    async fn read_object_record(&self, key: String) -> Result<ObjectRef> {
        Ok(self.read_object_record_with_etag(key).await?.0)
    }

    async fn read_optional_object_record(
        &self,
        key: String,
    ) -> Result<Option<(ObjectRef, Option<String>)>> {
        match self.read_object_record_with_etag(key).await {
            Ok(value) => Ok(Some(value)),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn put_object_record(
        &self,
        key: String,
        value: &ObjectRef,
        if_match: Option<&str>,
        if_none_match: bool,
    ) -> Result<()> {
        let mut request = self
            .client
            .objects()
            .put(&self.bucket, key)
            .body_bytes(serde_json::to_vec(value)?)
            .content_type("application/json")?;
        if let Some(etag) = if_match {
            request = request.if_match(etag)?;
        } else if if_none_match {
            request = request.if_none_match("*")?;
        }
        request.send().await?;
        Ok(())
    }

    fn unsupported_conditional_refs() -> anyhow::Error {
        BackendCapabilityUnsupported {
            backend: "s3-compatible".into(),
            capability: "conditional_ref_update",
        }
        .into()
    }

    fn is_precondition_failure(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            cause.downcast_ref::<s3::Error>().is_some_and(|remote| {
                remote
                    .status()
                    .is_some_and(|status| matches!(status.as_u16(), 409 | 412))
            })
        })
    }

    async fn current_version(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
    ) -> Option<ObjectVersion> {
        self.read_optional_object_record(Self::object_key(namespace, key, "current.json"))
            .await
            .ok()
            .flatten()
            .map(|(record, _)| record.version)
    }

    async fn object_exists(&self, key: String) -> Result<bool> {
        match self.client.objects().head(&self.bucket, key).send().await {
            Ok(_) => Ok(true),
            Err(error) => {
                let error: anyhow::Error = error.into();
                if is_not_found(&error) {
                    Ok(false)
                } else {
                    Err(error).context("S3 HEAD failed while checking object existence")
                }
            }
        }
    }
}

struct S3ObjectWriter {
    backend: S3Paddock,
    namespace: NamespaceId,
    key: ObjectKey,
    metadata: ObjectMetadata,
    condition: RefCondition,
    temp: std::path::PathBuf,
    file: Option<tokio::fs::File>,
    hasher: Sha256,
    size: u64,
}

impl Drop for S3ObjectWriter {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.temp);
    }
}

#[async_trait]
impl ObjectWriter for S3ObjectWriter {
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
        file.sync_all().await?;
        drop(file);
        let digest = BlobDigest::from_sha256_digest(self.hasher.clone().finalize().into());
        let blob_key = S3Paddock::generic_blob_key(&digest);
        if !self.backend.object_exists(blob_key.clone()).await? {
            let file = tokio::fs::File::open(&self.temp).await?;
            let stream = stream::unfold(file, |mut file| async move {
                let mut buffer = vec![0_u8; 64 * 1024];
                match file.read(&mut buffer).await {
                    Ok(0) => None,
                    Ok(size) => Some((
                        Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&buffer[..size])),
                        file,
                    )),
                    Err(error) => Some((Err(error), file)),
                }
            });
            self.backend
                .client
                .objects()
                .put(&self.backend.bucket, blob_key)
                .body_stream_sized(stream, self.size)
                .send()
                .await?;
        }

        let current_key = S3Paddock::object_key(&self.namespace, &self.key, "current.json");
        let current = self
            .backend
            .read_optional_object_record(current_key.clone())
            .await?;
        let current_record = current.as_ref().map(|(value, _)| value);
        let current_etag = current.as_ref().and_then(|(_, etag)| etag.as_deref());
        match self.condition {
            RefCondition::Unconditional => {}
            RefCondition::Absent if current_record.is_some() => {
                return Err(CasConflict {
                    expected: None,
                    actual: current_record.map(|value| value.version),
                }
                .into());
            }
            RefCondition::Absent => {}
            RefCondition::Version(expected) => {
                if Some(expected) != current_record.map(|value| value.version) {
                    return Err(CasConflict {
                        expected: Some(expected),
                        actual: current_record.map(|value| value.version),
                    }
                    .into());
                }
                if current_etag.is_none() {
                    return Err(S3Paddock::unsupported_conditional_refs());
                }
            }
        }
        let version = match current_record {
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
            size: BlobSize::new(self.size),
            metadata: self.metadata.clone(),
            deleted: false,
        };
        let version_key = S3Paddock::object_key(
            &self.namespace,
            &self.key,
            &format!("versions/{}.json", version),
        );
        match self.condition {
            RefCondition::Unconditional => {
                self.backend
                    .put_object_record(version_key, &record, None, false)
                    .await?;
                self.backend
                    .put_object_record(current_key, &record, None, false)
                    .await?;
            }
            RefCondition::Absent | RefCondition::Version(_) => {
                match self
                    .backend
                    .put_object_record(version_key.clone(), &record, None, true)
                    .await
                {
                    Ok(()) => {}
                    Err(error) if S3Paddock::is_precondition_failure(&error) => {
                        let existing = self.backend.read_object_record(version_key).await?;
                        if existing != record {
                            return Err(CasConflict {
                                expected: match self.condition {
                                    RefCondition::Absent => None,
                                    RefCondition::Version(expected) => Some(expected),
                                    RefCondition::Unconditional => None,
                                },
                                actual: current_record.map(|value| value.version),
                            }
                            .into());
                        }
                    }
                    Err(error) => return Err(error),
                }
                let result = match self.condition {
                    RefCondition::Absent => {
                        self.backend
                            .put_object_record(current_key, &record, None, true)
                            .await
                    }
                    RefCondition::Version(_) => {
                        self.backend
                            .put_object_record(current_key, &record, current_etag, false)
                            .await
                    }
                    RefCondition::Unconditional => unreachable!(),
                };
                if let Err(error) = result {
                    if S3Paddock::is_precondition_failure(&error) {
                        return Err(CasConflict {
                            expected: match self.condition {
                                RefCondition::Absent => None,
                                RefCondition::Version(expected) => Some(expected),
                                RefCondition::Unconditional => None,
                            },
                            actual: self
                                .backend
                                .current_version(&self.namespace, &self.key)
                                .await,
                        }
                        .into());
                    }
                    return Err(error);
                }
            }
        }
        Ok(record)
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.file.take();
        let _ = tokio::fs::remove_file(&self.temp).await;
        Ok(())
    }
}

#[async_trait]
impl PaddockBackend for S3Paddock {
    async fn has_blob(&self, digest: &ArtifactDigest) -> Result<bool> {
        self.object_exists(Self::blob_key(digest)).await
    }

    async fn put_blob(&self, digest: &ArtifactDigest, bytes: &[u8]) -> Result<BlobPut> {
        if ArtifactDigest::from_wasm(bytes) != *digest {
            bail!("blob bytes do not match digest {}", digest);
        }
        if self.has_blob(digest).await? {
            return Ok(BlobPut::AlreadyPresent);
        }
        self.client
            .objects()
            .put(&self.bucket, Self::blob_key(digest))
            .body_bytes(bytes.to_vec())
            .content_length(bytes.len() as u64)
            .send()
            .await?;
        Ok(BlobPut::Uploaded)
    }

    async fn get_blob(&self, digest: &ArtifactDigest) -> Result<Vec<u8>> {
        Ok(self
            .client
            .objects()
            .get(&self.bucket, Self::blob_key(digest))
            .send()
            .await?
            .bytes()
            .await?
            .to_vec())
    }

    async fn put_manifest(
        &self,
        digest: &ArtifactDigest,
        manifest: &ArtifactManifest,
    ) -> Result<()> {
        if manifest.artifact.sha256 != digest.hex() {
            bail!("manifest digest does not match {}", digest);
        }
        match self.get_manifest(digest).await {
            Ok(existing) => {
                if existing != *manifest {
                    bail!("immutable manifest {} has different metadata", digest);
                }
                return Ok(());
            }
            Err(error) if !is_not_found(&error) => {
                return Err(error).context("S3 manifest lookup failed before upload");
            }
            Err(_) => {}
        }
        self.client
            .objects()
            .put(&self.bucket, Self::manifest_key(digest))
            .body_bytes(manifest.to_json()?.into_bytes())
            .content_type("application/json")?
            .send()
            .await?;
        Ok(())
    }

    async fn get_manifest(&self, digest: &ArtifactDigest) -> Result<ArtifactManifest> {
        let bytes = self
            .client
            .objects()
            .get(&self.bucket, Self::manifest_key(digest))
            .send()
            .await?
            .bytes()
            .await?;
        Ok(serde_json::from_slice(&bytes).context("malformed remote Paddock manifest")?)
    }

    async fn set_ref(&self, reference: &PaddockRef, digest: &ArtifactDigest) -> Result<()> {
        if !self.has_blob(digest).await? {
            bail!("cannot publish ref before blob {}", digest);
        }
        match self.get_manifest(digest).await {
            Ok(_) => {}
            Err(error) if is_not_found(&error) => {
                bail!("cannot publish ref before manifest {}", digest);
            }
            Err(error) => {
                return Err(error).context("S3 manifest lookup failed before ref publish");
            }
        }
        self.client
            .objects()
            .put(&self.bucket, Self::ref_key(reference))
            .body_bytes(serde_json::to_vec(&ResolvedRef {
                reference: PaddockRefWire::from(reference),
                digest: digest.clone(),
            })?)
            .content_type("application/json")?
            .send()
            .await?;
        Ok(())
    }

    async fn resolve_ref(&self, reference: &PaddockRef) -> Result<ArtifactDigest> {
        let bytes = self
            .client
            .objects()
            .get(&self.bucket, Self::ref_key(reference))
            .send()
            .await?
            .bytes()
            .await?;
        let value: ResolvedRef =
            serde_json::from_slice(&bytes).context("malformed remote Paddock ref")?;
        if PaddockRef::try_from(value.reference.clone())? != *reference {
            bail!("remote Paddock ref metadata mismatch");
        }
        Ok(value.digest)
    }

    async fn list_refs(&self, name: Option<&ArtifactName>) -> Result<Vec<ResolvedRef>> {
        let prefix = name.map_or_else(|| "refs/".to_owned(), |value| format!("refs/{value}/"));
        let mut pager = self
            .client
            .objects()
            .list_v2(&self.bucket)
            .prefix(prefix)?
            .pager();
        let mut refs = Vec::new();
        while let Some(page) = pager.next_page().await? {
            for object in page.contents {
                if let Ok(bytes) = self
                    .client
                    .objects()
                    .get(&self.bucket, object.key)
                    .send()
                    .await?
                    .bytes()
                    .await
                    && let Ok(value) = serde_json::from_slice(&bytes)
                {
                    refs.push(value);
                }
            }
        }
        refs.sort_by(|a: &ResolvedRef, b: &ResolvedRef| {
            (a.reference.name.clone(), a.reference.tag.clone())
                .cmp(&(b.reference.name.clone(), b.reference.tag.clone()))
        });
        Ok(refs)
    }
}

#[async_trait]
impl PaddockObjectBackend for S3Paddock {
    fn capabilities(&self) -> PaddockCapabilities {
        PaddockCapabilities {
            range_read: true,
            streaming_write: true,
            // This is true only for an explicitly configured provider profile
            // whose conditional PUT behavior has been tested/accepted.
            conditional_ref_update: self.conditional_ref_update,
            atomic_ref_replace: false,
            durable_sync: true,
        }
    }

    async fn put_blob_bytes(&self, bytes: &[u8]) -> Result<(BlobDigest, BlobSize)> {
        let digest = BlobDigest::from_bytes(bytes);
        let key = Self::generic_blob_key(&digest);
        if !self.object_exists(key.clone()).await? {
            self.client
                .objects()
                .put(&self.bucket, key)
                .body_bytes(bytes.to_vec())
                .content_length(bytes.len() as u64)
                .send()
                .await?;
        } else if self.get_blob_bytes(&digest).await? != bytes {
            bail!("immutable blob {} has different bytes", digest);
        }
        Ok((digest, BlobSize::new(bytes.len() as u64)))
    }

    async fn get_blob_bytes(&self, digest: &BlobDigest) -> Result<Vec<u8>> {
        let bytes = self
            .client
            .objects()
            .get(&self.bucket, Self::generic_blob_key(digest))
            .send()
            .await?
            .bytes()
            .await?
            .to_vec();
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
        let head = self
            .client
            .objects()
            .head(&self.bucket, Self::generic_blob_key(digest))
            .send()
            .await?;
        let size = head.content_length.unwrap_or(0);
        if offset > size {
            bail!("range offset {offset} exceeds blob size {size}");
        }
        if offset == size || length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length - 1)
            .context("range end overflow")?
            .min(size - 1);
        Ok(self
            .client
            .objects()
            .get(&self.bucket, Self::generic_blob_key(digest))
            .range_bytes(offset, end)?
            .send()
            .await?
            .bytes()
            .await?
            .to_vec())
    }

    async fn begin_object_write(
        &self,
        namespace: NamespaceId,
        key: ObjectKey,
        metadata: ObjectMetadata,
        expected_version: Option<ObjectVersion>,
    ) -> Result<Box<dyn ObjectWriter>> {
        if expected_version.is_some() && !self.conditional_ref_update {
            return Err(Self::unsupported_conditional_refs());
        }
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
        if !self.conditional_ref_update && condition != RefCondition::Unconditional {
            return Err(Self::unsupported_conditional_refs());
        }
        self.begin_object_write_with_condition(namespace, key, metadata, condition)
            .await
    }
    async fn get_object(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
        version: Option<ObjectVersion>,
    ) -> Result<ObjectRef> {
        let suffix = match version {
            Some(version) => format!("versions/{}.json", version.get()),
            None => "current.json".into(),
        };
        let record = self
            .read_object_record(Self::object_key(namespace, key, &suffix))
            .await?;
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
        if expected_version.is_some() && !self.conditional_ref_update {
            return Err(Self::unsupported_conditional_refs());
        }
        let _guard = self.object_lock.lock().await;
        let current_key = Self::object_key(namespace, key, "current.json");
        let Some((current, current_etag)) = self
            .read_optional_object_record(current_key.clone())
            .await?
        else {
            bail!("object {namespace}/{key} is unavailable");
        };
        if current.deleted {
            bail!("object {namespace}/{key} is deleted");
        }
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
        let version_key = Self::object_key(
            namespace,
            key,
            &format!("versions/{}.json", tombstone.version.get()),
        );
        if let Some(expected) = expected_version {
            self.put_object_record(version_key, &tombstone, None, true)
                .await?;
            let etag = current_etag
                .as_deref()
                .ok_or_else(Self::unsupported_conditional_refs)?;
            if let Err(error) = self
                .put_object_record(current_key, &tombstone, Some(etag), false)
                .await
            {
                if Self::is_precondition_failure(&error) {
                    return Err(CasConflict {
                        expected: Some(expected),
                        actual: self.current_version(namespace, key).await,
                    }
                    .into());
                }
                return Err(error);
            }
            Ok(())
        } else {
            self.put_object_record(version_key, &tombstone, None, false)
                .await?;
            self.put_object_record(current_key, &tombstone, None, false)
                .await
        }
    }

    async fn list_objects(
        &self,
        namespace: &NamespaceId,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectRef>> {
        if let Some(prefix) = prefix {
            ObjectKey::validate_prefix(prefix)?;
        }
        let mut pager = self
            .client
            .objects()
            .list_v2(&self.bucket)
            .prefix(format!("objects/{namespace}/"))?
            .pager();
        let mut output = Vec::new();
        while let Some(page) = pager.next_page().await? {
            for item in page.contents {
                if !item.key.ends_with("/current.json") {
                    continue;
                }
                let Ok(value) = self.read_object_record(item.key).await else {
                    continue;
                };
                if value.deleted || !value.namespace.eq(namespace) {
                    continue;
                }
                if prefix.is_none_or(|wanted| value.key.as_str().starts_with(wanted)) {
                    output.push(value);
                }
            }
        }
        output.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(output)
    }
}

impl S3Paddock {
    async fn begin_object_write_with_condition(
        &self,
        namespace: NamespaceId,
        key: ObjectKey,
        metadata: ObjectMetadata,
        condition: RefCondition,
    ) -> Result<Box<dyn ObjectWriter>> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let temp = std::env::temp_dir().join(format!(
            "pit-paddock-s3-object-{}-{}.tmp",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .await?;
        Ok(Box::new(S3ObjectWriter {
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

pub fn redact_config(config: &S3PaddockConfig) -> String {
    format!(
        "endpoint={}, bucket={}, region={}, credentials=redacted",
        config.endpoint, config.bucket, config.region
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_keys_are_digest_derived_and_credentials_are_redacted() {
        let digest = ArtifactDigest::from_wasm(b"artifact");
        let reference: PaddockRef = "team-a/service-a:v1".parse().unwrap();
        assert_eq!(
            S3Paddock::blob_key(&digest),
            format!("blobs/sha256/{}/{}.wasm", &digest.hex()[..2], digest.hex())
        );
        assert_eq!(
            S3Paddock::manifest_key(&digest),
            format!("manifests/sha256/{}.json", digest.hex())
        );
        assert_eq!(
            S3Paddock::ref_key(&reference),
            "refs/team-a/service-a/v1.json"
        );
        let config = S3PaddockConfig {
            endpoint: "https://s3.example.test".into(),
            bucket: "artifacts".into(),
            region: "us-east-1".into(),
            access_key_env: "PIT_ACCESS".into(),
            secret_key_env: "PIT_SECRET".into(),
            session_token_env: None,
            conditional_ref_update: false,
        };
        let redacted = redact_config(&config);
        assert!(redacted.contains("credentials=redacted"));
        assert!(!redacted.contains("PIT_SECRET"));
    }
}
