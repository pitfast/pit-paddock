//! Content-addressed storage contracts for PitFast artifacts.

use std::fmt;
use std::str::FromStr;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use pit_artifact::{ArtifactFormat, ArtifactManifest};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// Generic content identity for arbitrary durable Paddock bytes.
///
/// ArtifactDigest remains a separate public type because the artifact plane
/// has a stable API and additional WebAssembly validation rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobDigest(String);

impl BlobDigest {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    pub fn from_sha256_digest(digest: [u8; 32]) -> Self {
        let hex = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Self(format!("sha256:{hex}"))
    }

    pub fn hex(&self) -> &str {
        &self.0[7..]
    }
}

impl FromStr for BlobDigest {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 71
            || !value.starts_with("sha256:")
            || !value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
            || value[7..].bytes().any(|byte| byte.is_ascii_uppercase())
        {
            bail!("invalid blob digest '{value}'; expected sha256:<64 lowercase hex chars>");
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for BlobDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for BlobDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BlobDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobSize(u64);

impl BlobSize {
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A logical isolation boundary. It is deliberately independent of backend
/// paths and does not grant access by itself; authorization belongs to the
/// host capability layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespaceId(String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_logical_path(&value, 256, "namespace")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_logical_path(&value, 1024, "object key")?;
        Ok(Self(value))
    }

    /// Validates a list prefix. Unlike a complete key, a prefix may end in a
    /// separator, but it still cannot escape the namespace or name internal
    /// storage components.
    pub fn validate_prefix(value: &str) -> Result<()> {
        validate_logical_prefix(value, 1024, "object prefix")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectVersion(u64);

impl ObjectVersion {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasConflict {
    pub expected: Option<ObjectVersion>,
    pub actual: Option<ObjectVersion>,
}

impl std::fmt::Display for CasConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "conditional object update conflicted: expected {:?}, actual {:?}",
            self.expected.map(ObjectVersion::get),
            self.actual.map(ObjectVersion::get)
        )
    }
}

impl std::error::Error for CasConflict {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub content_type: Option<String>,
    #[serde(default)]
    pub custom: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub namespace: NamespaceId,
    pub key: ObjectKey,
    pub version: ObjectVersion,
    pub digest: BlobDigest,
    pub size: BlobSize,
    pub metadata: ObjectMetadata,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaddockCapabilities {
    pub range_read: bool,
    pub streaming_write: bool,
    pub atomic_ref_replace: bool,
    pub conditional_ref_update: bool,
    pub durable_sync: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaddockGrant {
    pub namespace: NamespaceId,
    pub read: bool,
    pub write: bool,
    pub delete: bool,
    pub list: bool,
}

/// Host-owned inbound gateway credentials. The secret is carried only in the
/// trusted request context and is never serialized into a guest artifact or
/// exposed through the generic storage capability.
#[derive(Clone)]
pub struct PaddockGatewayAuth {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl PaddockGrant {
    pub const fn read_write(namespace: NamespaceId) -> Self {
        Self {
            namespace,
            read: true,
            write: true,
            delete: false,
            list: false,
        }
    }

    pub const fn full(namespace: NamespaceId) -> Self {
        Self {
            namespace,
            read: true,
            write: true,
            delete: true,
            list: true,
        }
    }

    pub fn allows(&self, namespace: &NamespaceId, write: bool) -> bool {
        self.namespace == *namespace && if write { self.write } else { self.read }
    }
}

#[async_trait]
pub trait ObjectWriter: Send {
    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<()>;
    async fn commit(self: Box<Self>) -> Result<ObjectRef>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub trait PaddockObjectBackend: Send + Sync {
    fn capabilities(&self) -> PaddockCapabilities;
    async fn put_blob_bytes(&self, bytes: &[u8]) -> Result<(BlobDigest, BlobSize)>;
    async fn get_blob_bytes(&self, digest: &BlobDigest) -> Result<Vec<u8>>;
    async fn read_blob_range(
        &self,
        digest: &BlobDigest,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>>;
    async fn begin_object_write(
        &self,
        namespace: NamespaceId,
        key: ObjectKey,
        metadata: ObjectMetadata,
        expected_version: Option<ObjectVersion>,
    ) -> Result<Box<dyn ObjectWriter>>;
    async fn get_object(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
        version: Option<ObjectVersion>,
    ) -> Result<ObjectRef>;
    async fn delete_object(
        &self,
        namespace: &NamespaceId,
        key: &ObjectKey,
        expected_version: Option<ObjectVersion>,
    ) -> Result<()>;
    async fn list_objects(
        &self,
        namespace: &NamespaceId,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectRef>>;
}

pub async fn put_object<B: PaddockObjectBackend + ?Sized>(
    backend: &B,
    namespace: NamespaceId,
    key: ObjectKey,
    metadata: ObjectMetadata,
    bytes: &[u8],
) -> Result<ObjectRef> {
    let mut writer = backend
        .begin_object_write(namespace, key, metadata, None)
        .await?;
    writer.write_chunk(bytes).await?;
    writer.commit().await
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactDigest(String);

impl ArtifactDigest {
    pub fn from_wasm(bytes: &[u8]) -> Self {
        Self(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    pub fn hex(&self) -> &str {
        &self.0[7..]
    }
}

impl FromStr for ArtifactDigest {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 71
            || !value.starts_with("sha256:")
            || !value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
            || value[7..].bytes().any(|byte| byte.is_ascii_uppercase())
        {
            bail!("invalid artifact digest '{value}'; expected sha256:<64 lowercase hex chars>");
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for ArtifactDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ArtifactDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ArtifactDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactName(String);

impl ArtifactName {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value.starts_with('/')
            || value.ends_with('/')
            || value.split('/').any(|part| {
                part.is_empty() || part == "." || part == ".." || !valid_name_part(part)
            })
        {
            bail!("invalid artifact name '{value}'")
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArtifactName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactTag(String);

impl ArtifactTag {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value == "."
            || value == ".."
            || value.starts_with('/')
            || value.contains('/')
            || value.contains('\\')
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-')
            })
        {
            bail!("invalid artifact tag '{value}'")
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArtifactTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PaddockRef {
    pub name: ArtifactName,
    pub tag: ArtifactTag,
}

impl FromStr for PaddockRef {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (name, tag) = value
            .rsplit_once(':')
            .ok_or_else(|| anyhow!("invalid Paddock ref '{value}'; expected name:tag"))?;
        Ok(Self {
            name: ArtifactName::new(name)?,
            tag: ArtifactTag::new(tag)?,
        })
    }
}

impl fmt::Display for PaddockRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.tag)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedRef {
    pub reference: PaddockRefWire,
    pub digest: ArtifactDigest,
}

/// Serializable ref representation; typed parsing is applied at backend boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaddockRefWire {
    pub name: String,
    pub tag: String,
}

impl From<&PaddockRef> for PaddockRefWire {
    fn from(value: &PaddockRef) -> Self {
        Self {
            name: value.name.to_string(),
            tag: value.tag.to_string(),
        }
    }
}

impl TryFrom<PaddockRefWire> for PaddockRef {
    type Error = anyhow::Error;
    fn try_from(value: PaddockRefWire) -> Result<Self> {
        Ok(Self {
            name: ArtifactName::new(value.name)?,
            tag: ArtifactTag::new(value.tag)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredArtifact {
    pub digest: ArtifactDigest,
    pub manifest: ArtifactManifest,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobPut {
    Uploaded,
    AlreadyPresent,
}

#[async_trait]
pub trait PaddockBackend: Send + Sync {
    async fn has_blob(&self, digest: &ArtifactDigest) -> Result<bool>;
    async fn put_blob(&self, digest: &ArtifactDigest, bytes: &[u8]) -> Result<BlobPut>;
    async fn get_blob(&self, digest: &ArtifactDigest) -> Result<Vec<u8>>;
    async fn put_manifest(
        &self,
        digest: &ArtifactDigest,
        manifest: &ArtifactManifest,
    ) -> Result<()>;
    async fn get_manifest(&self, digest: &ArtifactDigest) -> Result<ArtifactManifest>;
    async fn set_ref(&self, reference: &PaddockRef, digest: &ArtifactDigest) -> Result<()>;
    async fn resolve_ref(&self, reference: &PaddockRef) -> Result<ArtifactDigest>;
    async fn list_refs(&self, name: Option<&ArtifactName>) -> Result<Vec<ResolvedRef>>;
}

pub async fn push<B: PaddockBackend + ?Sized>(
    backend: &B,
    reference: &PaddockRef,
    manifest: ArtifactManifest,
    bytes: &[u8],
) -> Result<(StoredArtifact, BlobPut)> {
    manifest.validate()?;
    let digest = ArtifactDigest::from_wasm(bytes);
    if manifest.artifact.sha256 != digest.hex() {
        bail!("manifest SHA-256 does not match artifact bytes")
    }
    if manifest.artifact.size_bytes != bytes.len() as u64 {
        bail!("manifest size does not match artifact bytes")
    }
    validate_artifact_format(&manifest, bytes)?;
    let blob_put = if backend.has_blob(&digest).await? {
        let existing = backend.get_blob(&digest).await?;
        if existing != bytes {
            bail!("immutable blob {} exists with different bytes", digest)
        }
        BlobPut::AlreadyPresent
    } else {
        backend.put_blob(&digest, bytes).await?
    };
    backend.put_manifest(&digest, &manifest).await?;
    backend.set_ref(reference, &digest).await?;
    Ok((
        StoredArtifact {
            digest,
            manifest,
            size_bytes: bytes.len() as u64,
        },
        blob_put,
    ))
}

pub async fn pull<B: PaddockBackend + ?Sized>(
    backend: &B,
    reference: Option<&PaddockRef>,
    digest: Option<&ArtifactDigest>,
) -> Result<StoredArtifact> {
    let digest = match (reference, digest) {
        (Some(reference), None) => backend.resolve_ref(reference).await?,
        (None, Some(digest)) => digest.clone(),
        _ => bail!("provide exactly one Paddock ref or digest"),
    };
    let manifest = backend.get_manifest(&digest).await?;
    let bytes = backend.get_blob(&digest).await?;
    manifest.validate()?;
    if manifest.artifact.sha256 != digest.hex() {
        bail!("manifest digest mismatch for {}", digest)
    }
    if manifest.artifact.size_bytes != bytes.len() as u64 {
        bail!("artifact size mismatch for {}", digest)
    }
    if ArtifactDigest::from_wasm(&bytes) != digest {
        bail!("artifact digest mismatch for {}", digest)
    }
    Ok(StoredArtifact {
        digest,
        manifest,
        size_bytes: bytes.len() as u64,
    })
}

fn valid_name_part(value: &str) -> bool {
    value.bytes().enumerate().all(|(index, byte)| {
        (byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-'))
            && (index != 0 || byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

fn validate_logical_path(value: &str, max_len: usize, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > max_len
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.chars().any(|character| character.is_control())
        || value.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part == "versions"
                || part == "current.json"
                || part == ".tmp"
        })
    {
        bail!("invalid {label} '{value}'")
    }
    Ok(())
}

fn validate_logical_prefix(value: &str, max_len: usize, label: &str) -> Result<()> {
    if value.len() > max_len
        || value.starts_with('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.chars().any(|character| character.is_control())
    {
        bail!("invalid {label} '{value}'")
    }
    let components = value.split('/').collect::<Vec<_>>();
    if components.iter().enumerate().any(|(index, part)| {
        let last_empty = index + 1 == components.len() && part.is_empty();
        (!last_empty && (part.is_empty() || *part == "." || *part == ".."))
            || *part == "versions"
            || *part == "current.json"
            || *part == ".tmp"
    }) {
        bail!("invalid {label} '{value}'")
    }
    Ok(())
}

fn validate_artifact_format(manifest: &ArtifactManifest, bytes: &[u8]) -> Result<()> {
    for payload in wasmparser::Parser::new(0).parse_all(bytes) {
        if let wasmparser::Payload::Version { encoding, .. } = payload? {
            let actual = match encoding {
                wasmparser::Encoding::Module => ArtifactFormat::CoreModule,
                wasmparser::Encoding::Component => ArtifactFormat::Component,
            };
            if actual != manifest.runtime.format {
                bail!(
                    "artifact format does not match manifest: manifest {}, actual {}",
                    manifest.runtime.format,
                    actual
                );
            }
            return Ok(());
        }
    }
    bail!("artifact is not a valid WebAssembly binary")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_canonical() {
        let digest = ArtifactDigest::from_wasm(b"hello");
        assert_eq!(
            digest.to_string(),
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert!("sha256:ABC".parse::<ArtifactDigest>().is_err());
    }

    #[test]
    fn refs_are_safe_and_typed() {
        let reference: PaddockRef = "team-a/service-a:v1.0.0".parse().unwrap();
        assert_eq!(reference.to_string(), "team-a/service-a:v1.0.0");
        assert!("../service:v1".parse::<PaddockRef>().is_err());
        assert!("service:/tmp".parse::<PaddockRef>().is_err());
        assert!("SERVICE:v1".parse::<PaddockRef>().is_err());
    }

    #[test]
    fn generic_object_identity_and_paths_are_safe() {
        assert_eq!(BlobDigest::from_bytes(b"hello").hex().len(), 64);
        assert!(NamespaceId::new("tenant/a").is_ok());
        assert!(ObjectKey::new("receipts/é/2026.json").is_ok());
        for value in ["", "../tenant", "/tenant", "tenant/../x", "tenant\\x"] {
            assert!(NamespaceId::new(value).is_err(), "accepted {value:?}");
        }
        for value in ["", "../secret", "/etc/passwd", "a/./b", "a\\b"] {
            assert!(ObjectKey::new(value).is_err(), "accepted {value:?}");
        }
        for value in ["versions/file", "current.json", ".tmp/file"] {
            assert!(
                ObjectKey::new(value).is_err(),
                "accepted reserved {value:?}"
            );
        }
        assert!(ObjectKey::validate_prefix("receipts/2026/").is_ok());
        assert!(ObjectKey::validate_prefix("../").is_err());
    }
}
