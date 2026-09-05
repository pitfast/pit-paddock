//! Vendor-neutral S3-compatible Paddock backend.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pit_artifact::ArtifactManifest;
use pit_paddock_core::{
    ArtifactDigest, ArtifactName, BlobPut, PaddockBackend, PaddockRef, PaddockRefWire, ResolvedRef,
};
use s3::{AddressingStyle, Auth, Client, Credentials};

#[derive(Debug, Clone)]
pub struct S3PaddockConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_env: String,
    pub secret_key_env: String,
    pub session_token_env: Option<String>,
}

#[derive(Clone)]
pub struct S3Paddock {
    client: Arc<Client>,
    bucket: String,
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
}

#[async_trait]
impl PaddockBackend for S3Paddock {
    async fn has_blob(&self, digest: &ArtifactDigest) -> Result<bool> {
        Ok(self
            .client
            .objects()
            .head(&self.bucket, Self::blob_key(digest))
            .send()
            .await
            .is_ok())
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
        if let Ok(existing) = self.get_manifest(digest).await {
            if existing != *manifest {
                bail!("immutable manifest {} has different metadata", digest);
            }
            return Ok(());
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
        if !self.has_blob(digest).await? || self.get_manifest(digest).await.is_err() {
            bail!("cannot publish ref before blob {}", digest);
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
        };
        let redacted = redact_config(&config);
        assert!(redacted.contains("credentials=redacted"));
        assert!(!redacted.contains("PIT_SECRET"));
    }
}
