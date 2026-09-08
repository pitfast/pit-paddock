//! Shared named-Paddock configuration and construction.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use pit_paddock_core::{PaddockBackend, PaddockObjectBackend};
use pit_paddock_fs::FilesystemPaddock;
use pit_paddock_s3::{S3Paddock, S3PaddockConfig};
use serde::{Deserialize, Serialize};

pub type PaddockHandle = Arc<dyn PaddockBackend>;
pub type PaddockObjectHandle = Arc<dyn PaddockObjectBackend>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PaddockConfig {
    pub paddocks: BTreeMap<String, NamedPaddockConfig>,
    pub deployment: DeploymentPaddockConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum NamedPaddockConfig {
    Filesystem {
        path: Option<PathBuf>,
    },
    S3 {
        endpoint: String,
        bucket: String,
        #[serde(default = "default_region")]
        region: String,
        #[serde(default = "default_access_key_env")]
        access_key_env: String,
        #[serde(default = "default_secret_key_env")]
        secret_key_env: String,
        #[serde(default)]
        session_token_env: Option<String>,
    },
}

impl Default for NamedPaddockConfig {
    fn default() -> Self {
        Self::Filesystem { path: None }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DeploymentPaddockConfig {
    pub default_paddock: Option<String>,
}

fn default_region() -> String {
    "auto".into()
}
fn default_access_key_env() -> String {
    "PIT_PADDOCK_ACCESS_KEY".into()
}
fn default_secret_key_env() -> String {
    "PIT_PADDOCK_SECRET_KEY".into()
}

impl PaddockConfig {
    pub fn with_defaults() -> Self {
        let mut config = Self::default();
        config
            .paddocks
            .insert("local".into(), NamedPaddockConfig::default());
        config.deployment.default_paddock = Some("local".into());
        config
    }

    /// Loads user configuration and then project configuration. Project values win.
    pub fn load(project_dir: &Path) -> Result<Self> {
        let mut value = toml::Value::try_from(Self::with_defaults())?;
        if let Some(user) = user_config_path() {
            merge_toml(&mut value, read_toml(&user)?);
        }
        let project = project_dir.join("pit.toml");
        if project.is_file() {
            merge_toml(&mut value, read_toml(&project)?);
        }
        Ok(value.try_into()?)
    }

    pub fn open(
        &self,
        name: &str,
        explicit_filesystem_root: Option<PathBuf>,
    ) -> Result<PaddockHandle> {
        if let Some(root) = explicit_filesystem_root {
            return Ok(Arc::new(FilesystemPaddock::new(root)));
        }
        let definition = self
            .paddocks
            .get(name)
            .with_context(|| format!("unknown Paddock '{name}'"))?;
        match definition {
            NamedPaddockConfig::Filesystem { path } => {
                let root = path.clone().unwrap_or(default_filesystem_root()?);
                Ok(Arc::new(FilesystemPaddock::new(root)))
            }
            NamedPaddockConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key_env,
                secret_key_env,
                session_token_env,
            } => Ok(Arc::new(S3Paddock::from_config(S3PaddockConfig {
                endpoint: endpoint.clone(),
                bucket: bucket.clone(),
                region: region.clone(),
                access_key_env: access_key_env.clone(),
                secret_key_env: secret_key_env.clone(),
                session_token_env: session_token_env.clone(),
            })?)),
        }
    }

    /// Opens the same configured backend for generic durable objects. Artifact
    /// and object handles are intentionally separate traits while sharing the
    /// same physical Paddock configuration.
    pub fn open_objects(
        &self,
        name: &str,
        explicit_filesystem_root: Option<PathBuf>,
    ) -> Result<PaddockObjectHandle> {
        if let Some(root) = explicit_filesystem_root {
            return Ok(Arc::new(FilesystemPaddock::new(root)));
        }
        let definition = self
            .paddocks
            .get(name)
            .with_context(|| format!("unknown Paddock '{name}'"))?;
        match definition {
            NamedPaddockConfig::Filesystem { path } => {
                let root = path.clone().unwrap_or(default_filesystem_root()?);
                Ok(Arc::new(FilesystemPaddock::new(root)))
            }
            NamedPaddockConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key_env,
                secret_key_env,
                session_token_env,
            } => Ok(Arc::new(S3Paddock::from_config(S3PaddockConfig {
                endpoint: endpoint.clone(),
                bucket: bucket.clone(),
                region: region.clone(),
                access_key_env: access_key_env.clone(),
                secret_key_env: secret_key_env.clone(),
                session_token_env: session_token_env.clone(),
            })?)),
        }
    }

    pub fn default_name(&self) -> &str {
        self.deployment
            .default_paddock
            .as_deref()
            .unwrap_or("local")
    }
}

pub fn default_filesystem_root() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("PIT_PADDOCK_ROOT") {
        return Ok(path.into());
    }
    if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("pit/paddock"));
    }
    if let Some(path) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(path).join(".local/share/pit/paddock"));
    }
    bail!("unable to determine Paddock root; pass --paddock-dir or configure a path")
}

fn user_config_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(path).join("pit/pit.toml"));
    }
    std::env::var_os("HOME").map(|path| PathBuf::from(path).join(".config/pit/pit.toml"))
}

fn read_toml(path: &Path) -> Result<toml::Value> {
    if !path.is_file() {
        return Ok(toml::Value::Table(Default::default()));
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read Paddock config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("malformed Paddock config {}", path.display()))
}

fn merge_toml(base: &mut toml::Value, overlay: toml::Value) {
    if let (toml::Value::Table(base), toml::Value::Table(overlay)) = (base, overlay) {
        for (key, value) in overlay {
            if let Some(existing) = base.get_mut(&key) {
                merge_toml(existing, value);
            } else {
                base.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_have_local_paddock() {
        let config = PaddockConfig::with_defaults();
        assert_eq!(config.default_name(), "local");
        assert!(config.paddocks.contains_key("local"));
    }

    #[test]
    fn project_config_loads_named_s3_without_credentials() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join("pit.toml"),
            "[paddocks.origin]\nprovider=\"s3\"\nendpoint=\"https://s3.test\"\nbucket=\"b\"\n",
        )
        .unwrap();
        let config = PaddockConfig::load(temp.path()).unwrap();
        assert!(matches!(
            config.paddocks.get("origin"),
            Some(NamedPaddockConfig::S3 { .. })
        ));
    }
}
