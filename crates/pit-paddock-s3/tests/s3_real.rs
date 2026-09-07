//! Conditional real S3-compatible integration test.

use pit_artifact::{
    ArtifactFormat, ArtifactManifest, ArtifactSpec, BuildProfile, BuildSpec, Entrypoint,
    ExecutionDefaults, RuntimeAbi, RuntimeSpec, SCHEMA_VERSION,
};
use pit_paddock_core::{ArtifactDigest, PaddockRef, pull, push};
use pit_paddock_s3::{S3Paddock, S3PaddockConfig};

#[tokio::test]
async fn real_s3_roundtrip_when_configured() {
    let required = [
        "PITFAST_TEST_S3_ENDPOINT",
        "PITFAST_TEST_S3_BUCKET",
        "PITFAST_TEST_S3_ACCESS_KEY",
        "PITFAST_TEST_S3_SECRET_KEY",
    ];
    if required.iter().any(|name| std::env::var_os(name).is_none()) {
        eprintln!("SKIP real S3-compatible E2E: test configuration is not present");
        return;
    }
    let store = S3Paddock::from_config(S3PaddockConfig {
        endpoint: std::env::var("PITFAST_TEST_S3_ENDPOINT").unwrap(),
        bucket: std::env::var("PITFAST_TEST_S3_BUCKET").unwrap(),
        region: std::env::var("PITFAST_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_env: "PITFAST_TEST_S3_ACCESS_KEY".into(),
        secret_key_env: "PITFAST_TEST_S3_SECRET_KEY".into(),
        session_token_env: None,
    })
    .unwrap();
    let bytes = b"\0asm\x01\0\0\0";
    let digest = ArtifactDigest::from_wasm(bytes);
    let manifest = ArtifactManifest {
        schema_version: SCHEMA_VERSION,
        artifact: ArtifactSpec {
            name: "s3-integration".into(),
            path: "build/s3-integration.wasm".into(),
            sha256: digest.hex().into(),
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
    };
    let reference: PaddockRef = format!("pitfast-test-{}:e2e", std::process::id())
        .parse()
        .unwrap();
    push(&store, &reference, manifest, bytes).await.unwrap();
    let stored = pull(&store, Some(&reference), None).await.unwrap();
    assert_eq!(stored.digest, digest);
}
