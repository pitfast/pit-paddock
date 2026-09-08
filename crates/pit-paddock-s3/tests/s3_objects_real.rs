use pit_paddock_core::{
    BlobDigest, NamespaceId, ObjectKey, ObjectMetadata, ObjectVersion, PaddockObjectBackend,
    put_object,
};
use pit_paddock_s3::{S3Paddock, S3PaddockConfig};

fn open_store() -> S3Paddock {
    S3Paddock::from_config(S3PaddockConfig {
        endpoint: std::env::var("PITFAST_TEST_S3_ENDPOINT").unwrap(),
        bucket: std::env::var("PITFAST_TEST_S3_BUCKET").unwrap(),
        region: std::env::var("PITFAST_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_env: "PITFAST_TEST_S3_ACCESS_KEY".into(),
        secret_key_env: "PITFAST_TEST_S3_SECRET_KEY".into(),
        session_token_env: None,
    })
    .unwrap()
}

fn configured() -> bool {
    [
        "PITFAST_TEST_S3_ENDPOINT",
        "PITFAST_TEST_S3_BUCKET",
        "PITFAST_TEST_S3_ACCESS_KEY",
        "PITFAST_TEST_S3_SECRET_KEY",
    ]
    .iter()
    .all(|name| std::env::var_os(name).is_some())
}

#[tokio::test]
async fn real_s3_object_roundtrip_and_restart() {
    if !configured() {
        eprintln!("SKIP real S3 object E2E: test configuration is not present");
        return;
    }
    let store = open_store();
    let namespace = NamespaceId::new(format!("real-s3-{}", std::process::id())).unwrap();
    let key = ObjectKey::new("nested/日本語 key.bin").unwrap();
    let first = put_object(
        &store,
        namespace.clone(),
        key.clone(),
        ObjectMetadata {
            content_type: Some("application/octet-stream".into()),
            custom: [("purpose".into(), "integration".into())]
                .into_iter()
                .collect(),
        },
        b"0123456789",
    )
    .await
    .unwrap();
    assert_eq!(first.version, ObjectVersion::new(1));
    assert_eq!(
        store.get_blob_bytes(&first.digest).await.unwrap(),
        b"0123456789"
    );
    assert_eq!(
        store.read_blob_range(&first.digest, 3, 4).await.unwrap(),
        b"3456"
    );
    assert_eq!(
        store.get_object(&namespace, &key, None).await.unwrap(),
        first
    );

    let second = put_object(
        &store,
        namespace.clone(),
        key.clone(),
        ObjectMetadata::default(),
        b"replacement",
    )
    .await
    .unwrap();
    assert_eq!(second.version, ObjectVersion::new(2));
    assert_ne!(first.digest, second.digest);
    assert_eq!(
        store
            .list_objects(&namespace, Some("nested/"))
            .await
            .unwrap()
            .len(),
        1
    );

    // A fresh backend handle must observe the same durable remote state.
    let restarted = open_store();
    assert_eq!(
        restarted.get_object(&namespace, &key, None).await.unwrap(),
        second
    );
    restarted
        .delete_object(&namespace, &key, Some(second.version))
        .await
        .unwrap();
    assert!(restarted.get_object(&namespace, &key, None).await.is_err());
    assert!(
        !restarted
            .get_object(&namespace, &key, Some(first.version))
            .await
            .unwrap()
            .deleted
    );
    assert!(
        restarted
            .list_objects(&namespace, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(BlobDigest::from_bytes(b"0123456789"), first.digest);
}

#[tokio::test]
async fn backend_does_not_turn_network_failure_into_an_upload() {
    if !configured() {
        eprintln!("SKIP network failure E2E: test credentials are not present");
        return;
    }
    let config = S3PaddockConfig {
        endpoint: "http://127.0.0.1:1".into(),
        bucket: "unreachable".into(),
        region: "us-east-1".into(),
        access_key_env: "PITFAST_TEST_S3_ACCESS_KEY".into(),
        secret_key_env: "PITFAST_TEST_S3_SECRET_KEY".into(),
        session_token_env: None,
    };
    let store = S3Paddock::from_config(config).unwrap();
    let error = store
        .put_blob_bytes(b"network failure must propagate")
        .await;
    assert!(
        error.is_err(),
        "connection refusal must not be treated as a cache miss"
    );
}
