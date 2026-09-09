use std::process::{Command, Stdio};

use pit_paddock_core::{
    CasConflict, NamespaceId, ObjectKey, ObjectMetadata, ObjectVersion, PaddockObjectBackend,
    RefCondition, put_object, put_object_if,
};
use pit_paddock_s3::{S3Paddock, S3PaddockConfig};

fn configured() -> bool {
    let required = [
        "PITFAST_TEST_S3_ENDPOINT",
        "PITFAST_TEST_S3_BUCKET",
        "PITFAST_TEST_S3_ACCESS_KEY",
        "PITFAST_TEST_S3_SECRET_KEY",
    ];
    required.iter().all(|name| std::env::var_os(name).is_some())
        && std::env::var("PITFAST_TEST_S3_CONDITIONAL_REFS")
            .ok()
            .as_deref()
            == Some("1")
}

fn open_store() -> S3Paddock {
    S3Paddock::from_config(S3PaddockConfig {
        endpoint: std::env::var("PITFAST_TEST_S3_ENDPOINT").unwrap(),
        bucket: std::env::var("PITFAST_TEST_S3_BUCKET").unwrap(),
        region: std::env::var("PITFAST_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_env: "PITFAST_TEST_S3_ACCESS_KEY".into(),
        secret_key_env: "PITFAST_TEST_S3_SECRET_KEY".into(),
        session_token_env: None,
        conditional_ref_update: true,
    })
    .unwrap()
}

#[tokio::test]
async fn s3_conditional_authority_has_one_winner_under_process_contention() {
    if !configured() {
        eprintln!("SKIP S3 authority conformance: MinIO configuration is not present");
        return;
    }
    let store = open_store();
    assert!(store.capabilities().conditional_ref_update);
    for iteration in 0..100_u32 {
        let namespace =
            NamespaceId::new(format!("authority-{}-{iteration}", std::process::id())).unwrap();
        let key = ObjectKey::new("counter").unwrap();
        let initial = put_object(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            b"initial",
        )
        .await
        .unwrap();
        let mut a = Command::new(std::env::current_exe().unwrap());
        let mut b = Command::new(std::env::current_exe().unwrap());
        for command in [&mut a, &mut b] {
            command
                .arg("--exact")
                .arg("s3_conditional_worker")
                .arg("--nocapture")
                .env("PITFAST_S3_WORKER_MODE", "update")
                .env("PITFAST_S3_WORKER_NAMESPACE", namespace.as_str())
                .env("PITFAST_S3_WORKER_KEY", key.as_str())
                .env(
                    "PITFAST_S3_WORKER_VERSION",
                    initial.version.get().to_string(),
                )
                .stdout(Stdio::null())
                .stderr(Stdio::null());
        }
        let left = a.spawn().unwrap();
        let right = b.spawn().unwrap();
        let left = left.wait_with_output().unwrap();
        let right = right.wait_with_output().unwrap();
        assert_eq!(
            [left.status.success(), right.status.success()]
                .into_iter()
                .filter(|success| *success)
                .count(),
            1,
            "iteration {iteration} did not have exactly one winner"
        );
        assert_eq!(
            [left.status.success(), right.status.success()]
                .into_iter()
                .filter(|success| !*success)
                .count(),
            1,
            "iteration {iteration} did not have exactly one conflict"
        );
        let current = store.get_object(&namespace, &key, None).await.unwrap();
        assert_eq!(current.version, ObjectVersion::new(2));
    }
}

#[tokio::test]
async fn s3_conditional_create_and_tombstone_are_atomic() {
    if !configured() {
        eprintln!("SKIP S3 authority conformance: MinIO configuration is not present");
        return;
    }
    let store = open_store();
    let namespace = NamespaceId::new(format!("authority-create-{}", std::process::id())).unwrap();
    let key = ObjectKey::new("create").unwrap();
    let mut children = Vec::new();
    for value in [b"a".as_slice(), b"b".as_slice()] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("s3_conditional_worker")
            .arg("--nocapture")
            .env("PITFAST_S3_WORKER_MODE", "create")
            .env("PITFAST_S3_WORKER_NAMESPACE", namespace.as_str())
            .env("PITFAST_S3_WORKER_KEY", key.as_str())
            .env(
                "PITFAST_S3_WORKER_VALUE",
                String::from_utf8(value.to_vec()).unwrap(),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        children.push(command.spawn().unwrap());
    }
    let statuses = children
        .into_iter()
        .map(|child| child.wait_with_output().unwrap().status.success())
        .collect::<Vec<_>>();
    assert_eq!(statuses.iter().filter(|status| **status).count(), 1);
    assert_eq!(statuses.iter().filter(|status| !**status).count(), 1);
    let initial = store.get_object(&namespace, &key, None).await.unwrap();

    let mut update = Command::new(std::env::current_exe().unwrap());
    let mut delete = Command::new(std::env::current_exe().unwrap());
    for command in [&mut update, &mut delete] {
        command
            .arg("--exact")
            .arg("s3_conditional_worker")
            .arg("--nocapture")
            .env("PITFAST_S3_WORKER_NAMESPACE", namespace.as_str())
            .env("PITFAST_S3_WORKER_KEY", key.as_str())
            .env(
                "PITFAST_S3_WORKER_VERSION",
                initial.version.get().to_string(),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null());
    }
    update.env("PITFAST_S3_WORKER_MODE", "update");
    delete.env("PITFAST_S3_WORKER_MODE", "delete");
    let a = update.spawn().unwrap();
    let b = delete.spawn().unwrap();
    let a = a.wait_with_output().unwrap();
    let b = b.wait_with_output().unwrap();
    assert_eq!(
        [a.status.success(), b.status.success()]
            .into_iter()
            .filter(|v| *v)
            .count(),
        1
    );
    assert_eq!(
        [a.status.success(), b.status.success()]
            .into_iter()
            .filter(|v| !*v)
            .count(),
        1
    );
}

#[tokio::test]
async fn s3_conditional_worker() {
    if std::env::var_os("PITFAST_S3_WORKER_MODE").is_none() {
        return;
    }
    let store = open_store();
    let namespace =
        NamespaceId::new(std::env::var("PITFAST_S3_WORKER_NAMESPACE").unwrap()).unwrap();
    let key = ObjectKey::new(std::env::var("PITFAST_S3_WORKER_KEY").unwrap()).unwrap();
    let mode = std::env::var("PITFAST_S3_WORKER_MODE").unwrap();
    let result = match mode.as_str() {
        "create" => put_object_if(
            &store,
            namespace,
            key,
            ObjectMetadata::default(),
            RefCondition::Absent,
            std::env::var("PITFAST_S3_WORKER_VALUE").unwrap().as_bytes(),
        )
        .await
        .map(|_| ()),
        "update" => {
            let version = ObjectVersion::new(
                std::env::var("PITFAST_S3_WORKER_VERSION")
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
            put_object_if(
                &store,
                namespace,
                key,
                ObjectMetadata::default(),
                RefCondition::Version(version),
                b"updated",
            )
            .await
            .map(|_| ())
        }
        "delete" => {
            let version = ObjectVersion::new(
                std::env::var("PITFAST_S3_WORKER_VERSION")
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
            store
                .delete_object_if(&namespace, &key, RefCondition::Version(version))
                .await
        }
        other => panic!("unknown worker mode {other}"),
    };
    if let Err(error) = result {
        if error.downcast_ref::<CasConflict>().is_some() {
            std::process::exit(2);
        }
        panic!("worker error: {error:#}");
    }
}
