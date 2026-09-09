use std::process::Command;

use anyhow::Result;
use pit_paddock_core::{
    CasConflict, NamespaceId, ObjectKey, ObjectMetadata, PaddockObjectBackend, put_object,
};
use pit_paddock_fs::FilesystemPaddock;

const WORKER_ROOT: &str = "PITFAST_PADDOCK_CAS_WORKER_ROOT";
const WORKER_BODY: &str = "PITFAST_PADDOCK_CAS_WORKER_BODY";
const WORKER_EXPECTED: &str = "PITFAST_PADDOCK_CAS_WORKER_EXPECTED";
#[cfg(feature = "fault-injection")]
const WORKER_FAULT: &str = "PITFAST_FS_FAULT_POINT";

#[tokio::test]
async fn cas_worker() {
    let Some(root) = std::env::var_os(WORKER_ROOT) else {
        return;
    };
    let expected = std::env::var(WORKER_EXPECTED)
        .ok()
        .map(|value| pit_paddock_core::ObjectVersion::new(value.parse().unwrap()));
    let store = FilesystemPaddock::new(root);
    let mut writer = store
        .begin_object_write(
            NamespaceId::new("test").unwrap(),
            ObjectKey::new("shared").unwrap(),
            ObjectMetadata::default(),
            expected,
        )
        .await
        .unwrap();
    writer
        .write_chunk(std::env::var(WORKER_BODY).unwrap().as_bytes())
        .await
        .unwrap();
    match writer.commit().await {
        Ok(_) => {}
        Err(error) if error.downcast_ref::<CasConflict>().is_some() => std::process::exit(2),
        Err(error) => panic!("unexpected worker failure: {error:#}"),
    }
}

#[tokio::test]
async fn cross_process_cas_has_one_winner_for_each_iteration() -> Result<()> {
    for iteration in 0..100 {
        let root = tempfile::tempdir()?;
        let store = FilesystemPaddock::new(root.path());
        let namespace = NamespaceId::new("test")?;
        let key = ObjectKey::new("shared")?;
        let initial = put_object(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            b"initial",
        )
        .await?;
        let exe = std::env::current_exe()?;
        let spawn = |body: &str| {
            Command::new(&exe)
                .args(["--exact", "cas_worker", "--nocapture"])
                .env(WORKER_ROOT, root.path())
                .env(WORKER_BODY, body)
                .env(WORKER_EXPECTED, initial.version.get().to_string())
                .spawn()
                .unwrap()
        };
        let mut first = spawn(&format!("first-{iteration}"));
        let mut second = spawn(&format!("second-{iteration}"));
        let first_result = first.wait();
        let second_result = second.wait();
        let first = first_result?;
        let second = second_result?;
        let statuses = [first.code(), second.code()];
        assert!(
            statuses.contains(&Some(0)),
            "iteration {iteration}: no CAS winner: {statuses:?}"
        );
        assert!(
            statuses.contains(&Some(2)),
            "iteration {iteration}: no CAS conflict: {statuses:?}"
        );
        let current = store.get_object(&namespace, &key, None).await?;
        assert_eq!(current.version.get(), 2);
        let bytes = store.get_blob_bytes(&current.digest).await?;
        assert!(
            bytes == format!("first-{iteration}").as_bytes()
                || bytes == format!("second-{iteration}").as_bytes()
        );
    }
    Ok(())
}

#[tokio::test]
async fn cross_process_unconditional_writes_never_corrupt_current_ref() -> Result<()> {
    for iteration in 0..25 {
        let root = tempfile::tempdir()?;
        let store = FilesystemPaddock::new(root.path());
        let namespace = NamespaceId::new("test")?;
        let key = ObjectKey::new("shared")?;
        put_object(
            &store,
            namespace.clone(),
            key.clone(),
            ObjectMetadata::default(),
            b"initial",
        )
        .await?;
        let exe = std::env::current_exe()?;
        let spawn = |body: &str| {
            Command::new(&exe)
                .args(["--exact", "cas_worker", "--nocapture"])
                .env(WORKER_ROOT, root.path())
                .env(WORKER_BODY, body)
                .env_remove(WORKER_EXPECTED)
                .spawn()
                .unwrap()
        };
        let mut first = spawn(&format!("first-{iteration}"));
        let mut second = spawn(&format!("second-{iteration}"));
        let first_result = first.wait();
        let second_result = second.wait();
        assert_eq!(first_result?.code(), Some(0));
        assert_eq!(second_result?.code(), Some(0));
        let current = store.get_object(&namespace, &key, None).await?;
        assert_eq!(current.version.get(), 3);
        let bytes = store.get_blob_bytes(&current.digest).await?;
        assert!(
            bytes == format!("first-{iteration}").as_bytes()
                || bytes == format!("second-{iteration}").as_bytes()
        );
    }
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn abrupt_publication_exit_leaves_coherent_authority() -> Result<()> {
    for iteration in 0..100 {
        for fault in [
            "after_temp_write",
            "after_blob_rename",
            "after_version_rename",
            "after_current_ref_rename",
            "before_directory_fsync",
            "after_directory_fsync",
        ] {
            let root = tempfile::tempdir()?;
            let store = FilesystemPaddock::new(root.path());
            let namespace = NamespaceId::new("test")?;
            let key = ObjectKey::new("shared")?;
            let initial = put_object(
                &store,
                namespace.clone(),
                key.clone(),
                ObjectMetadata::default(),
                b"initial",
            )
            .await?;
            let exe = std::env::current_exe()?;
            let mut child = Command::new(&exe)
                .args(["--exact", "cas_worker", "--nocapture"])
                .env(WORKER_ROOT, root.path())
                .env(WORKER_BODY, "after-crash")
                .env(WORKER_EXPECTED, initial.version.get().to_string())
                .env(WORKER_FAULT, fault)
                .spawn()?;
            let status = child.wait()?;
            assert_eq!(
                status.code(),
                Some(137),
                "iteration {iteration}, fault {fault} did not abort: {status}"
            );
            let current = store.get_object(&namespace, &key, None).await?;
            assert!(current.version.get() == 1 || current.version.get() == 2);
            let bytes = store.get_blob_bytes(&current.digest).await?;
            assert!(bytes == b"initial" || bytes == b"after-crash");
            assert_eq!(
                store
                    .get_object(&namespace, &key, Some(initial.version))
                    .await?
                    .version,
                initial.version
            );
        }
    }
    Ok(())
}
