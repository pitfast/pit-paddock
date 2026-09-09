# Paddock v1.2 authority baseline

Before v1.2, the generic object writer accepted `Option<ObjectVersion>`.
`None` meant an unconditional write, so the API could not express
create-if-absent. Filesystem Paddock serialized object publication with a
kernel-managed `flock` and already provided cross-process CAS. The S3 backend
reported `conditional_ref_update = false` and performed a read/check followed
by an unconditional current-ref PUT.

That S3 sequence had a race:

```text
writer A: GET current version N
writer B: GET current version N
writer A: PUT current version N+1
writer B: PUT current version N+1  # stale writer could overwrite A
```

The v1.2 contract separates three cases with `RefCondition`:

- `Unconditional`: ordinary last-committed-writer-wins publication;
- `Absent`: provider-enforced create-if-absent;
- `Version(N)`: provider-enforced compare-and-publish.

For S3-compatible backends, `Absent` uses `If-None-Match: *`. `Version(N)`
reads the current ref and its provider ETag, verifies the semantic version,
then publishes with `If-Match: <ETag>`. A provider precondition failure is
translated to the typed `CasConflict`; it is never retried as a blind PUT.

Because S3-compatible implementations do not share one guaranteed contract,
the S3 profile defaults `conditional_ref_update` to false. It is enabled only
by explicit configuration (`conditional_ref_update = true`) and the MinIO
integration profile. An unprofiled provider remains usable for unconditional
object writes but is not authority-capable.

Version metadata is itself published with `If-None-Match: *` for conditional
writes. A conflicting version record is treated as a CAS conflict. The
current-ref conditional PUT is the authority decision; immutable blobs and
version records may remain as safe, unreachable garbage after a losing race.

The provider ETag is an internal concurrency token. It is not exposed as a
Paddock `ObjectVersion`, a `BlobDigest`, or an S3 response ETag.

## Provider configuration

```toml
[paddocks.minio]
provider = "s3"
endpoint = "http://127.0.0.1:9000"
bucket = "pitfast-test"
conditional_ref_update = true
```

Set this only after validating the provider's conditional PUT semantics in an
isolated bucket/prefix. Future engine conformance must reject a backend whose
capabilities report `conditional_ref_update = false`.

The v1.2 test harness runs independent child processes against one MinIO
bucket for conditional create, update, and tombstone contention. A real
remote cloud provider is tested only when the explicit `PITFAST_TEST_S3_*`
configuration is present.
