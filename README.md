# PitPaddock

Paddock is PitFast's durable content-addressed data and artifact substrate.
Paddock stores truth; PitFast executes behavior. Bytes are immutable and
logical names/refs may move.

The artifact plane remains the executable path: PitCrew builds canonical
`.wasm`, Paddock stores it under an `ArtifactDigest`, and PitBox prepares a
host-local disposable `.cwasm`. The object plane stores arbitrary durable
bytes without making them processes or runtime instances.

```text
PitCrew → artifact.json + .wasm → Paddock → local cache → PitBox
```

Immutable artifacts are identified by canonical SHA-256 digests:

```text
sha256:<64 lowercase hexadecimal characters>
```

Human-facing refs such as `service-a:v1` are mutable pointers to those
digests. Moving a ref never modifies or deletes a blob.

## Filesystem backend

The offline backend stores raw bytes and the canonical `pit-artifact`
`ArtifactManifest` without duplicating its schema:

```text
<root>/
├── blobs/sha256/ab/<digest>.wasm
├── manifests/sha256/<digest>.json
└── refs/<name>/<tag>.json
```

Blobs and refs are written through temporary files and atomic rename. Refs are
published only after the blob and manifest exist. Object publication uses a
kernel-managed advisory lock per namespace/key, so CAS semantics hold across
backend handles and independent processes sharing the same root. There is no
automatic garbage collection.

Generic objects use a separate namespace-aware layout and never replace the
artifact layout:

```text
<root>/
├── blobs/sha256/ab/<digest>.blob
└── objects/<namespace>/<key>/
    ├── current.json
    └── versions/<version>.json
```

Object writes stream to a temporary file, hash and size are finalized before
publication, then the immutable blob and versioned ref are published with
atomic renames. Range reads seek directly into the blob. A tombstone moves the
current ref while retaining historical versions and bytes. The filesystem
backend fsyncs file and containing-directory barriers around publication and
advertises durable writes, atomic ref replacement, and cross-process
conditional updates. Fault-injection tests cover crashes before and after the
authority rename.

## S3-compatible backend

`pit-paddock-s3` uses the standard S3 API with path-style addressing and
host-side credentials. It is deliberately vendor-neutral and has no AWS,
Cloudflare, R2, or MinIO-specific behavior. Configure the endpoint, bucket,
region, and credential environment variables in the caller; secrets are never
part of manifests, refs, fingerprints, or normal output. Real local MinIO
integration tests exercise blob/object PUT, GET, range, metadata, refs,
overwrite, tombstone, and restart behavior. Portable S3 conditional ref CAS
is intentionally not advertised because a provider-neutral two-object update
cannot be made atomic by a read-then-write sequence.

## CLI

The `pit` CLI uses the filesystem backend by default:

```bash
pit push service-a:v1
pit pull service-a:v1
pit pull sha256:...
pit paddock list
pit paddock inspect service-a:v1
```

`pit pull` verifies the manifest, size, and SHA-256 before placing the bytes in
the project-local `.pit/cache/` directory. Paddock is not on PitLane's request
path and cannot interrupt a service whose artifact is already local and
prepared.

`pit-paddock-s3` is an outbound backend adapter: Paddock uses an
S3-compatible service for storage. It is not the inbound S3 compatibility API.
Generic object blobs, version refs, range reads, and streamed uploads use
vendor-neutral S3 operations. The SDK/backend currently does not advertise a
portable conditional ref-update guarantee. Integration tests run only when an
explicit real S3-compatible test configuration is provided.

The separate [`gateway/`](gateway/) workload is the inbound compatibility
proof. It is a normal `wasi:http/proxy` component that imports only the generic
Paddock capability plus a host-owned authentication capability and is intended
to run through PitLane. Its alpha subset is object `PUT`, `GET`, `HEAD`,
`DELETE`, prefix listing, and single-range reads. Header-based AWS SigV4 is
supported with a 15-minute clock-skew window; presigned URLs, multipart upload,
bucket policies, and full S3 compatibility are intentionally not claimed. It
has no resident guest server; each request executes and then ends.

## Object contract

The object API is typed around `NamespaceId`, `ObjectKey`, `ObjectVersion`,
`BlobDigest`, `ObjectMetadata`, and `ObjectRef`. Namespaces are isolation
boundaries and reject traversal/ambiguous path components. `begin_object_write`
returns a streaming writer with `write_chunk`, `commit`, and `abort`; the
buffered `put_object` helper is only a convenience wrapper. Backends expose
range reads and a capability report so callers can distinguish guarantees
instead of assuming S3 and filesystem semantics are identical.

## Named Paddocks

The `pit-paddock-factory` crate owns the shared configuration model used by
the CLI and deployment tooling. A project or user configuration can define
named filesystem and S3-compatible Paddocks; credentials are resolved from
host environment variables when the backend is opened. The default name is
`local`. The factory returns the existing `PaddockBackend` abstraction for
artifacts and a separate `PaddockObjectBackend` handle for durable objects.
Deployment and rollback do not contain provider-specific branches.
