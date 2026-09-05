# PitPaddock

PitPaddock is PitFast's content-addressed artifact storage and distribution
layer. PitCrew builds artifacts, Paddock stores and verifies them, and PitBox
executes bytes that have already been acquired locally.

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
published only after the blob and manifest exist. There is no automatic
garbage collection.

## S3-compatible backend

`pit-paddock-s3` uses the standard S3 API with path-style addressing and
host-side credentials. It is deliberately vendor-neutral and has no AWS,
Cloudflare, R2, or MinIO-specific behavior. Configure the endpoint, bucket,
region, and credential environment variables in the caller; secrets are never
part of manifests, refs, fingerprints, or normal output.

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

S3 integration is supported by the library backend and is exercised only when
an explicit real S3-compatible test configuration is provided.
