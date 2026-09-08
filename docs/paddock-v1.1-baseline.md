# Paddock v1.1 baseline

This note records the pre-hardening behavior observed before the v1.1
changes. It is an engineering baseline, not a durability certification.

## Local filesystem backend

- Blob and object writes already used temporary files and atomic rename.
- Object refs were versioned, but conditional publication was serialized only
  by a backend-handle mutex; separate backend objects/processes had no shared
  CAS lock.
- Ref publication validated the in-process view of the current version.
- File sync existed on important writes, but directory-level sync was not
  consistently applied after renames.
- The object layout was namespace/key aware and range reads sought directly in
  immutable blob files.

## S3-compatible backend

- `pit-paddock-s3` was an outbound S3-compatible storage adapter, using
  path-style addressing and host-provided credentials.
- Artifact blobs, manifests, refs, and generic object blobs used separate key
  layouts.
- Generic object writes streamed through a local temporary file before upload.
- The backend did not advertise portable conditional-ref CAS: a two-object
  version record/current-ref publication cannot be made atomic by a generic
  read-then-write sequence.
- Provider restart and real external-provider interoperability had not been
  validated in this checkout.

## Gateway

- The gateway was a disposable `wasi:http/proxy` workload using the generic
  Paddock store capability.
- It supported a deliberately small object HTTP subset and had no permanent
  guest process.
- Authentication was not yet a host-owned, tested SigV4 contract.
- Multipart upload, presigned URLs, complete S3 XML semantics, and a
  distributable versioned gateway package were not implemented.

## v1.1 measurement boundary

The hardening tests use independent processes for filesystem contention and
fault-injection exits. Local S3 tests use an explicitly configured ephemeral
MinIO server. No cloud-provider credentials are assumed or included.
