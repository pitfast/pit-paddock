# Paddock S3 gateway workload

This directory contains the first inbound S3-compatible gateway workload for
Paddock v1. It is a normal `wasi:http/proxy` component with an imported,
generic `pitfast:paddock/store` capability. It is not a Paddock backend and it
does not run as a resident server process.

The workload currently implements the small alpha subset needed to prove the
boundary: `PUT`, `GET`, `HEAD`, `DELETE`, `ListObjectsV2`-style listing, and
single-range reads. The namespace is explicitly granted by the host. S3
authentication, bucket policy, multipart upload, and the rest of the AWS S3
surface are intentionally outside this milestone.

Build locally with the pinned `componentize-js` tool:

```sh
componentize-js gateway.js --wit wit --world-name gateway --out gateway.wasm
```

`gateway.wasm` is a generated test artifact and is intentionally ignored.
