# Paddock S3 gateway workload

This directory contains the first inbound S3-compatible gateway workload for
Paddock v1. It is a normal `wasi:http/proxy` component with an imported,
generic `pitfast:paddock/store` capability. It is not a Paddock backend and it
does not run as a resident server process.

The workload currently implements the small alpha subset needed to prove the
boundary: `PUT`, `GET`, `HEAD`, `DELETE`, `ListObjectsV2`-style listing, single-
range reads, and a bounded multipart flow (`CreateMultipartUpload`,
`UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`). Multipart
parts and session metadata are ordinary durable Paddock objects, so each
request remains disposable. The namespace is explicitly granted by the host.
When the host supplies a gateway credential grant, requests must use
header-based AWS Signature Version 4; the secret stays in the host request
context and is not embedded in this component. Bucket policy, presigned URLs,
and the rest of the AWS S3 surface are intentionally outside this milestone.

Build locally with the pinned `componentize-js` tool:

```sh
./build-gateway.sh
```

`build-gateway.sh` requires the pinned `componentize-js` and `wasm-tools`
commands, validates the resulting `wasi:http/proxy` component, and prints its
SHA-256. `package-gateway.sh` creates a consumer package containing only the
canonical `.wasm` and a small manifest; `.cwasm` is never distributed.

The current protocol is deliberately partial: the host HTTP layer supplies
`Content-Length` on responses, while the gateway validates request
`Content-Length`, supports one `bytes=start-end` range, and returns bounded XML
errors/results for multipart operations. The generic store ABI currently
returns bounded range chunks rather than exposing a guest read stream;
completion therefore copies each part in 64 KiB chunks. `Content-Range`, ETag
headers, presigned URLs, XML list responses, and bucket management remain
deferred until their ABI and interoperability tests are complete.
