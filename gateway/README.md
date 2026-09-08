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

The current protocol is deliberately partial: the gateway validates request
`Content-Length`, returns XML success/error/list bodies, exposes deterministic
ETag and `Accept-Ranges` headers, supports one `bytes=start-end` range with
`206` and `Content-Range`, and copies multipart parts in 64 KiB chunks.
Presigned URLs, conditional requests, and bucket management remain deferred.
The current HTTP component path does not reliably permit an explicit
object-sized `Content-Length` on a HEAD/full-GET response, so AWS CLI
interoperability is validated while clients that require that metadata (such
as the tested mc version) remain partial.

AWS CLI 1.46.1 successfully exercises signed PUT, HEAD, GET, LIST, DELETE,
UTF-8/space-containing keys, and range GET against the PitLane-routed gateway.

For S3-shaped clients the fixed alpha bucket route is
`/s3-alpha/<object-key>`; the legacy `/<object-key>` route is retained for
the PitFast-native smoke harness. Header SigV4 accepts the AWS
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` request marker after PitLane has decoded
the body, but does not claim wire-level per-chunk signature verification.
