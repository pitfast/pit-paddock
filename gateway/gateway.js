import {
  Fields,
  IncomingRequest,
  IncomingBody,
  OutgoingBody,
  OutgoingResponse,
  ResponseOutparam,
} from 'wasi:http/types@0.2.0';
import * as store from 'pitfast:paddock/store@0.1.0';
import * as auth from 'pitfast:paddock/auth@0.1.0';

const namespace = 's3-alpha';
const MAX_ERROR_BYTES = 4096;
const MAX_MULTIPART_XML_BYTES = 64 * 1024;
const MAX_MULTIPART_PARTS = 10_000;
const MAX_MULTIPART_PART_BYTES = 128 * 1024 * 1024;
const MULTIPART_PREFIX = '__paddock_multipart/';
const S3_LAST_MODIFIED = 'Wed, 01 Jan 2020 00:00:00 GMT';
let uploadSequence = 0;

// Legacy signing helpers remain below as non-executed reference code. The
// active verifier is host-owned so credentials never enter the guest module.
const SHA256_K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b,
  0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01,
  0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7,
  0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
  0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152,
  0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
  0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
  0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08,
  0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f,
  0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

function rotr(value, bits) {
  return (value >>> bits) | (value << (32 - bits));
}

function sha256(input) {
  const bytes = input instanceof Uint8Array ? input : text(input);
  const bitLength = bytes.length * 8;
  const paddedLength = ((bytes.length + 9 + 63) >> 6) << 6;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x100000000));
  view.setUint32(paddedLength - 4, bitLength >>> 0);
  let h0 = 0x6a09e667; let h1 = 0xbb67ae85; let h2 = 0x3c6ef372;
  let h3 = 0xa54ff53a; let h4 = 0x510e527f; let h5 = 0x9b05688c;
  let h6 = 0x1f83d9ab; let h7 = 0x5be0cd19;
  const w = new Uint32Array(64);
  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getUint32(offset + i * 4);
    for (let i = 16; i < 64; i++) {
      const s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3);
      const s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
    }
    let a = h0; let b = h1; let c = h2; let d = h3;
    let e = h4; let f = h5; let g = h6; let h = h7;
    for (let i = 0; i < 64; i++) {
      const s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      const ch = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + ch + SHA256_K[i] + w[i]) >>> 0;
      const s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + maj) >>> 0;
      h = g; g = f; f = e; e = (d + temp1) >>> 0;
      d = c; c = b; b = a; a = (temp1 + temp2) >>> 0;
    }
    h0 = (h0 + a) >>> 0; h1 = (h1 + b) >>> 0; h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0; h4 = (h4 + e) >>> 0; h5 = (h5 + f) >>> 0;
    h6 = (h6 + g) >>> 0; h7 = (h7 + h) >>> 0;
  }
  const output = new Uint8Array(32);
  const result = [h0, h1, h2, h3, h4, h5, h6, h7];
  result.forEach((value, index) => viewFor(output).setUint32(index * 4, value));
  return output;
}

function viewFor(bytes) { return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength); }

function hmacSha256(key, message) {
  let actual = key instanceof Uint8Array ? key : text(key);
  if (actual.length > 64) actual = sha256(actual);
  const padded = new Uint8Array(64); padded.set(actual);
  const inner = new Uint8Array(64); const outer = new Uint8Array(64);
  for (let i = 0; i < 64; i++) { inner[i] = padded[i] ^ 0x36; outer[i] = padded[i] ^ 0x5c; }
  const innerInput = new Uint8Array(inner.length + message.length);
  innerInput.set(inner); innerInput.set(message, inner.length);
  const outerInput = new Uint8Array(outer.length + 32);
  outerInput.set(outer); outerInput.set(sha256(innerInput), outer.length);
  return sha256(outerInput);
}

function hex(bytes) {
  return Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
}

function constantTimeEqual(left, right) {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let i = 0; i < left.length; i++) difference |= left.charCodeAt(i) ^ right.charCodeAt(i);
  return difference === 0;
}

function awsEncode(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, character => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
}

function canonicalQuery(path) {
  const query = path.indexOf('?');
  if (query === -1) return '';
  const pairs = path.slice(query + 1).split('&').filter(Boolean).map(part => {
    const equal = part.indexOf('=');
    const key = equal === -1 ? part : part.slice(0, equal);
    const value = equal === -1 ? '' : part.slice(equal + 1);
    return [awsEncode(decodeURIComponent(key)), awsEncode(decodeURIComponent(value))];
  });
  pairs.sort((a, b) => a[0] === b[0] ? a[1].localeCompare(b[1]) : a[0].localeCompare(b[0]));
  return pairs.map(pair => `${pair[0]}=${pair[1]}`).join('&');
}

function canonicalUri(path) {
  const raw = path.split('?', 1)[0] || '/';
  return raw.split('/').map(segment => awsEncode(decodeURIComponent(segment))).join('/') || '/';
}

function headerEntries(headers) {
  return headers.entries().map(([name, value]) => [name.toLowerCase(), new TextDecoder().decode(Uint8Array.from(value)).trim().replace(/\s+/g, ' ')]);
}

function authFailure(message) { return { status: 403, code: 'AccessDenied', message }; }

function validateAuth(request, payloadHash) {
  // Avoid touching the optional WASI environment capability for the normal
  // unauthenticated alpha path. This also keeps the no-auth gateway hot path
  // identical to earlier releases.
  const authorization = firstHeaderValue(request.headers(), 'authorization');
  if (authorization === null) return null;
  const env = new Map();
  const accessKey = env.get('PITFAST_S3_GATEWAY_ACCESS_KEY');
  const secretKey = env.get('PITFAST_S3_GATEWAY_SECRET_KEY');
  if (accessKey === undefined && secretKey === undefined) return null;
  if (accessKey === undefined || secretKey === undefined) return authFailure('gateway authentication is misconfigured');
  const date = firstHeaderValue(request.headers(), 'x-amz-date');
  if (authorization === null || date === null) return authFailure('SigV4 Authorization and x-amz-date are required');
  const match = /^AWS4-HMAC-SHA256 Credential=([^/]+)\/(\d{8})\/([^/]+)\/([^/]+)\/aws4_request, SignedHeaders=([^,]+), Signature=([0-9a-f]{64})$/.exec(authorization);
  if (match === null || match[1] !== accessKey) return authFailure('invalid SigV4 credential');
  if (!/^\d{8}T\d{6}Z$/.test(date)) return authFailure('invalid x-amz-date');
  const year = Number(date.slice(0, 4)); const month = Number(date.slice(4, 6)) - 1;
  const day = Number(date.slice(6, 8)); const hour = Number(date.slice(9, 11));
  const minute = Number(date.slice(11, 13)); const second = Number(date.slice(13, 15));
  const signedAt = Date.UTC(year, month, day, hour, minute, second) / 1000;
  const now = Math.floor(Date.now() / 1000);
  if (!Number.isFinite(signedAt) || Math.abs(now - signedAt) > 900) return authFailure('request timestamp is outside the 15 minute SigV4 window');
  const signedHeaders = match[5].split(';');
  if (!signedHeaders.includes('host') || !signedHeaders.includes('x-amz-date')) return authFailure('host and x-amz-date must be signed');
  const entries = new Map(headerEntries(request.headers()));
  const canonicalHeaders = signedHeaders.map(name => {
    if (!entries.has(name)) throw authFailure(`signed header is missing: ${name}`);
    return `${name}:${entries.get(name)}\n`;
  }).join('');
  const canonical = `${methodName(request)}\n${canonicalUri(request.pathWithQuery())}\n${canonicalQuery(request.pathWithQuery())}\n${canonicalHeaders}\n${signedHeaders.join(';')}\n${payloadHash}`;
  const hashedCanonical = hex(sha256(text(canonical)));
  const region = env.get('PITFAST_S3_GATEWAY_REGION') ?? 'us-east-1';
  const scope = `${match[2]}/${match[3]}/${match[4]}/aws4_request`;
  const stringToSign = `AWS4-HMAC-SHA256\n${date}\n${scope}\n${hashedCanonical}`;
  const kDate = hmacSha256(text(`AWS4${secretKey}`), text(match[2]));
  const kRegion = hmacSha256(kDate, text(region));
  const kService = hmacSha256(kRegion, text(match[4]));
  const signingKey = hmacSha256(kService, text('aws4_request'));
  const expected = hex(hmacSha256(signingKey, text(stringToSign)));
  if (!constantTimeEqual(expected, match[6])) return authFailure('signature does not match');
  return null;
}

function response(status, body = new Uint8Array(), contentType = 'text/plain', extraHeaders = []) {
  const headers = new Fields();
  headers.set('content-type', [text(contentType)]);
  headers.set('cache-control', [text('no-store')]);
  for (const [name, value] of extraHeaders) headers.set(name, [text(value)]);
  const outgoing = new OutgoingResponse(headers);
  outgoing.setStatusCode(status);
  const output = outgoing.body();
  const stream = output.write();
  let offset = 0;
  while (offset < body.length) {
    const ready = stream.subscribe();
    ready.block();
    ready[Symbol.dispose]();
    const permit = Number(stream.checkWrite());
    if (permit === 0) continue;
    const end = Math.min(body.length, offset + permit);
    stream.write(body.slice(offset, end));
    offset = end;
  }
  stream.flush();
  const flushed = stream.subscribe();
  flushed.block();
  flushed[Symbol.dispose]();
  stream[Symbol.dispose]();
  OutgoingBody.finish(output, undefined);
  return outgoing;
}

function text(value) {
  return new TextEncoder().encode(value);
}

function streamBody(request, writer, maxBytes = null) {
  const body = request.consume();
  const input = body.stream();
  let size = 0n;
  try {
    while (true) {
      const ready = input.subscribe();
      ready.block();
      ready[Symbol.dispose]();
      let chunk;
      try {
        chunk = input.read(65536n);
      } catch (error) {
        if (error?.payload?.tag === 'closed' || error?.tag === 'closed') break;
        throw error;
      }
      if (chunk.length === 0) continue;
      if (maxBytes !== null && size + BigInt(chunk.length) > BigInt(maxBytes)) {
        throw new Error('request body exceeds the configured object size limit');
      }
      writer.write(chunk);
      size += BigInt(chunk.length);
    }
  } finally {
    input[Symbol.dispose]();
    const trailers = IncomingBody.finish(body);
    const trailersReady = trailers.subscribe();
    trailersReady.block();
    trailersReady[Symbol.dispose]();
    trailers.get();
  }
  return size;
}

function targetFromRequest(request) {
  const path = request.pathWithQuery().split('?', 1)[0];
  if (!path.startsWith('/')) return { bucket: null, key: null };
  const relative = path.slice(1);
  if (relative === '') return { bucket: null, key: null };
  const bucketPrefix = `${namespace}/`;
  if (relative === namespace) return { bucket: namespace, key: null };
  if (relative.startsWith(bucketPrefix)) {
    const key = decodeObjectKey(relative.slice(bucketPrefix.length));
    return { bucket: namespace, key: key.length > 0 ? key : null };
  }
  // Retain the original alpha route shape (/object-key) for callers that do
  // not model S3 buckets. S3 clients can use /s3-alpha/object-key.
  return { bucket: null, key: decodeObjectKey(relative) };
}

function decodeObjectKey(value) {
  return value.split('/').map(segment => decodeURIComponent(segment)).join('/');
}

function queryFromRequest(request) {
  const raw = request.pathWithQuery();
  const query = raw.indexOf('?');
  return query === -1 ? new URLSearchParams() : new URLSearchParams(raw.slice(query + 1));
}

function errorText(error) {
  if (error === null || error === undefined) return 'unknown error';
  if (error.payload !== undefined) return JSON.stringify(error.payload);
  if (typeof error === 'object' && error.tag !== undefined) {
    return `${error.tag}: ${error.val ?? ''}`;
  }
  return String(error);
}

function objectJson(object) {
  return {
    namespace: object.namespace,
    key: object.key,
    version: String(object.version),
    digest: String(object.digest),
    size: String(object.size),
    contentType: object.contentType ?? null,
    deleted: object.deleted ?? false,
  };
}

function methodName(request) {
  const method = request.method();
  return (method.tag === 'other' ? method.val : method.tag).toUpperCase();
}

function firstHeaderValue(headers, name) {
  const value = headers.get(name);
  if (!Array.isArray(value) || value.length === 0) return null;
  const first = value[0];
  if (typeof first === 'string') return first;
  return new TextDecoder().decode(Uint8Array.from(first));
}

function xmlEscape(value) {
  return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

function s3Error(status, code, message) {
  const body = `<Error><Code>${xmlEscape(code)}</Code><Message>${xmlEscape(message)}</Message></Error>`;
  return response(status, text(body.slice(0, MAX_ERROR_BYTES)), 'application/xml');
}

function objectEtag(object) {
  return `"${String(object.digest).replace(/^sha256:/, '')}"`;
}

function listXml(objects) {
  const visible = objects.filter(object => !object.key.startsWith(MULTIPART_PREFIX));
  const entries = visible
    .map(object => `<Contents><Key>${xmlEscape(object.key)}</Key><LastModified>2020-01-01T00:00:00.000Z</LastModified><ETag>${xmlEscape(objectEtag(object))}</ETag><Size>${xmlEscape(String(object.size))}</Size><StorageClass>STANDARD</StorageClass></Contents>`)
    .join('');
  return `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>${xmlEscape(namespace)}</Name><KeyCount>${visible.length}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>${entries}</ListBucketResult>`;
}

function multipartId() {
  // The identifier is only an opaque durable namespace key. It is never used
  // as an authority token. Include time, a per-instance sequence, and random
  // entropy so independently spawned disposable gateway executions do not
  // normally collide while the upload state itself remains in Paddock.
  const entropy = Math.floor(Math.random() * 0x100000000).toString(16).padStart(8, '0');
  const sequence = (uploadSequence++).toString(36);
  return `${Date.now().toString(36)}-${sequence}-${entropy}`;
}

function multipartPartKey(uploadId, partNumber) {
  return `${MULTIPART_PREFIX}${uploadId}/part/${partNumber}`;
}

function multipartMetaKey(uploadId) {
  return `${MULTIPART_PREFIX}${uploadId}/meta`;
}

function putSmallObject(key, bytes, contentType) {
  const writer = store.beginWrite(namespace, key, contentType);
  writer.write(bytes);
  return writer.commit();
}

function readSmallObject(key) {
  let object;
  try {
    object = store.get(namespace, key);
  } catch (_) {
    return null;
  }
  if (object === null || BigInt(object.size) > BigInt(MAX_MULTIPART_XML_BYTES)) return null;
  return store.readRange(namespace, key, 0n, BigInt(object.size));
}

function multipartMeta(uploadId) {
  const bytes = readSmallObject(multipartMetaKey(uploadId));
  if (bytes === null) return null;
  try {
    const meta = JSON.parse(new TextDecoder().decode(Uint8Array.from(bytes)));
    if (typeof meta.key !== 'string' || typeof meta.contentType !== 'string' && meta.contentType !== null) return null;
    return meta;
  } catch (_) {
    return null;
  }
}

function multipartXmlParts(bytes) {
  if (bytes.length > MAX_MULTIPART_XML_BYTES) throw new Error('multipart completion XML exceeds the configured limit');
  const xml = new TextDecoder().decode(Uint8Array.from(bytes));
  if (!/^\s*<CompleteMultipartUpload[\s>]/.test(xml) || !/<\/CompleteMultipartUpload>\s*$/.test(xml)) {
    throw new Error('invalid CompleteMultipartUpload XML');
  }
  const parts = [];
  const pattern = /<Part>\s*<PartNumber>(\d+)<\/PartNumber>\s*(?:<ETag>[^<]*<\/ETag>\s*)?<\/Part>/g;
  let match;
  while ((match = pattern.exec(xml)) !== null) {
    const number = Number(match[1]);
    if (!Number.isSafeInteger(number) || number < 1 || number > MAX_MULTIPART_PARTS) {
      throw new Error('multipart part number is out of range');
    }
    if (parts.length > 0 && number <= parts[parts.length - 1]) {
      throw new Error('multipart parts must be listed in strictly increasing order');
    }
    parts.push(number);
  }
  if (parts.length === 0) throw new Error('multipart completion requires at least one part');
  if (parts.length > MAX_MULTIPART_PARTS) throw new Error('multipart part count exceeds the configured limit');
  return parts;
}

function multipartObjects(uploadId) {
  return store.listObjects(namespace, `${MULTIPART_PREFIX}${uploadId}/part/`)
    .filter(object => !object.deleted);
}

function cleanupMultipart(uploadId) {
  for (const object of multipartObjects(uploadId)) {
    try { store.delete(namespace, object.key); } catch (_) { /* deferred cleanup is safe */ }
  }
  try { store.delete(namespace, multipartMetaKey(uploadId)); } catch (_) { /* deferred cleanup is safe */ }
}

function createMultipart(key, contentType) {
  const uploadId = multipartId();
  putSmallObject(multipartMetaKey(uploadId), text(JSON.stringify({ key, contentType })), 'application/json');
  return response(200, text(`<InitiateMultipartUploadResult><Bucket>${xmlEscape(namespace)}</Bucket><Key>${xmlEscape(key)}</Key><UploadId>${xmlEscape(uploadId)}</UploadId></InitiateMultipartUploadResult>`), 'application/xml');
}

function uploadMultipartPart(request, key, uploadId, partNumber) {
  const number = Number(partNumber);
  if (!Number.isSafeInteger(number) || number < 1 || number > MAX_MULTIPART_PARTS) {
    return s3Error(400, 'InvalidRequest', 'partNumber must be between 1 and 10000');
  }
  const meta = multipartMeta(uploadId);
  if (meta === null || meta.key !== key) return s3Error(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');
  const writer = store.beginWrite(namespace, multipartPartKey(uploadId, number), meta.contentType);
  const declaredLength = firstHeaderValue(request.headers(), 'content-length');
  let written;
  try {
    written = streamBody(request, writer, MAX_MULTIPART_PART_BYTES);
  } catch (error) {
    try { writer.abort(); } catch (_) { /* best effort; incomplete part is not visible */ }
    throw error;
  }
  if (declaredLength !== null && (!/^\d+$/.test(declaredLength) || BigInt(declaredLength) !== written)) {
    writer.abort();
    return s3Error(400, 'InvalidRequest', 'Content-Length does not match the request body');
  }
  writer.commit();
  return response(200);
}

function completeMultipart(request, key, uploadId) {
  const meta = multipartMeta(uploadId);
  if (meta === null || meta.key !== key) return s3Error(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');
  let parts;
  try {
    parts = multipartXmlParts(readRequestBody(request));
  } catch (error) {
    return s3Error(400, 'InvalidRequest', errorText(error));
  }
  const available = new Map(multipartObjects(uploadId).map(object => [object.key, object]));
  for (const number of parts) {
    if (!available.has(multipartPartKey(uploadId, number))) {
      return s3Error(400, 'InvalidRequest', `multipart part ${number} is missing`);
    }
  }
  const writer = store.beginWrite(namespace, key, meta.contentType);
  try {
    for (const number of parts) {
      const part = available.get(multipartPartKey(uploadId, number));
      let offset = 0n;
      const size = BigInt(part.size);
      while (offset < size) {
        const length = size - offset > 65536n ? 65536n : size - offset;
        const chunk = store.readRange(namespace, part.key, offset, length);
        if (chunk.length === 0) throw new Error(`multipart part ${number} ended unexpectedly`);
        writer.write(Uint8Array.from(chunk));
        offset += BigInt(chunk.length);
      }
    }
    const committed = writer.commit();
    cleanupMultipart(uploadId);
    return response(200, text(`<CompleteMultipartUploadResult><Key>${xmlEscape(key)}</Key><ETag>${xmlEscape(objectEtag(committed))}</ETag><VersionId>${xmlEscape(String(committed.version))}</VersionId></CompleteMultipartUploadResult>`), 'application/xml');
  } catch (error) {
    try { writer.abort(); } catch (_) { /* best effort; incomplete object is not visible */ }
    throw error;
  }
}

function readRequestBody(request) {
  const body = request.consume();
  const input = body.stream();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const ready = input.subscribe();
      ready.block();
      ready[Symbol.dispose]();
      let chunk;
      try { chunk = input.read(65536n); }
      catch (error) {
        if (error?.payload?.tag === 'closed' || error?.tag === 'closed') break;
        throw error;
      }
      if (chunk.length === 0) continue;
      total += chunk.length;
      if (total > MAX_MULTIPART_XML_BYTES) throw new Error('request body exceeds the configured multipart XML limit');
      chunks.push(Uint8Array.from(chunk));
    }
  } finally {
    input[Symbol.dispose]();
    const trailers = IncomingBody.finish(body);
    const trailersReady = trailers.subscribe();
    trailersReady.block();
    trailersReady[Symbol.dispose]();
    trailers.get();
  }
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.length; }
  return output;
}

export const incomingHandler = {
  handle(request, responseOutparam) {
    let result;
    try {
      const target = targetFromRequest(request);
      const key = target.key;
      const bucketRequest = target.bucket !== null;
      const method = methodName(request);
      const query = queryFromRequest(request);
      const declaredPayloadHash = firstHeaderValue(request.headers(), 'x-amz-content-sha256');
      let authenticationError = null;
      try {
        auth.verify();
      } catch (error) {
        authenticationError = s3Error(403, 'AccessDenied', errorText(error).slice(0, MAX_ERROR_BYTES));
      }
      if (authenticationError !== null) {
        result = authenticationError;
      } else if (key === null && method === 'GET' && query.has('location')) {
        result = response(200, text('<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>'), 'application/xml');
      } else if (key === null && method === 'GET' && (query.get('list-type') === '2' || bucketRequest)) {
        const listed = store.listObjects(namespace, query.get('prefix'));
        result = response(200, text(listXml(listed)), 'application/xml');
      } else if (bucketRequest && key === null && method === 'HEAD') {
        result = response(200);
      } else if (bucketRequest && key === null && method === 'PUT') {
        result = response(200);
      } else if (key === null) {
        result = response(400, text('object key is required\n'));
      } else {
        const uploadId = query.get('uploadId');
        const partNumber = query.get('partNumber');
        if (method === 'PUT' && query.has('uploads') && uploadId === null && partNumber === null) {
          result = createMultipart(key, firstHeaderValue(request.headers(), 'content-type'));
        } else if (method === 'PUT' && uploadId !== null && partNumber !== null) {
          result = uploadMultipartPart(request, key, uploadId, partNumber);
        } else if (method === 'POST' && uploadId !== null) {
          result = completeMultipart(request, key, uploadId);
        } else if (method === 'DELETE' && uploadId !== null) {
          const meta = multipartMeta(uploadId);
          if (meta === null || meta.key !== key) {
            result = s3Error(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');
          } else {
            cleanupMultipart(uploadId);
            result = response(204);
          }
        } else if (method === 'PUT') {
          const contentType = firstHeaderValue(request.headers(), 'content-type');
          const writer = store.beginWrite(namespace, key, contentType);
          const declaredLength = firstHeaderValue(request.headers(), 'content-length');
          const written = streamBody(request, writer);
          if (declaredLength !== null && (!/^\d+$/.test(declaredLength) || BigInt(declaredLength) !== written)) {
            writer.abort();
            result = s3Error(400, 'InvalidRequest', 'Content-Length does not match the request body');
          } else {
          const committed = writer.commit();
          result = response(200, new Uint8Array(), 'application/xml', [['etag', objectEtag(committed)]]);
          }
        } else if (method === 'GET' || method === 'HEAD') {
          let found;
          try {
            found = store.get(namespace, key);
          } catch (_) {
            found = null;
          }
          if (found === null) {
            result = s3Error(404, 'NoSuchKey', 'The specified key does not exist.');
          } else if (method === 'HEAD') {
            result = response(200, new Uint8Array(), found.contentType ?? 'application/octet-stream', [
              ['accept-ranges', 'bytes'],
              ['etag', objectEtag(found)],
              ['last-modified', S3_LAST_MODIFIED],
            ]);
          } else {
            const range = firstHeaderValue(request.headers(), 'range');
            let offset = 0n;
            let length = BigInt(found.size);
            if (range !== null) {
              const match = /^bytes=(\d+)-(\d*)$/.exec(range);
              if (match === null) {
                result = s3Error(416, 'InvalidRange', 'The requested range is not satisfiable.');
              } else {
                offset = BigInt(match[1]);
                const size = BigInt(found.size);
                const end = match[2] === '' ? size - 1n : BigInt(match[2]);
                if (size === 0n || offset > end || end >= size) {
                  result = s3Error(416, 'InvalidRange', 'The requested range is not satisfiable.');
                } else {
                  length = end - offset + 1n;
                }
              }
            }
            if (result === undefined) {
              const bytes = store.readRange(namespace, key, offset, length);
              const headers = [
                ['accept-ranges', 'bytes'],
                ['etag', objectEtag(found)],
                ['last-modified', S3_LAST_MODIFIED],
              ];
              if (range !== null) {
                headers.push(['content-range', `bytes ${offset}-${offset + BigInt(bytes.length) - 1n}/${found.size}`]);
              }
              result = response(range === null ? 200 : 206, bytes, found.contentType ?? 'application/octet-stream', headers);
            }
          }
        } else if (method === 'DELETE') {
          store.delete(namespace, key);
          result = response(204);
        } else {
          result = s3Error(405, 'InvalidRequest', 'method not allowed');
        }
      }
    } catch (error) {
      result = s3Error(500, 'InternalError', errorText(error));
    }
    ResponseOutparam.set(responseOutparam, { tag: 'ok', val: result });
  },
};
