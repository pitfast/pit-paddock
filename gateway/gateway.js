import {
  Fields,
  IncomingRequest,
  IncomingBody,
  OutgoingBody,
  OutgoingResponse,
  ResponseOutparam,
} from 'wasi:http/types@0.2.0';
import * as store from 'pitfast:paddock/store@0.1.0';

const namespace = 's3-alpha';

function response(status, body = new Uint8Array(), contentType = 'text/plain') {
  const headers = new Fields();
  headers.set('content-type', contentType);
  headers.set('cache-control', 'no-store');
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

function streamBody(request, writer) {
  const body = request.consume();
  const input = body.stream();
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
      writer.write(chunk);
    }
  } finally {
    input[Symbol.dispose]();
    const trailers = IncomingBody.finish(body);
    const trailersReady = trailers.subscribe();
    trailersReady.block();
    trailersReady[Symbol.dispose]();
    trailers.get();
  }
}

function keyFromRequest(request) {
  const path = request.pathWithQuery().split('?', 1)[0];
  if (!path.startsWith('/')) return null;
  const key = path.slice(1);
  return key.length > 0 ? key : null;
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

export const incomingHandler = {
  handle(request, responseOutparam) {
    let result;
    try {
      const key = keyFromRequest(request);
      const method = methodName(request);
      const query = queryFromRequest(request);
      if (key === null && method === 'GET' && query.get('list-type') === '2') {
        const listed = store.listObjects(namespace, query.get('prefix'));
        result = response(200, text(JSON.stringify({
            name: namespace,
            keyCount: listed.length,
            keys: listed.map(objectJson),
          }) + '\n'), 'application/json');
      } else if (key === null) {
        result = response(400, text('object key is required\n'));
      } else {
        if (method === 'PUT') {
          const contentType = firstHeaderValue(request.headers(), 'content-type');
          const writer = store.beginWrite(namespace, key, contentType);
          streamBody(request, writer);
          const committed = writer.commit();
          result = response(200, text(JSON.stringify(objectJson(committed)) + '\n'), 'application/json');
        } else if (method === 'GET' || method === 'HEAD') {
          let found;
          try {
            found = store.get(namespace, key);
          } catch (_) {
            found = null;
          }
          if (found === null) {
            result = response(404, text('not found\n'));
          } else if (method === 'HEAD') {
            result = response(200, new Uint8Array(), found.contentType ?? 'application/octet-stream');
          } else {
            const range = firstHeaderValue(request.headers(), 'range');
            let offset = 0n;
            let length = BigInt(found.size);
            if (range !== null) {
              const match = /^bytes=(\d+)-(\d*)$/.exec(range);
              if (match === null) {
                result = response(416, text('invalid range\n'));
              } else {
                offset = BigInt(match[1]);
                const size = BigInt(found.size);
                const end = match[2] === '' ? size - 1n : BigInt(match[2]);
                if (size === 0n || offset > end || end >= size) {
                  result = response(416, text('invalid range\n'));
                } else {
                  length = end - offset + 1n;
                }
              }
            }
            if (result === undefined) {
              const bytes = store.readRange(namespace, key, offset, length);
              result = response(range === null ? 200 : 206, bytes, found.contentType ?? 'application/octet-stream');
            }
          }
        } else if (method === 'DELETE') {
          store.delete(namespace, key);
          result = response(204);
        } else {
          result = response(405, text('method not allowed\n'));
        }
      }
    } catch (error) {
      result = response(500, text(`gateway failure: ${errorText(error)}\n`));
    }
    ResponseOutparam.set(responseOutparam, { tag: 'ok', val: result });
  },
};
