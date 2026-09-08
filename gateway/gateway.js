import {
  Fields,
  IncomingRequest,
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
  if (body.length > 0) stream.blockingWriteAndFlush(body);
  stream[Symbol.dispose]();
  OutgoingBody.finish(output, undefined);
  return outgoing;
}

function text(value) {
  return new TextEncoder().encode(value);
}

function streamBody(request, writer) {
  const input = request.consume().stream();
  try {
    while (true) {
      const chunk = input.blockingRead(65536n);
      if (chunk.length === 0) break;
      const writeResult = writer.write(chunk);
      const error = resultError(writeResult);
      if (error !== null) throw new Error(`write failed: ${JSON.stringify(error)}`);
    }
  } finally {
    input[Symbol.dispose]();
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

function resultError(result) {
  if (result.tag === 'ok') return null;
  return result.val;
}

export const incomingHandler = {
  handle(request, responseOutparam) {
    let result;
    try {
      const key = keyFromRequest(request);
      const query = queryFromRequest(request);
      if (key === null && request.method() === 'GET' && query.get('list-type') === '2') {
        const listed = store.listObjects(namespace, query.get('prefix'));
        const listError = resultError(listed);
        result = listError === null
          ? response(200, text(JSON.stringify({
            name: namespace,
            keyCount: listed.val.length,
            keys: listed.val.map((object) => ({
              key: object.key,
              version: object.version,
              size: object.size,
              digest: object.digest,
            })),
          }) + '\n'), 'application/json')
          : response(500, text(`list failed: ${JSON.stringify(listError)}\n`));
      } else if (key === null) {
        result = response(400, text('object key is required\n'));
      } else {
        const method = request.method();
        if (method === 'PUT') {
          const contentType = request.headers().get('content-type');
          const writerResult = store.beginWrite(namespace, key, contentType);
          const error = resultError(writerResult);
          if (error !== null) {
            result = response(500, text(`begin write failed: ${JSON.stringify(error)}\n`));
          } else {
            const writer = writerResult.val;
            streamBody(request, writer);
            const committed = writer.commit();
            const commitError = resultError(committed);
            result = commitError === null
              ? response(200, text(JSON.stringify(committed.val) + '\n'), 'application/json')
              : response(500, text(`commit failed: ${JSON.stringify(commitError)}\n`));
          }
        } else if (method === 'GET' || method === 'HEAD') {
          const found = store.get(namespace, key);
          const error = resultError(found);
          if (error !== null) {
            result = response(404, text('not found\n'));
          } else if (method === 'HEAD') {
            result = response(200, new Uint8Array(), found.val.contentType ?? 'application/octet-stream');
          } else {
            const range = request.headers().get('range');
            let offset = 0n;
            let length = BigInt(found.val.size);
            if (range !== null) {
              const match = /^bytes=(\\d+)-(\\d*)$/.exec(range);
              if (match === null) {
                result = response(416, text('invalid range\n'));
              } else {
                offset = BigInt(match[1]);
                const size = BigInt(found.val.size);
                const end = match[2] === '' ? size - 1n : BigInt(match[2]);
                if (size === 0n || offset > end || end >= size) {
                  result = response(416, text('invalid range\n'));
                } else {
                  length = end - offset + 1n;
                }
              }
            }
            if (result === undefined) {
              const bytesResult = store.readRange(namespace, key, offset, length);
              const bytesError = resultError(bytesResult);
              result = bytesError === null
                ? response(range === null ? 200 : 206, bytesResult.val, found.val.contentType ?? 'application/octet-stream')
                : response(500, text('read failed\n'));
            }
          }
        } else if (method === 'DELETE') {
          const deleted = store.delete(namespace, key);
          const error = resultError(deleted);
          result = error === null ? response(204) : response(404, text('not found\n'));
        } else {
          result = response(405, text('method not allowed\n'));
        }
      }
    } catch (error) {
      result = response(500, text(`gateway failure: ${error}\n`));
    }
    ResponseOutparam.set(responseOutparam, { tag: 'ok', val: result });
  },
};
