#!/usr/bin/env sh
set -eu

command -v componentize-js >/dev/null 2>&1 || {
  echo "componentize-js is required; install the pinned tool from gateway/README.md" >&2
  exit 127
}
command -v wasm-tools >/dev/null 2>&1 || {
  echo "wasm-tools is required for component validation" >&2
  exit 127
}

output=${1:-gateway.wasm}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
componentize-js "$script_dir/gateway.js" --wit "$script_dir/wit" --world-name gateway --out "$output"
wasm-tools validate "$output"
sha256sum "$output"
