#!/usr/bin/env sh
set -eu

version=${PADDOCK_S3_GATEWAY_VERSION:-0.1.0-alpha.1}
package_name="paddock-s3-gateway-${version}"
output_dir=${1:-dist}
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT INT TERM

mkdir -p "$output_dir" "$work_dir/$package_name"
(cd "$(dirname "$0")" && ./build-gateway.sh "$work_dir/$package_name/paddock-s3-gateway.wasm") >/dev/null
artifact="$work_dir/$package_name/paddock-s3-gateway.wasm"
artifact_sha=$(sha256sum "$artifact" | awk '{print $1}')
artifact_size=$(wc -c < "$artifact" | tr -d ' ')
sed \
  -e "s/@VERSION@/$version/" \
  -e "s/@ARTIFACT_SHA256@/$artifact_sha/" \
  -e "s/@ARTIFACT_SIZE@/$artifact_size/" \
  "$(dirname "$0")/gateway-package.json" > "$work_dir/$package_name/release-manifest.json"
tar -C "$work_dir" -czf "$output_dir/$package_name.tar.gz" "$package_name"
sha256sum "$output_dir/$package_name.tar.gz" > "$output_dir/$package_name.tar.gz.sha256"
echo "$output_dir/$package_name.tar.gz"
