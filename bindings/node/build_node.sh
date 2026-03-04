#!/bin/bash

rustup target add x86_64-apple-darwin \
                  aarch64-apple-darwin \
                  x86_64-unknown-linux-gnu \
                  aarch64-unknown-linux-gnu \
                  x86_64-unknown-linux-musl \
                  aarch64-unknown-linux-musl \
                  x86_64-pc-windows-msvc \
                  aarch64-pc-windows-msvc

output_dir="dist_node"
mkdir -p "$output_dir"

targets=(
    "x86_64-apple-darwin"
    "aarch64-apple-darwin"
    "x86_64-unknown-linux-gnu"
    "aarch64-unknown-linux-gnu"
    "x86_64-unknown-linux-musl"
    "aarch64-unknown-linux-musl"
    "x86_64-pc-windows-msvc"
    "aarch64-pc-windows-msvc"
)

echo "========================================"
echo "Building Node.js bindings via N-API"
echo "========================================"

for target in "${targets[@]}"; do
    echo " -> Target: $target"
    
    npx napi build --release \
                   --target "$target" \
                   --platform \
                   --cross-compile \
                   -o "$output_dir"
done

echo "🎉 All Node.js binaries built successfully in ./$output_dir"