#!/usr/bin/env bash
set -e

PY_CMD="python"
if ! command -v "$PY_CMD" >/dev/null 2>&1; then
    if command -v uv >/dev/null 2>&1; then
        PY_CMD="uv run python"
    else
        PY_CMD="python3"
    fi
fi

echo "Running benchmarks with Python: $($PY_CMD -V)..."
echo "Publishing and subscribing with pure Python implementation (sequential- uvloop)..."
$PY_CMD uv_publish_sub_pure_sequential.py
echo "Publishing and subscribing with Rust implementation (sequential- uvloop)..."
$PY_CMD uv_publish_sub_rs_sequential.py
echo "Publishing and subscribing with pure Python implementation (concurrent- uvloop)..."
$PY_CMD uv_publish_sub_pure_concurrent.py
echo "Publishing and subscribing with Rust implementation (concurrent- uvloop)..."
$PY_CMD uv_publish_sub_rs_concurrent.py
echo "Publishing and subscribing with Rust implementation (parallel- uvloop)..."
$PY_CMD uv_publish_sub_rs_parallel.py
echo "Rpc with pure Python implementation (sequential- asyncio)..."
$PY_CMD rpc_client_server_pure_sequential.py
echo "Rpc with pure Rust implementation (sequential- asyncio)..."
$PY_CMD rpc_client_server_rs_sequential.py
