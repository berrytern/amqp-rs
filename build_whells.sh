#!/bin/bash

# 1. Add all required Rust targets
rustup target add x86_64-apple-darwin \
                  aarch64-apple-darwin \
                  x86_64-unknown-linux-gnu \
                  aarch64-unknown-linux-gnu \
                  x86_64-unknown-linux-musl \
                  aarch64-unknown-linux-musl \
                  x86_64-pc-windows-msvc \
                  aarch64-pc-windows-msvc

# 2. Prepare output directory
output_dir="dist_py"
rm -rf "$output_dir"

# 3. Define versions and all targets in a single array
py_versions=("3.8" "3.9" "3.10" "3.11" "3.12" "3.13" "3.13t" "3.14" "3.14t")

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

# 4. Activate environment (assuming maturin is installed here)
source .venv/bin/activate

# 5. Build wheels
for ver in "${py_versions[@]}"; do
    echo "========================================"
    echo "Building for Python $ver"
    echo "========================================"
    
    # Use uv to fetch the exact path to the requested Python version
    py_exec=$(uv python find "$ver")

    for target in "${targets[@]}"; do
        echo " -> Target: $target"
        
        # If your pyproject.toml is fixed, you don't need the -m flag here.
        # If you prefer to skip modifying pyproject.toml, you can append: 
        # -m "path/to/crate/Cargo.toml" to this command.
        uv run maturin build --release \
                             --target "$target" \
                             --strip \
                             --zig \
                             --out "$output_dir" \
                             -i "$py_exec"
    done
done

echo "🎉 All wheels built successfully in ./$output_dir"