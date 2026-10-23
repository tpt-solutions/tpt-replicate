#!/bin/sh
# CI-style checks for tpt-replicate. Twin of scripts/ci.ps1. Run from the repository root:
#   sh scripts/ci.sh
set -eu

step() {
    printf '\n==> %s\n' "$1"
    shift
    "$@"
}

step "format" cargo fmt --check
step "build, no default features" cargo build --no-default-features
step "build, all features and targets" cargo build --all-features --all-targets
step "test, no default features" cargo test
step "test, all features" cargo test --all-features
step "clippy, warnings are errors" cargo clippy --all-targets --all-features -- -D warnings
step "rustdoc, warnings are errors" env RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
step "bare-metal target: add" rustup target add thumbv7em-none-eabihf
step "no_std bare-metal build (thumbv7em-none-eabihf)" \
    cargo build --no-default-features --target thumbv7em-none-eabihf

printf '\n==> dependency audit (normal dependencies must be empty)\n'
deps=$(cargo tree -e normal --no-default-features --prefix none | grep -v '^tpt-replicate ' || true)
if [ -n "$deps" ]; then
    echo "unexpected normal dependencies:"
    echo "$deps"
    exit 1
fi

printf '\nall checks passed\n'
