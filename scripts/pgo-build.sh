#!/usr/bin/env bash
# Profile-Guided Optimization build script for linera, linera-proxy, linera-server.
#
# Usage:
#   ./scripts/pgo-build.sh [--skip-workload] [--lto]
#
# Requirements:
#   - rustup component add llvm-tools    (for llvm-profdata)
#   - A running storage-service (linera-storage-service) for the workload tests
#
# The script performs:
#   1. Instrumented build  (cargo build --release with -Cprofile-generate)
#   2. Workload execution  (runs the pgo_workload integration test)
#   3. Profile merging     (llvm-profdata merge)
#   4. Optimized build     (cargo build --release with -Cprofile-use)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGO_DATA_DIR="${REPO_ROOT}/target/pgo-data"
MERGED_PROF="${PGO_DATA_DIR}/merged.profdata"

SKIP_WORKLOAD=false
USE_LTO=false

for arg in "$@"; do
    case "$arg" in
        --skip-workload) SKIP_WORKLOAD=true ;;
        --lto)           USE_LTO=true ;;
        *)               echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# Find llvm-profdata from the rustup toolchain
find_llvm_profdata() {
    local sysroot
    sysroot="$(rustc --print sysroot)"
    local host
    host="$(rustc -vV | grep '^host:' | cut -d' ' -f2)"
    local profdata="${sysroot}/lib/rustlib/${host}/bin/llvm-profdata"
    if [ -x "$profdata" ]; then
        echo "$profdata"
        return
    fi
    # Fallback: look in PATH
    if command -v llvm-profdata &>/dev/null; then
        echo "llvm-profdata"
        return
    fi
    echo "ERROR: llvm-profdata not found. Run: rustup component add llvm-tools" >&2
    exit 1
}

LLVM_PROFDATA="$(find_llvm_profdata)"
echo "Using llvm-profdata: ${LLVM_PROFDATA}"

# =========================================================================
# Phase 1: Instrumented build
# =========================================================================
echo ""
echo "=== Phase 1: Instrumented build ==="
echo ""

rm -rf "${PGO_DATA_DIR}"
mkdir -p "${PGO_DATA_DIR}"

EXTRA_RUSTFLAGS=""
if [ "$USE_LTO" = true ]; then
    EXTRA_RUSTFLAGS="-Clto=fat"
fi

RUSTFLAGS="-Cprofile-generate=${PGO_DATA_DIR} ${EXTRA_RUSTFLAGS}" \
    cargo build --release -p linera-service --manifest-path "${REPO_ROOT}/Cargo.toml"

echo "Instrumented binaries built successfully."

# =========================================================================
# Phase 2: Run workload
# =========================================================================
if [ "$SKIP_WORKLOAD" = false ]; then
    echo ""
    echo "=== Phase 2: Running PGO workload ==="
    echo ""

    # The workload test uses the instrumented binaries from target/release.
    # Because LocalNetConfig resolves binaries from the current binary's dir,
    # and `cargo test` compiles a test binary into target/release/deps,
    # we need to ensure the instrumented linera/linera-server/linera-proxy
    # are the ones in target/release. The cargo test invocation below builds
    # against the same profile, so the test harness picks them up.
    RUSTFLAGS="-Cprofile-generate=${PGO_DATA_DIR} ${EXTRA_RUSTFLAGS}" \
        cargo test --release -p linera-service --features storage-service \
        --test pgo_workload -- --nocapture test_pgo_workload \
        --manifest-path "${REPO_ROOT}/Cargo.toml" \
        2>&1 | tee "${PGO_DATA_DIR}/workload.log"

    echo ""
    echo "Workload complete. Profile data written to ${PGO_DATA_DIR}/"
    echo "Number of .profraw files: $(find "${PGO_DATA_DIR}" -name '*.profraw' | wc -l)"
else
    echo ""
    echo "=== Phase 2: Skipped (--skip-workload) ==="
    echo "Make sure ${PGO_DATA_DIR}/ contains .profraw files from a prior run."
fi

# =========================================================================
# Phase 3: Merge profiles
# =========================================================================
echo ""
echo "=== Phase 3: Merging profiles ==="
echo ""

PROFRAW_COUNT=$(find "${PGO_DATA_DIR}" -name '*.profraw' | wc -l)
if [ "$PROFRAW_COUNT" -eq 0 ]; then
    echo "ERROR: No .profraw files found in ${PGO_DATA_DIR}/" >&2
    echo "The workload may not have generated profile data." >&2
    exit 1
fi

"${LLVM_PROFDATA}" merge -o "${MERGED_PROF}" "${PGO_DATA_DIR}"

echo "Merged ${PROFRAW_COUNT} profile(s) into ${MERGED_PROF}"

# =========================================================================
# Phase 4: Optimized build
# =========================================================================
echo ""
echo "=== Phase 4: Optimized build with PGO ==="
echo ""

RUSTFLAGS="-Cprofile-use=${MERGED_PROF} -Cllvm-args=-pgo-warn-missing-function ${EXTRA_RUSTFLAGS}" \
    cargo build --release -p linera-service --manifest-path "${REPO_ROOT}/Cargo.toml"

echo ""
echo "=== PGO build complete ==="
echo ""
echo "Optimized binaries are in:"
echo "  ${REPO_ROOT}/target/release/linera"
echo "  ${REPO_ROOT}/target/release/linera-proxy"
echo "  ${REPO_ROOT}/target/release/linera-server"
