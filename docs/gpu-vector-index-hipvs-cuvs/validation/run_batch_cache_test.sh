#!/usr/bin/env bash
#
# Checks the reuse rule of the GPU CAGRA cache that dbms_vector.batch_knn
# uses. It needs no GPU and no database, so the rule can be verified anywhere.
#
#   bash docs/gpu-vector-index-hipvs-cuvs/validation/run_batch_cache_test.sh
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
CXX="${CXX:-g++}"
OUT="${OUT:-$(mktemp -d)/test_batch_cache}"

"$CXX" -std=c++20 -O2 -Wall -Wextra -Werror \
  -I "$ROOT/src/oblib/lib/vector" \
  "$HERE/test_batch_cache.cpp" \
  -o "$OUT"
"$OUT"
