/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file test_batch_cache.cpp
 *
 * Checks the reuse rule of the GPU CAGRA cache that dbms_vector.batch_knn
 * uses. The rule is what keeps the answer correct: an index may only be reused
 * for exactly the same base data, so the test drives the cache with changed
 * data and watches the build count.
 *
 * It needs no GPU and no database:
 *
 *   g++ -std=c++20 -I src/oblib/lib/vector \
 *       docs/gpu-vector-index-hipvs-cuvs/validation/test_batch_cache.cpp \
 *       -o /tmp/test_batch_cache && /tmp/test_batch_cache
 */

#include "ob_cuvs_batch_cache.h"

#include <cstdio>
#include <cstdlib>
#include <vector>

using oceanbase::common::obvsag::CuvsBatchIndexCache;

namespace {

int g_failures = 0;

void check(bool condition, const char *what)
{
  if (condition) {
    printf("ok   - %s\n", what);
  } else {
    printf("FAIL - %s\n", what);
    g_failures++;
  }
}

// Counts the builds and hands out a handle that points at the base data, so
// the test can tell the reused index from a fresh one.
int g_builds = 0;
bool g_build_fails = false;

std::shared_ptr<CuvsBatchIndexCache::Entry> build(const float *base, long n, long dim,
                                                   uint64_t fingerprint)
{
  (void)base;
  (void)n;
  (void)dim;
  if (g_build_fails) { return nullptr; }
  g_builds++;
  auto entry = std::make_shared<CuvsBatchIndexCache::Entry>();
  // The real path owns a GPU handle here; the test only needs a marker.
  entry->index_ = std::shared_ptr<void>(reinterpret_cast<void *>(static_cast<uintptr_t>(fingerprint)),
                                        [](void *) {});
  return entry;
}

std::vector<float> make_base(long n, long dim, float seed)
{
  std::vector<float> data(static_cast<size_t>(n * dim));
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = seed + static_cast<float>(i) * 0.5f;
  }
  return data;
}

} // namespace

int main()
{
  const long n = 64;
  const long dim = 16;

  // 1. The same base data is built once and reused afterwards.
  {
    g_builds = 0;
    CuvsBatchIndexCache cache(1);
    std::vector<float> base = make_base(n, dim, 1.0f);
    bool reused = false;
    auto first = cache.get(base.data(), n, dim, build, &reused);
    check(first != nullptr && !reused, "the first call builds the index");
    auto second = cache.get(base.data(), n, dim, build, &reused);
    check(second != nullptr && reused, "the second call over the same data reuses it");
    check(g_builds == 1, "the graph is built exactly once");
    check(first == second, "the reused entry is the same object");
  }

  // 2. One changed float must not be served from the cache.
  {
    g_builds = 0;
    CuvsBatchIndexCache cache(1);
    std::vector<float> base = make_base(n, dim, 1.0f);
    bool reused = false;
    cache.get(base.data(), n, dim, build, &reused);
    base[17] += 0.25f;
    cache.get(base.data(), n, dim, build, &reused);
    check(g_builds == 2, "changed data is rebuilt");
    cache.get(base.data(), n, dim, build, &reused);
    check(g_builds == 2, "the changed data is reused after the rebuild");
  }

  // 3. The same shape with different content is a different entry.
  {
    g_builds = 0;
    CuvsBatchIndexCache cache(2);
    std::vector<float> first = make_base(n, dim, 1.0f);
    std::vector<float> second = make_base(n, dim, 100.0f);
    bool reused = false;
    cache.get(first.data(), n, dim, build, &reused);
    cache.get(second.data(), n, dim, build, &reused);
    check(g_builds == 2, "two data sets with the same shape are two indexes");
    check(CuvsBatchIndexCache::fingerprint_of(first.data(), n, dim)
              != CuvsBatchIndexCache::fingerprint_of(second.data(), n, dim),
          "the two data sets have different fingerprints");
    check(CuvsBatchIndexCache::fingerprint_of(first.data(), n, dim)
              == CuvsBatchIndexCache::fingerprint_of(first.data(), n, dim),
          "the fingerprint is stable for the same data");
  }

  // 4. The cache keeps at most `limit` indexes and drops the oldest one.
  {
    g_builds = 0;
    CuvsBatchIndexCache cache(1);
    std::vector<float> first = make_base(n, dim, 1.0f);
    std::vector<float> second = make_base(n, dim, 100.0f);
    bool reused = false;
    cache.get(first.data(), n, dim, build, &reused);
    cache.get(second.data(), n, dim, build, &reused);
    check(g_builds == 2, "the second data set is built");
    cache.get(first.data(), n, dim, build, &reused);
    check(g_builds == 3 && !reused, "the evicted data set is built again");
  }

  // 5. A limit of zero switches the cache off: every call builds.
  {
    g_builds = 0;
    CuvsBatchIndexCache cache(0);
    std::vector<float> base = make_base(n, dim, 1.0f);
    bool reused = false;
    cache.get(base.data(), n, dim, build, &reused);
    cache.get(base.data(), n, dim, build, &reused);
    check(g_builds == 2, "a cache of zero entries never reuses");
  }

  // 6. A failed build is reported, not cached.
  {
    g_builds = 0;
    g_build_fails = true;
    CuvsBatchIndexCache cache(1);
    std::vector<float> base = make_base(n, dim, 1.0f);
    bool reused = true;
    auto entry = cache.get(base.data(), n, dim, build, &reused);
    check(entry == nullptr, "a failed build returns no index");
    g_build_fails = false;
  }

  if (g_failures == 0) {
    printf("batch cache test PASS\n");
    return 0;
  }
  printf("batch cache test FAIL: %d check(s) failed\n", g_failures);
  return 1;
}
