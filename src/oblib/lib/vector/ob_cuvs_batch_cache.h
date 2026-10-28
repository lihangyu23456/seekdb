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

#ifndef OB_CUVS_BATCH_CACHE_H
#define OB_CUVS_BATCH_CACHE_H

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>

namespace oceanbase {
namespace common {
namespace obvsag {

/**
 * @brief A small LRU cache of GPU CAGRA indexes for the raw batch path.
 *
 * dbms_vector.batch_knn re-reads the base table on every call and used to build
 * a brand new CAGRA graph each time. The graph build (NN-Descent on the GPU)
 * costs orders of magnitude more than the search itself, so the index is kept
 * and reused while the base data stays the same.
 *
 * The cache key is a fingerprint over every float of the base buffer: a changed
 * row produces a different key, so the cache can never answer with an index
 * that was built from different data.
 *
 * The class holds no dependency on the database, which keeps the reuse rule
 * testable without a GPU.
 */
class CuvsBatchIndexCache {
public:
  /**
   * @brief One cached GPU index.
   *
   * The index handle is owned by index_ (its deleter releases the GPU
   * resources). mu_ serializes the searches on the same index, because a CAGRA
   * graph must not be searched by two threads at once.
   */
  struct Entry {
    std::shared_ptr<void> index_;
    std::mutex mu_;
    long n_ = 0;
    long dim_ = 0;
    uint64_t fingerprint_ = 0;
    uint64_t last_use_ = 0;
  };

  /**
   * @brief Builds the index for data that is not cached yet.
   *
   * The builder runs while the cache lock is held, so two concurrent calls over
   * the same data build the graph only once. It returns null when the build
   * fails; the caller then falls back to the one-shot path.
   */
  using Builder = std::shared_ptr<Entry> (*)(const float *base, long n, long dim, uint64_t fingerprint);

  explicit CuvsBatchIndexCache(size_t limit) : limit_(limit) {}

  /**
   * @brief Returns the index for the given base data.
   *
   * @param base     the base vectors, row major
   * @param n        the number of base vectors
   * @param dim      the dimension of a base vector
   * @param builder  builds the index when the data is not cached yet
   * @param reused   set to true when a cached index was returned
   * @return the index, or null when the data is not cached and the build failed
   */
  std::shared_ptr<Entry> get(const float *base, long n, long dim, Builder builder, bool *reused) {
    const uint64_t fingerprint = fingerprint_of(base, n, dim);
    std::lock_guard<std::mutex> guard(mu_);
    auto it = entries_.find(fingerprint);
    if (it != entries_.end() && it->second->n_ == n && it->second->dim_ == dim) {
      it->second->last_use_ = ++clock_;
      if (reused != nullptr) { *reused = true; }
      return it->second;
    }
    if (reused != nullptr) { *reused = false; }
    std::shared_ptr<Entry> entry = builder(base, n, dim, fingerprint);
    if (entry == nullptr) { return nullptr; }
    entry->fingerprint_ = fingerprint;
    entry->n_ = n;
    entry->dim_ = dim;
    entry->last_use_ = ++clock_;
    // Keep the cache small: the least recently used index is dropped first. A
    // running search keeps its own reference, so the GPU memory is released
    // once that search finishes.
    while (limit_ > 0 && entries_.size() >= limit_) {
      auto oldest = entries_.begin();
      for (auto candidate = entries_.begin(); candidate != entries_.end(); ++candidate) {
        if (candidate->second->last_use_ < oldest->second->last_use_) { oldest = candidate; }
      }
      entries_.erase(oldest);
    }
    if (limit_ > 0) { entries_[fingerprint] = entry; }
    return entry;
  }

  /**
   * @brief Fingerprint of the base vectors.
   *
   * Every byte of the buffer takes part, so two different data sets only share
   * a key when they really are identical.
   */
  static uint64_t fingerprint_of(const float *base, long n, long dim) {
    uint64_t h = mix64(((uint64_t)n << 32) ^ (uint64_t)dim);
    const size_t bytes = (size_t)n * (size_t)dim * sizeof(float);
    const uint8_t *data = reinterpret_cast<const uint8_t *>(base);
    size_t offset = 0;
    for (; offset + 16 <= bytes; offset += 16) {
      uint64_t first = 0;
      uint64_t second = 0;
      memcpy(&first, data + offset, sizeof(first));
      memcpy(&second, data + offset + 8, sizeof(second));
      h = (h ^ first ^ mix64(second)) * 0x100000001b3ULL;
      h ^= h >> 29;
    }
    for (; offset + 8 <= bytes; offset += 8) {
      uint64_t word = 0;
      memcpy(&word, data + offset, sizeof(word));
      h = (h ^ word) * 0x100000001b3ULL;
    }
    for (; offset < bytes; ++offset) {
      h = (h ^ (uint64_t)data[offset]) * 0x100000001b3ULL;
    }
    return mix64(h);
  }

  size_t limit() const { return limit_; }

private:
  static uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
  }

  size_t limit_;
  std::mutex mu_;
  std::map<uint64_t, std::shared_ptr<Entry>> entries_;
  uint64_t clock_ = 0;
};

} // namespace obvsag
} // namespace common
} // namespace oceanbase

#endif // OB_CUVS_BATCH_CACHE_H
