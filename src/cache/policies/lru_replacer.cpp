/**
 * @file src/cache/policies/lru_replacer.cpp
 * @brief Implements the LRUReplacer class for buffer pool page replacement.
 */

#include "pulsedb/cache/policies/lru_replacer.hpp"

namespace pulse::cache {
  void LRUReplacer::pin(size_t frameId) {
    std::lock_guard lock(mutex);

    auto it = frameMap.find(frameId);
    if (it != frameMap.end()) {
      frameList.erase(it->second);
      frameMap.erase(it);
    }
  }

  void LRUReplacer::unpin(size_t frameId) {
    std::lock_guard lock(mutex);

    // If frame is already tracked, remove the old entry.
    auto it = frameMap.find(frameId);
    if (it != frameMap.end()) {
      frameList.erase(it->second);
      frameMap.erase(it);
    }

    // Add to front of LRU list.
    frameList.push_front(frameId);
    frameMap[frameId] = frameList.begin();
  }

  [[nodiscard]] std::optional<size_t> LRUReplacer::victim() {
    std::lock_guard lock(mutex);

    if (frameList.empty()) {
      return std::nullopt;
    }

    // Get victim from back of list.
    size_t victimId = frameList.back();
    frameList.pop_back();
    frameMap.erase(victimId);

    return victimId;
  }
} // namespace pulse::cache
