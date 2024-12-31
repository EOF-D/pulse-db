/**
 * @file include/pulsedb/cache/policies/lru_replacer.hpp
 * @brief The LRUReplacer class for buffer pool page replacement.
 */

#ifndef PULSEDB_CACHE_POLICIES_LRU_REPLACER_HPP
#define PULSEDB_CACHE_POLICIES_LRU_REPLACER_HPP

#include "pulsedb/cache/replacer.hpp"

/**
 * @namespace pulse::cache
 * @brief The namespace for the cache system.
 */
namespace pulse::cache {
  /**
   * @class LRUReplacer
   * @brief LRU page replacement implementation.
   */
  class LRUReplacer : public Replacer {
  public:
    /**
     * @brief Constructs a new LRU replacer.
     */
    LRUReplacer() = default;

    /**
     * @brief Remove a frame from replacement consideration.
     * @param frameId The ID of the frame to remove.
     */
    void pin(size_t frameId) override {
      std::lock_guard lock(mutex);

      auto it = frameMap.find(frameId);
      if (it != frameMap.end()) {
        frameList.erase(it->second);
        frameMap.erase(it);
      }
    }

    /**
     * @brief Record a frame access, moving it to most recently used.
     * @param frameId The ID of the frame that was accessed.
     */
    void unpin(size_t frameId) override {
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

    /**
     * @brief Get the least recently used frame.
     * @return The ID of the LRU frame, or nullopt if no frames available.
     */
    [[nodiscard]] std::optional<size_t> victim() override {
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

  private:
    mutable std::mutex mutex;    /**< Mutex for thread safety. */
    std::list<size_t> frameList; /**< List of frame IDs in LRU order. */
    std::unordered_map<size_t, std::list<size_t>::iterator>
        frameMap; /**< Frame to list positions. */
  };
} // namespace pulse::cache

#endif // PULSEDB_CACHE_POLICIES_LRU_REPLACER_HPP
