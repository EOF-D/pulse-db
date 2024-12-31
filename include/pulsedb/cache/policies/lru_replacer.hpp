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
    void pin(size_t frameId) override;

    /**
     * @brief Record a frame access, moving it to most recently used.
     * @param frameId The ID of the frame that was accessed.
     */
    void unpin(size_t frameId) override;

    /**
     * @brief Get the least recently used frame.
     * @return The ID of the LRU frame, or nullopt if no frames available.
     */
    [[nodiscard]] std::optional<size_t> victim() override;

  private:
    mutable std::mutex mutex;    /**< Mutex for thread safety. */
    std::list<size_t> frameList; /**< List of frame IDs in LRU order. */
    std::unordered_map<size_t, std::list<size_t>::iterator>
        frameMap; /**< Frame to list positions. */
  };
} // namespace pulse::cache

#endif // PULSEDB_CACHE_POLICIES_LRU_REPLACER_HPP
