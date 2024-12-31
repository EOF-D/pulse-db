/**
 * @file include/pulsedb/cache/replacer.hpp
 * @brief The Replacer interface for buffer pool page replacement.
 */

#ifndef PULSEDB_CACHE_REPLACER_HPP
#define PULSEDB_CACHE_REPLACER_HPP

#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>

/**
 * @namespace pulse::cache
 * @brief The namespace for the cache system.
 */
namespace pulse::cache {
  /**
   * @class Replacer
   * @brief Abstract interface for page replacement policies.
   */
  class Replacer {
  public:
    virtual ~Replacer() = default;

    /**
     * @brief Record a frame as pinned, removing it from eviction candidates.
     * @param frameId The ID of the frame to pin.
     */
    virtual void pin(size_t frameId) = 0;

    /**
     * @brief Record a frame access, marking it as unpinned.
     * @param frameId The ID of the frame to unpin.
     */
    virtual void unpin(size_t frameId) = 0;

    /**
     * @brief Select a victim frame for eviction based on the policy.
     * @return The frame ID to evict, or nullopt if no frames are available.
     */
    [[nodiscard]] virtual std::optional<size_t> victim() = 0;
  };
} // namespace pulse::cache

#endif // PULSEDB_CACHE_REPLACER_HPP
