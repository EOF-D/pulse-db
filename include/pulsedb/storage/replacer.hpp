/**
 * @file include/pulsedb/storage/replacer.hpp
 * @brief The Replacer interface and LRU implementation for buffer pool page replacement.
 */

#ifndef PULSEDB_STORAGE_REPLACER_HPP
#define PULSEDB_STORAGE_REPLACER_HPP

#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

/**
 * @namespace pulse::storage
 * @brief The namespace for the storage system.
 */
namespace pulse::storage {
  /**
   * @class Replacer
   * @brief Abstract interface for page replacement policies.
   */
  class Replacer {
  public:
    virtual ~Replacer() = default;

    /**
     * @brief Record a frame access, updating it's position in order.
     * @param frameId The ID of the accessed frame.
     */
    virtual void recordAccess(size_t frameId) = 0;

    /**
     * @brief Remove a frame from replacement candidates.
     * @param frameId The ID of frame to remove.
     */
    virtual void remove(size_t frameId) = 0;

    /**
     * @brief Select a victim frame for removal.
     * @return The frame ID to remove, or nullopt if none available.
     */
    [[nodiscard]] virtual std::optional<size_t> victim() = 0;

    /**
     * @brief Get current number of frames.
     * @return The number of frames.
     */
    [[nodiscard]] virtual size_t size() const = 0;
  };

  /**
   * @class LRUReplacer
   * @brief LRU page replacement implementation.
   */
  class LRUReplacer : public Replacer {
  public:
    /**
     * @brief Construct a new replacer with the given capacity.
     * @param capacity Maximum number of frames to track.
     */
    explicit LRUReplacer(size_t capacity = 1024) : capacity(capacity) { frames.reserve(capacity); }

    /**
     * @brief Record a frame access, updating it's position in order.
     * @param frameId The ID of the accessed frame.
     */
    void recordAccess(size_t frameId) override {
      std::unique_lock lock(mutex); // Exclusive access.

      // Remove if already present in the list.
      for (auto it = frames.begin(); it != frames.end(); ++it) {
        if (*it == frameId) {
          frames.erase(it);
          break;
        }
      }

      // If at capacity, remove oldest.
      if (frames.size() >= capacity) {
        frames.erase(frames.begin());
      }

      // Add to the back of the list.
      frames.push_back(frameId);
    }

    void remove(size_t frameId) override {
      std::unique_lock lock(mutex);

      // Remove if present in the list.
      for (auto it = frames.begin(); it != frames.end(); ++it) {
        if (*it == frameId) {
          frames.erase(it);
          break;
        }
      }
    }

    [[nodiscard]] std::optional<size_t> victim() override {
      std::unique_lock lock(mutex);
      if (frames.empty())
        return std::nullopt;

      // Return least recently used frame.
      size_t victimId = frames.front();
      frames.erase(frames.begin());

      return victimId;
    }

    [[nodiscard]] size_t size() const override {
      std::shared_lock lock(mutex);
      return frames.size();
    }

  private:
    const size_t capacity;           /**< Maximum frames to track. */
    std::vector<size_t> frames;      /**< Frame IDs in LRU order (oldest first). */
    mutable std::shared_mutex mutex; /**< Reader-writer lock. */
  };

} // namespace pulse::storage

#endif // PULSEDB_STORAGE_REPLACER_HPP
