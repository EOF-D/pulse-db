/**
 * @file include/pulsedb/cache/buffer_pool.hpp
 * @brief The BufferPool class used in the cache system. Manages caching and eviction with LRU
 * policy.
 */

#ifndef PULSEDB_CACHE_BUFFER_POOL_HPP
#define PULSEDB_CACHE_BUFFER_POOL_HPP

#include "pulsedb/cache/frame.hpp"
#include "pulsedb/cache/policies/lru_replacer.hpp"
#include "pulsedb/storage/disk_manager.hpp"
#include "pulsedb/storage/page.hpp"
#include "pulsedb/utils/logger.hpp"

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

/**
 * @namespace pulse::cache
 * @brief The namespace for the cache system.
 */
namespace pulse::cache {
  /**
   * @class BufferPool
   * @brief Manages a pool of memory frames for caching pages.
   */
  class BufferPool {
  public:
    /**
     * @brief Constructs a new buffer pool.
     * @param diskManager The disk manager to use.
     * @param poolSize Size of pool in frames.
     * @note Default pool size is 1024 frames. 4MB of memory (1024 * 4KB)
     */
    explicit BufferPool(storage::DiskManager &diskManager, size_t poolSize = 1024) noexcept;

    /**
     * @brief Clean up resources.
     */
    ~BufferPool() noexcept;

    // Disable copy operations.
    BufferPool(const BufferPool &) = delete;
    BufferPool &operator=(const BufferPool &) = delete;

    /**
     * @brief Fetch page into buffer pool.
     * @param pageId The ID of the page to fetch.
     * @return Pointer to the page in memory or nullptr.
     */
    [[nodiscard]] storage::Page *fetchPage(uint32_t pageId);

    /**
     * @brief Create a new page in the buffer pool.
     * @param type The type of page to create.
     * @param isLeaf Whether the page is a leaf node or not.
     * @param level The level of the page in the B+ tree.
     * @return Pointer to the new page.
     */
    [[nodiscard]] storage::Page *
    createPage(storage::PageType type, bool isLeaf = true, uint16_t level = 0);

    /**
     * @brief Delete a page from the buffer pool and disk.
     * @param pageId The ID of the page to delete.
     * @return True if successful, otherwise false.
     */
    bool deletePage(uint32_t pageId);

    /**
     * @brief Unpin a page from the buffer pool.
     * @param pageId The ID of the page to unpin.
     * @param dirty Whether the page was modified or not.
     * @return True if unpinned, otherwise false.
     */
    bool unpinPage(uint32_t pageId, bool dirty);

    /**
     * @brief Flush a specific page to the disk.
     * @param pageId The ID of the page to flush.
     * @return True if flushed, otherwise false.
     */
    bool flushPage(uint32_t pageId);

    /**
     * @brief Flush all pages in the buffer pool to the disk.
     */
    void flushAll();

    /**
     * @brief Get the number of frames in use.
     * @return Number of frames currently holding pages.
     */
    [[nodiscard]] size_t size() const noexcept { return pageTable.size(); }

  private:
    /**
     * @brief Find a frame to evict.
     * @return Frame ID if found, nullopt if none available.
     */
    [[nodiscard]] std::optional<size_t> findVictim();

    /**
     * @brief Evict a page from the given frame.
     * @param frameId The ID of the frame to evict.
     * @return True if evicted, otherwise false.
     */
    bool evictFrame(size_t frameId);

    std::vector<Frame> frames;                      /**< Pool of frames. */
    std::unordered_map<uint32_t, size_t> pageTable; /**< Page IDs to frames. */
    LRUReplacer replacer;                           /**< Page replacement policy. */

    utils::Logger logger;              /**< Logger instance. */
    storage::DiskManager &diskManager; /**< Disk manager instance. */
    mutable std::mutex mutex;          /**< Mutex for thread safety. */
  };
} // namespace pulse::cache

#endif // PULSEDB_CACHE_BUFFER_POOL_HPP
