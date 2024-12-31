/**
 * @file include/pulsedb/cache/frame.hpp
 * @brief The Frame class used in the cache system. Holds a page in memory with additional
 * metadata.
 */

#ifndef PULSEDB_CACHE_FRAME_HPP
#define PULSEDB_CACHE_FRAME_HPP

#include "pulsedb/storage/page.hpp"
#include <atomic>

/**
 * @namespace pulse::cache
 * @brief The namespace for the cache system.
 */
namespace pulse::cache {
  /**
   * @class Frame
   * @brief A frame in the buffer pool that holds a page and its metadata.
   */
  class Frame {
  public:
    /**
     * @brief Constructs a new frame.
     */
    explicit Frame() noexcept : page(nullptr), pageId(0), pinCount(0), dirty(false) {}

    /**
     * @brief Reset the frame with a new page.
     * @param newPage Unique pointer to the new page.
     */
    void reset(std::unique_ptr<storage::Page> newPage) noexcept {
      page = std::move(newPage);
      pageId = page ? page->id() : 0;
      pinCount = 0;
      dirty = false;
    }

    /**
     * @brief Pin the frame.
     * @return The new pin count.
     */
    size_t pin() noexcept { return ++pinCount; }

    /**
     * @brief Unpin the frame.
     * @return The new pin count.
     */
    size_t unpin() noexcept { return pinCount > 0 ? --pinCount : 0; }

    /**
     * @brief Getters for the frame class.
     * @{
     */

    /**
     * @brief Get the page ID of the frame.
     * @return The page ID of the frame.
     */
    [[nodiscard]] uint32_t id() const noexcept { return pageId; }

    /**
     * @brief Get the pin count of the frame.
     * @return The pin count of the frame.
     */
    [[nodiscard]] size_t pins() const noexcept { return pinCount; }

    /**
     * @brief Check if the frame is dirty.
     * @return True if the frame is dirty, false otherwise.
     */
    [[nodiscard]] bool isDirty() const noexcept { return dirty; }

    /**
     * @brief Get the page in the frame.
     * @return The page in the frame.
     */
    [[nodiscard]] storage::Page *getPage() noexcept { return page.get(); }

    /**
     * @brief Get the page in the frame.
     * @return The page in the frame.
     */
    [[nodiscard]] const storage::Page *getPage() const noexcept { return page.get(); }

    /**
     * @brief Check if the frame is unpinned.
     * @return True if the frame is unpinned, false otherwise.
     */
    [[nodiscard]] bool isUnpinned() const noexcept { return pinCount == 0; }

    /** @} */

    /**
     * @brief Setters for the frame class.
     * @{
     */

    /**
     * @brief Mark a frame as dirty.
     */
    void mark() noexcept { dirty = true; }

    /**
     * @brief Unmark a frame as dirty.
     */
    void unmark() noexcept { dirty = false; }

    /** @} */

  private:
    std::unique_ptr<storage::Page> page; /**< The page in the frame. */
    uint32_t pageId;                     /**< The page ID. */
    std::atomic<uint32_t> pinCount;      /**< The pin count. */
    std::atomic<bool> dirty;             /**< Whether the page is dirty or not. */
  };
} // namespace pulse::cache

#endif // PULSEDB_CACHE_FRAME_HPP
