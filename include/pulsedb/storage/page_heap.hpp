/**
 * @file include/pulsedb/storage/page_heap.hpp
 * @brief PageHeap class for managing allocations for free pages.
 */

#ifndef PULSEDB_STORAGE_PAGE_HEAP_HPP
#define PULSEDB_STORAGE_PAGE_HEAP_HPP

#include <cstdint>
#include <cstring>
#include <new>

namespace pulse::storage {
  /**
   * @class PageHeader
   * @brief Manages free pages using a min-heap.
   */
  class PageHeap {
  public:
    /**
     * @brief Constructs a new page heap.
     * @param capacity The maximum number of pages.
     * @note The default capacity is 1024 pages.
     */
    explicit PageHeap(size_t capacity = 1024);

    /**
     * @brief Cleans up resources.
     */
    ~PageHeap() noexcept;

    // Disable copy operations to prevent double-frees.
    PageHeap(const PageHeap &) = delete;
    PageHeap &operator=(const PageHeap &) = delete;

    // Allo move operations.
    PageHeap(PageHeap &&other) noexcept;
    PageHeap &operator=(PageHeap &&other) noexcept;

    /**
     * @brief Inserts a page ID into the heap.
     * @param pageId The page ID to insert.
     */
    void insert(uint32_t pageId);

    /**
     * @brief Get and pop the minimum page ID from the heap.
     * @return The minimum page ID.
     */
    uint32_t extractMin() noexcept;

    /**
     * @brief Returns the minimum page ID without popping it.
     * @return The minimum page ID.
     */
    [[nodiscard]] uint32_t minimum() const noexcept { return pages[0]; }

    /**
     * @brief Checks if the heap is empty.
     * @return True if the heap is empty, false otherwise.
     */
    [[nodiscard]] bool empty() const noexcept { return heapSize == 0; }

    /**
     * @brief Returns the current size of the heap.
     * @return The current size of the heap.
     */
    [[nodiscard]] size_t size() const noexcept { return heapSize; }

  private:
    /**
     * @brief Maintain the heap property by moving an element up.
     * @param index The index of the element to sift up.
     */
    void siftUp(size_t index) noexcept;

    /**
     * @brief Maintain the heap property by moving an element down.
     * @param index The index of the element to sift down.
     */
    void siftDown(size_t index) noexcept;

    /**
     * @brief Resizes the heap to double its current capacity.
     */
    void resize();

    /**
     * @brief Builds a heap from an unordered array.
     */
    void heapify() noexcept;

    uint32_t *pages; /**< Array of page IDs. */
    size_t capacity; /**< Current capacity. */
    size_t heapSize; /**< Current heap size. */
  };
} // namespace pulse::storage

#endif // PULSEDB_STORAGE_PAGE_HEAP_HPP
