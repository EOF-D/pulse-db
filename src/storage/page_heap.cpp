/**
 * @file src/storage/page_heap.cpp
 * @brief Implements the PageHeap class.
 */

#include "pulsedb/storage/page_heap.hpp"

namespace pulse::storage {
  PageHeap::PageHeap(size_t capacity) : capacity(capacity), heapSize(0) {
    pages = static_cast<uint32_t *>(::operator new(
        capacity * sizeof(uint32_t),
        std::align_val_t{64})); // Each page ID is 4 bytes, so we allocate capacity * 4 bytes.
  }

  PageHeap::~PageHeap() noexcept {
    if (pages) {
      ::operator delete(pages, std::align_val_t{64});
    }
  }

  PageHeap::PageHeap(PageHeap &&other) noexcept
      : pages(other.pages), capacity(other.capacity), heapSize(other.heapSize) {
    // Reset the other priority queue.
    other.pages = nullptr;
    other.capacity = 0;
    other.heapSize = 0;
  }

  PageHeap &PageHeap::operator=(PageHeap &&other) noexcept {
    if (this != &other) {
      // Clean up resources.
      if (pages) {
        ::operator delete(pages, std::align_val_t{64});
      }

      // Move the other priority queue.
      pages = other.pages;
      capacity = other.capacity;
      heapSize = other.heapSize;

      // Reset the other priority queue.
      other.pages = nullptr;
      other.capacity = 0;
      other.heapSize = 0;
    }

    return *this;
  }

  void PageHeap::insert(uint32_t pageId) {
    if (heapSize == capacity) {
      resize();
    }

    pages[heapSize] = pageId;
    siftUp(heapSize++);
  }

  uint32_t PageHeap::extractMin() noexcept {
    uint32_t min = pages[0];
    pages[0] = pages[--heapSize];

    if (heapSize > 0) {
      siftDown(0);
    }

    return min;
  }

  void PageHeap::siftUp(size_t index) noexcept {
    uint32_t element = pages[index]; // The element to sift up.

    while (index > 0) {
      // Calculate the parent index.
      size_t parent = (index - 1) >> 1;

      // If the parent is less than or equal to the element, we're done.
      if (pages[parent] <= element) {
        break;
      }

      // Move the parent down.
      pages[index] = pages[parent];
      index = parent;
    }

    pages[index] = element;
  }

  void PageHeap::siftDown(size_t index) noexcept {
    uint32_t element = pages[index]; // The element to sift down.

    // The last non-leaf node.
    size_t lastParent = (heapSize - 2) >> 1;
    while (index <= lastParent) {
      // Calculate the children indices.
      size_t leftChild = (index << 1) + 1;
      size_t rightChild = leftChild + 1;
      size_t minChild = leftChild;

      // Find the minimum child.
      if (rightChild < heapSize && pages[rightChild] < pages[leftChild]) {
        minChild = rightChild;
      }

      // Satifies the heap property.
      if (element <= pages[minChild]) {
        break;
      }

      // Move the minimum child up.
      pages[index] = pages[minChild];
      index = minChild;
    }

    // Place the element in it's final position.
    pages[index] = element;
  }

  void PageHeap::resize() {
    // Double the current capacity.
    size_t newCapacity = capacity * 2;
    uint32_t *newPages = static_cast<uint32_t *>(
        ::operator new(newCapacity * sizeof(uint32_t), std::align_val_t{64}));

    // Copy the elements to the new array.
    std::memcpy(newPages, pages, heapSize * sizeof(uint32_t));

    // Delete the old pages.
    ::operator delete(pages, std::align_val_t{64});

    // Update the pages and capacity.
    pages = newPages;
    capacity = newCapacity;
  }

  void PageHeap::heapify() noexcept {
    // If the heap size is less than or equal to 1, we're done.
    if (heapSize <= 1) {
      return;
    }

    // Start from the last non-leaf node and sift down.
    for (size_t i = (heapSize - 2) >> 1; i < heapSize; --i) {
      siftDown(i);
    }
  }
} // namespace pulse::storage
