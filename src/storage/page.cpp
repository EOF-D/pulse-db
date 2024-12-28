/**
 * @file src/storage/page.hpp
 * @brief Implements Page class for storage.
 */

#include "pulsedb/storage/page.hpp"

namespace pulse::storage {
  Page::Page(uint32_t pageId) noexcept {
    // Allocate page memory aligned to page size.
    data = static_cast<uint8_t *>(::operator new(PAGE_SIZE, std::align_val_t{64}));

    // Initialize header.
    std::memset(data, 0, PAGE_SIZE);
    header()->pageId = pageId;
    header()->dataSize = 0;
  }

  Page::~Page() noexcept {
    if (data) {
      ::operator delete(data, std::align_val_t{64});
    }
  }

  Page::Page(Page &&other) noexcept : data(other.data) {
    // Reset other page's data.
    other.data = nullptr;
  }

  Page &Page::operator=(Page &&other) noexcept {
    if (this != &other) {
      // Free current memory.
      if (data) {
        ::operator delete(data, std::align_val_t{64});
      }

      // Take ownership of other page's memory.
      data = other.data;
      other.data = nullptr;
    }

    return *this;
  }

  bool Page::write(uint32_t offset, const void *data, uint32_t length) noexcept {
    // Check if write would exceed page size.
    if (offset + length > MAX_DATA_SIZE) {
      return false;
    }

    // Write the data.
    std::memcpy(getData() + offset, data, length);

    // Update data size if necessary.
    uint32_t newSize = offset + length;
    if (newSize > header()->dataSize) {
      header()->dataSize = newSize;
    }

    return true;
  }

  uint32_t Page::read(uint32_t offset, void *dest, uint32_t length) const noexcept {
    // Don't read past the current data size.
    length = std::min(length, header()->dataSize - std::min(offset, header()->dataSize));

    // Read the data if there is any.
    if (length > 0) {
      std::memcpy(dest, getData() + offset, length);
    }

    return length;
  }
} // namespace pulse::storage
