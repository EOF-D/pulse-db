/**
 * @file src/storage/page.cpp
 * @brief Implements the base Page class for the storage system.
 */

#include "pulsedb/storage/page.hpp"
#include <cstring>

namespace pulse::storage {
  Page::Page(uint32_t pageId, PageType type) noexcept {
    // Allocate memory for the page with 64-byte alignment.
    data = static_cast<uint8_t *>(::operator new(PAGE_SIZE, std::align_val_t{64}));

    // Initialize the header.
    std::memset(data, 0, PAGE_SIZE);
    header()->type = type;
    header()->pageId = pageId;
    header()->lsn = 0;
    header()->freeSpace = MAX_FREE_SPACE;
    header()->itemCount = 0;
  }

  Page::~Page() noexcept {
    // Free the memory if it exists.
    if (data) {
      ::operator delete(data, std::align_val_t{64});
    }
  }

  Page::Page(Page &&other) noexcept : data(other.data) {
    // Reset the other page's data.
    other.data = nullptr;
  }

  Page &Page::operator=(Page &&other) noexcept {
    if (this != &other) {
      // Free current memory if it exists.
      if (data) {
        ::operator delete(data, std::align_val_t{64});
      }

      // Take ownership of the data pointer.
      data = other.data;
      other.data = nullptr;
    }

    return *this;
  }
} // namespace pulse::storage
