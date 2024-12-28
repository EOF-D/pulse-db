/**
 * @file include/pulsedb/storage/page.hpp
 * @brief Page class for storage.
 */

#ifndef PULSEDB_STORAGE_PAGE_HPP
#define PULSEDB_STORAGE_PAGE_HPP

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <new>

namespace pulse::storage {
#pragma pack(push, 1)
  /**
   * @struct PageHeader
   * @brief The header of a page.
   */
  struct PageHeader {
    uint32_t pageId;   /**< The page ID. */
    uint32_t dataSize; /**< The size of the data stored. */
  };
#pragma pack(pop)

  /**
   * @class Page
   * @brief Represents a database page. Manages a fixed-sized block of memory.
   */
  class Page {
  public:
    static const uint32_t PAGE_SIZE = 4096; /**< The default size of a page. */

    static const uint32_t HEADER_SIZE =
        sizeof(PageHeader); /**< The size of the header in a page. */

    static const uint32_t MAX_DATA_SIZE =
        PAGE_SIZE - HEADER_SIZE; /**< The maximum size of the data in a page. */

  public:
    /**
     * @brief Constructs a new page with a given ID.
     * @param pageId The ID of the page.
     */
    explicit Page(uint32_t pageId) noexcept;

    /**
     * @brief Cleans up resources.
     */
    ~Page() noexcept;

    // Disable copy operations to prevent double-frees.
    Page(const Page &) = delete;
    Page &operator=(const Page &) = delete;

    // Allow move operations.
    Page(Page &&other) noexcept;
    Page &operator=(Page &&other) noexcept;

    /**
     * @brief Write data to the page.
     * @param offset The offset to write to.
     * @param data The data to write.
     * @param length The length of the data.
     * @return True if the write was successful, false otherwise.
     */
    bool write(uint32_t offset, const void *data, uint32_t length) noexcept;

    /**
     * @brief Read data from the page.
     * @param offset The offset to read from.
     * @param dest The destination buffer.
     * @param length The length of the data to read.
     * @return The number of bytes read.
     */
    uint32_t read(uint32_t offset, void *dest, uint32_t length) const noexcept;

    /**
     * @brief Get the page ID.
     * @return The page ID.
     */
    [[nodiscard]] uint32_t pageId() const noexcept { return header()->pageId; }

    /**
     * @brief Get the size of the data stored in the page.
     * @return The size of the data.
     */
    [[nodiscard]] uint32_t dataSize() const noexcept { return header()->dataSize; }

    /**
     * @brief Get the page data.
     * @return Pointer to the page data.
     */
    [[nodiscard]] uint8_t *getData() noexcept { return data + HEADER_SIZE; }

    /**
     * @brief Get the page data.
     * @return Const pointer to the page data.
     */
    [[nodiscard]] const uint8_t *getData() const noexcept { return data + HEADER_SIZE; }

  private:
    /**
     * @brief Gets a pointer to the page header.
     * @return Pointer to the page header.
     */
    [[nodiscard]] PageHeader *header() noexcept { return reinterpret_cast<PageHeader *>(data); }

    /**
     * @brief Gets a const pointer to the page header.
     * @return Const pointer to the page header.
     */
    [[nodiscard]] const PageHeader *header() const noexcept {
      return reinterpret_cast<const PageHeader *>(data);
    }

    uint8_t *data; /**< Raw page data including header. */
  };
} // namespace pulse::storage

#endif // PULSEDB_STORAGE_PAGE_HPP
