/**
 * @file include/pulsedb/storage/page.hpp
 * @brief The base Page class used in the storage system.
 *
 * All pages are fixed to 4096 bytes and are 64-byte aligned. Each page type builds upon
 * the base PageHeader structure with additional fields specific to its purpose.
 *
 * Base Page Layout (4096 bytes total):
 * +---------------------------------+ 0x0000
 * | PageHeader (13 bytes)           |
 * |   type:      uint8_t            | -- Page type identifier.
 * |   pageId:    uint32_t           | -- Unique page identifier.
 * |   lsn:       uint32_t           | -- Log sequence number.
 * |   freeSpace: uint16_t           | -- Available free space.
 * |   itemCount: uint16_t           | -- Number of items in page.
 * +---------------------------------+ 0x000D
 * | Page Specific Data (4083 bytes) | -- Type-specific content.
 * +---------------------------------+ 0x1000
 */

#ifndef PULSEDB_STORAGE_PAGE_HPP
#define PULSEDB_STORAGE_PAGE_HPP

#include <cstdint>
#include <memory>

/**
 * @namespace pulse::storage
 * @brief The namespace for the storage system.
 */
namespace pulse::storage {
  /**
   * @enum PageType
   * @brief Represents the type of a page.
   */
  enum class PageType : uint8_t {
    INVALID = 0, /**< Invalid page. */
    INDEX = 1,   /**< Index page. */
    DATA = 2,    /**< Data page. */
    SPECIAL = 3  /**< Special page. */
  };

// Packing struct so that there is no padding.
#pragma pack(push, 1)
  /**
   * @struct PageHeader
   * @brief Holds information about a page.
   */
  struct PageHeader {
    PageType type;      /**< The type of the page. */
    uint32_t pageId;    /**< The id of the page. */
    uint32_t lsn;       /**< The log sequence number of the page. */
    uint16_t freeSpace; /**< The free space in the page. */
    uint16_t itemCount; /**< The number of items in the page. */
  };
#pragma pack(pop)
} // namespace pulse::storage

namespace pulse::storage {
  /**
   * @class Page
   * @brief The base class for all pages.
   */
  class Page {
    friend class DiskManager; // Allow the disk manager to access the page.

  public:
    static const uint32_t PAGE_SIZE = 4096;                 /**< The size of a page. */
    static const uint32_t HEADER_SIZE = sizeof(PageHeader); /**< The size of the header. */
    static const uint32_t MAX_FREE_SPACE =
        PAGE_SIZE - HEADER_SIZE; /**< The maximum free space in a page. */

  public:
    /**
     * @brief Construct a new page with the given ID.
     * @param pageId the ID of the page.
     * @param type the type of the page.
     */
    explicit Page(uint32_t pageId, PageType type) noexcept;

    /**
     * @brief Cleans up resources.
     */
    virtual ~Page() noexcept;

    // Disable copy operations to prevent double-frees.
    Page(const Page &) = delete;
    Page &operator=(const Page &) = delete;

    // Allow move operations.
    Page(Page &&) noexcept;
    Page &operator=(Page &&) noexcept;

    /**
     * @brief Check if the page has enough free space.
     * @param size the size to check for.
     * @return True if there is enough space, otherwise false.
     */
    [[nodiscard]] bool hasSpace(uint32_t size) const noexcept { return freeSpace() >= size; }

    /**
     * @brief Getters for the page class.
     * @{
     */

    /**
     * @brief Get the type of the page.
     * @return The type of the page.
     */
    [[nodiscard]] PageType type() const noexcept { return header()->type; }

    /**
     * @brief Get the ID of the page.
     * @return The ID of the page.
     */
    [[nodiscard]] uint32_t id() const noexcept { return header()->pageId; }

    /**
     * @brief Get the log sequence number of the page.
     * @return The log sequence number of the page.
     */
    [[nodiscard]] uint32_t lsn() const noexcept { return header()->lsn; }

    /**
     * @brief Get the free space in the page.
     * @return The free space in the page.
     */
    [[nodiscard]] uint16_t freeSpace() const noexcept { return header()->freeSpace; }

    /**
     * @brief Get the number of items in the page.
     * @return The number of items in the page.
     */
    [[nodiscard]] uint16_t itemCount() const noexcept { return header()->itemCount; }

    /** @} */

  protected:
    /**
     * @brief Const & non-const getters for page header.
     * @{
     */

    /**
     * @brief Gets pointer to page header.
     * @tparam H Header type (defaults to PageHeader)
     * @return Pointer to header.
     */
    template <typename H = PageHeader> [[nodiscard]] H *header() noexcept {
      static_assert(std::is_base_of_v<PageHeader, H>, "Header type must inherit from PageHeader");
      return reinterpret_cast<H *>(data);
    }

    /**
     * @brief Gets const pointer to page header.
     * @tparam H Header type (defaults to PageHeader)
     * @return Const pointer to header.
     */
    template <typename H = PageHeader> [[nodiscard]] const H *header() const noexcept {
      static_assert(std::is_base_of_v<PageHeader, H>, "Header type must inherit from PageHeader");
      return reinterpret_cast<const H *>(data);
    }

    /** @} */

    /**
     * @brief Const & non-const getters for page data.
     * @{
     */

    /**
     * @brief Gets pointer to data area after header.
     * @return Pointer to data area.
     */
    [[nodiscard]] uint8_t *getData() noexcept { return data + HEADER_SIZE; }

    /**
     * @brief Gets const pointer to data area.
     * @return Const pointer to data area.
     */
    [[nodiscard]] const uint8_t *getData() const noexcept { return data + HEADER_SIZE; }

    /** @} */

    uint8_t *data; /**< The data of the page. */
  };
} // namespace pulse::storage

#endif // PULSEDB_STORAGE_PAGE_HPP
