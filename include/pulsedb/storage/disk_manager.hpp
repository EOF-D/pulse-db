/**
 * @file include/pulsedb/storage/disk_manager.hpp
 * @brief The DiskManager class used in the storage system. This class manages physical page I/O.
 */

#ifndef PULSEDB_STORAGE_DISK_MANAGER_HPP
#define PULSEDB_STORAGE_DISK_MANAGER_HPP

#include "pulsedb/storage/page.hpp"
#include "pulsedb/utils/logger.hpp"
#include <filesystem>

/**
 * @namespace pulse::storage
 * @brief The namespace for the storage system.
 */
namespace pulse::storage {
#pragma pack(push, 1)
  /**
   * @struct DatabaseHeader
   * @brief Header information for the database file.
   */
  struct DatabaseHeader {
    uint32_t magic;         /**< Magic number to identify database file. */
    uint32_t version;       /**< Database format version. */
    uint32_t pageSize;      /**< Size of each page. */
    uint32_t pageCount;     /**< Total number of pages. */
    uint32_t firstFreePage; /**< First free page ID. */
    uint64_t lastLsn;       /**< Last log sequence number. */
  };
#pragma pack(pop)

  /**
   * @class DiskManager
   * @brief Manages physical page I/O and database file operations.
   */
  class DiskManager {
  public:
    static const uint32_t DB_MAGIC = 0x504442; /**< "PDB" magic number for database files. */
    static const uint32_t DB_VERSION = 1;      /**< Current database version. */
    static const uint32_t INVALID_PAGE_ID = 0xDEADBEEF; /**< Invalid page ID. */

    /**
     * @brief Constructs a new disk manager using the given file path.
     * @param path Path to database file.
     * @param create Whether to create a new database or use an existing one.
     */
    explicit DiskManager(const std::filesystem::path &path, bool create = false);

    /**
     * @brief Closes database and syncs pending writes.
     */
    ~DiskManager() noexcept;

    // Disable copy operations to prevent double-frees.
    DiskManager(const DiskManager &) = delete;
    DiskManager &operator=(const DiskManager &) = delete;

    // Allow move operations.
    DiskManager(DiskManager &&) noexcept;
    DiskManager &operator=(DiskManager &&) noexcept;

    /**
     * @brief Allocates a new page. If there are free pages, one is popped off the stack.
     * @return New page ID or INVALID_PAGE_ID if allocation fails.
     */
    [[nodiscard]] uint32_t allocatePage();

    /**
     * @brief Deallocates a page.
     * @param pageId ID of page to deallocate.
     * @return True if successful, false if failed.
     */
    bool deallocatePage(uint32_t pageId);

    /**
     * @brief Reads a page from the disk.
     * @param pageId ID of page to read.
     * @return Unique pointer to page or nullptr if read fails.
     */
    [[nodiscard]] std::unique_ptr<Page> fetchPage(uint32_t pageId);

    /**
     * @brief Forces all pending writes to disk.
     * @return True if successful, false if sync fails.
     */
    bool sync();

    /**
     * @brief Writes a page to the disk.
     * @param page Page to write.
     * @return True if successful, false if write fails.
     */
    bool flushPage(const Page &page);

    /**
     * @brief Getters for the disk manager class.
     * @{
     */

    /**
     * @brief Gets the total number of pages.
     * @return Total page count.
     */
    [[nodiscard]] uint32_t pageCount() const noexcept { return header.pageCount; }

    /**
     * @brief Gets the current database file size.
     * @return File size in bytes.
     */
    [[nodiscard]] uint64_t fileSize() const noexcept;

    /** @} */

  private:
    /**
     * @brief Reads the database header.
     * @throws std::runtime_error if header is invalid.
     */
    void readHeader();

    /**
     * @brief Writes the database header.
     * @return True if successful, false if write fails.
     */
    bool writeHeader();

    /**
     * @brief Initializes a new database file.
     * @throws std::runtime_error if initialization fails.
     */
    void initializeDatabase();

    /**
     * @brief Calculates the file offset for a page.
     * @param pageId Page ID to calculate offset for.
     * @return Offset in bytes from start of file.
     */
    [[nodiscard]] uint64_t getOffset(uint32_t pageId) const noexcept {
      return sizeof(DatabaseHeader) + static_cast<uint64_t>(pageId) * Page::PAGE_SIZE;
    }

    bool dirty;                      /**< Whether header needs to be written. */
    DatabaseHeader header;           /**< In-memory copy of database header. */
    std::filesystem::path path;      /**< Path to database file. */
    std::vector<uint32_t> freePages; /**< Stack of free page IDs. */
    uint32_t nextPageId;             /**< Next page ID to allocate. */
    utils::Logger logger;            /**< Logger instance. */
  };
} // namespace pulse::storage

#endif // PULSEDB_STORAGE_DISK_MANAGER_HPP
