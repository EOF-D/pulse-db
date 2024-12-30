/**
 * @file src/storage/disk_manager.cpp
 * @brief Implements the disk manager class.
 */

#include "pulsedb/storage/disk_manager.hpp"
#include "pulsedb/storage/data_page.hpp"
#include "pulsedb/storage/index_page.hpp"
#include <cstring>
#include <fstream>

namespace fs = std::filesystem;

namespace pulse::storage {
  DiskManager::DiskManager(const fs::path &path, bool create)
      : dirty(false), path(path), nextPageId(0), logger("disk-manager") {
    if (!create && !std::filesystem::exists(path)) {
      throw std::runtime_error("Database file does not exist.");
    }

    // Initialize or read existing database.
    if (create) {
      initializeDatabase();
    }

    else {
      readHeader();
    }
  }

  DiskManager::~DiskManager() noexcept {
    try {
      if (dirty) {
        writeHeader();
        sync();
      }
    } catch (const std::exception &e) {
      logger.error("error during destruction: {}", e.what());
    }
  }

  DiskManager::DiskManager(DiskManager &&other) noexcept
      : dirty(other.dirty), header(other.header), path(std::move(other.path)),
        freePages(std::move(other.freePages)), nextPageId(other.nextPageId),
        logger(std::move(other.logger)) {
    other.dirty = false;
    other.nextPageId = 0;
  }

  DiskManager &DiskManager::operator=(DiskManager &&other) noexcept {
    if (this != &other) {
      if (dirty) {
        try {
          writeHeader();
        }

        catch (const std::exception &e) {
          logger.error("error writing header during move: {}", e.what());
        }
      }

      // Move resources.
      dirty = other.dirty;
      header = other.header;
      path = std::move(other.path);
      freePages = std::move(other.freePages);
      nextPageId = other.nextPageId;

      other.dirty = false;
      other.nextPageId = 0;
    }

    return *this;
  }

  uint32_t DiskManager::allocatePage() {
    uint32_t pageId;

    if (!freePages.empty()) {
      pageId = freePages.back();
      freePages.pop_back();
      logger.info("allocated page {} from the free list", pageId);
    }

    else {
      pageId = header.pageCount++;
      logger.info("allocated new page {}", pageId);
    }

    dirty = true;
    return pageId;
  }

  bool DiskManager::deallocatePage(uint32_t pageId) {
    // Check if the page ID is valid.
    if (pageId >= header.pageCount) {
      logger.error("cant deallocate invalid page ID: {}", pageId);
      return false;
    }

    logger.info("deallocating page: {}", pageId);
    freePages.push_back(pageId);

    dirty = true;
    return true;
  }

  std::unique_ptr<Page> DiskManager::fetchPage(uint32_t pageId) {
    if (pageId >= header.pageCount) {
      logger.error("invalid page ID: {}", pageId);
      return nullptr;
    }

    logger.info("fetching page: {}", pageId);

    auto offset = getOffset(pageId);
    std::vector<uint8_t> buffer(Page::PAGE_SIZE);

    std::fstream file(path, std::ios::binary | std::ios::in);
    if (!file) {
      logger.error("failed to open database file");
      return nullptr;
    }

    file.seekg(offset);
    file.read(reinterpret_cast<char *>(buffer.data()), Page::PAGE_SIZE);

    if (!file || file.gcount() != Page::PAGE_SIZE) {
      logger.error("failed to read page: {}", pageId);
      return nullptr;
    }

    // Get page type from header.
    auto type = static_cast<PageType>(buffer[0]);
    std::unique_ptr<Page> page;

    switch (type) {
      case PageType::DATA: {
        page = std::make_unique<DataPage>(pageId);
        break;
      }

      case PageType::INDEX: {
        const auto *indexHeader = reinterpret_cast<const IndexHeader *>(buffer.data());
        page = std::make_unique<IndexPage>(pageId, indexHeader->isLeaf, indexHeader->level);
        break;
      }

      default:
        logger.error("invalid page type: {}", static_cast<int>(type));
        return nullptr;
    }

    logger.info("fetched page: {}, type: {}", pageId, static_cast<int>(type));

    // Copy the header and data portions.
    std::memcpy(page->getData() - Page::HEADER_SIZE, buffer.data(), Page::HEADER_SIZE);
    std::memcpy(page->getData(), buffer.data() + Page::HEADER_SIZE, Page::MAX_FREE_SPACE);

    return page;
  }

  bool DiskManager::sync() {
    logger.info("syncing database");

    if (dirty && !writeHeader()) {
      return false;
    }

    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
      logger.error("failed to open database file");
      return false;
    }

    file.flush();
    dirty = false;

    return true;
  }

  bool DiskManager::flushPage(const Page &page) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
      logger.error("failed to open database file: {}", path.string());
      return false;
    }

    uint64_t offset = getOffset(page.id());
    file.seekp(offset);
    if (!file) {
      logger.error("failed to seek to page {}", page.id());
      return false;
    }

    file.write(
        reinterpret_cast<const char *>(page.getData() - Page::HEADER_SIZE), Page::PAGE_SIZE
    );

    if (!file) {
      logger.error("failed to write page {}", page.id());
      return false;
    }

    file.flush();
    logger.info("flushed page {}", page.id());
    return true;
  }

  uint64_t DiskManager::fileSize() const noexcept {
    std::error_code error_code;
    auto size = std::filesystem::file_size(path, error_code);

    if (error_code) {
      logger.error("failed to get file size: {}", error_code.message());
      return 0;
    }

    return size;
  }

  void DiskManager::readHeader() {
    std::fstream file(path, std::ios::binary | std::ios::in);
    if (!file) {
      logger.error("failed to open database file");
      throw std::runtime_error("Failed to open database file.");
    }

    file.read(reinterpret_cast<char *>(&header), sizeof(DatabaseHeader));
    if (!file || file.gcount() != sizeof(DatabaseHeader)) {
      logger.error("failed to read header");
      throw std::runtime_error("Failed to read header.");
    }

    if (header.magic != DB_MAGIC) {
      logger.error("invalid magic number");
      throw std::runtime_error("Invalid magic number.");
    }

    if (header.version != DB_VERSION) {
      logger.error("unsupported version: {}", header.version);
      throw std::runtime_error("Unsupported version.");
    }

    if (header.pageSize != Page::PAGE_SIZE) {
      logger.error("invalid page size: expected {}, got {}", Page::PAGE_SIZE, header.pageSize);
      throw std::runtime_error("Invalid page size.");
    }

    logger.info("header read successfully");
  }

  bool DiskManager::writeHeader() {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
      logger.error("failed to open database file");
      return false;
    }

    file.seekp(0);
    file.write(reinterpret_cast<const char *>(&header), sizeof(DatabaseHeader));

    if (!file) {
      logger.error("failed to write header");
      return false;
    }

    file.flush();
    return true;
  }

  void DiskManager::initializeDatabase() {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Failed to create database file.");
    }

    // Initialize header
    header.magic = DB_MAGIC;
    header.version = DB_VERSION;
    header.pageSize = Page::PAGE_SIZE;
    header.pageCount = 0;
    header.firstFreePage = INVALID_PAGE_ID;
    header.lastLsn = 0;

    if (!writeHeader()) {
      throw std::runtime_error("Failed to write initial database header.");
    }

    logger.info("initialized new database at {}", path.string());
  }
} // namespace pulse::storage
