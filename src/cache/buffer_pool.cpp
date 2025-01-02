/**
 * @file src/cache/buffer_pool.cpp
 * @brief Implements the buffer pool class.
 */

#include "pulsedb/cache/buffer_pool.hpp"
#include "pulsedb/storage/data_page.hpp"
#include "pulsedb/storage/index_page.hpp"

namespace pulse::cache {
  BufferPool::BufferPool(storage::DiskManager &diskManager, size_t poolSize) noexcept
      : frames(poolSize), diskManager(diskManager), logger("buffer-pool") {
    logger.info("initialized buffer pool with {} frames", poolSize);
  }

  BufferPool::~BufferPool() noexcept {
    try {
      flushAll();
    } catch (const std::exception &e) {
      logger.error("error during destruction: {}", e.what());
    }
  }

  storage::Page *BufferPool::fetchPage(uint32_t pageId) {
    std::lock_guard lock(mutex);

    // Check if page is already in pool.
    auto it = pageTable.find(pageId);
    if (it != pageTable.end()) {
      Frame &frame = frames[it->second];
      frame.pin();
      replacer.pin(it->second);

      logger.debug("hit on page {} in frame {}", pageId, it->second);
      return frame.getPage();
    }

    // Find a frame for the new page.
    auto victimId = findVictim();
    if (!victimId) {
      logger.error("no frames available for page {}", pageId);
      return nullptr;
    }

    // Load page from disk.
    auto page = diskManager.fetchPage(pageId);
    if (!page) {
      logger.error("failed to fetch page {} from disk", pageId);
      return nullptr;
    }

    // If the victim frame is occupied, evict it.
    if (!evictFrame(*victimId)) {
      logger.error("failed to evict frame {} for page {}", *victimId, pageId);
      return nullptr;
    }

    // Insert page into frame.
    Frame &frame = frames[*victimId];
    frame.reset(std::move(page));
    frame.pin();

    pageTable[pageId] = *victimId;
    replacer.pin(*victimId);

    logger.info("loaded page {} into frame {}", pageId, *victimId);
    return frame.getPage();
  }

  storage::Page *BufferPool::createPage(storage::PageType type, bool isLeaf, uint16_t level) {
    std::lock_guard lock(mutex);

    // Allocate a new page ID from the disk manager.
    uint32_t newPageId = diskManager.allocatePage();
    if (newPageId == storage::DiskManager::INVALID_PAGE_ID) {
      logger.error("failed to allocate new page");
      return nullptr;
    }

    // Find a frame for the new page.
    auto victimId = findVictim();
    if (!victimId) {
      logger.error("no frames available for new page");
      return nullptr;
    }

    // If the victim frame is occupied, evict it.
    if (!evictFrame(*victimId)) {
      logger.error("failed to evict frame {} for new page", *victimId);
      return nullptr;
    }

    // Create the appropriate page type.
    std::unique_ptr<storage::Page> page;
    switch (type) {
      case storage::PageType::INDEX:
        page = std::make_unique<storage::IndexPage>(newPageId, isLeaf, level);
        break;

      case storage::PageType::DATA:
        page = std::make_unique<storage::DataPage>(newPageId);
        break;

      default:
        logger.error("invalid page type: {}", static_cast<int>(type));
        return nullptr;
    }

    // Insert page into frame.
    Frame &frame = frames[*victimId];
    frame.reset(std::move(page));
    frame.pin();
    frame.mark(); // New pages are dirty.

    pageTable[newPageId] = *victimId;
    replacer.pin(*victimId);
    logger.info(
        "created new {} page {} in frame {}", static_cast<int>(type), newPageId, *victimId
    );

    return frame.getPage();
  }

  bool BufferPool::deletePage(uint32_t pageId) {
    std::lock_guard lock(mutex);

    // Check if page is in pool.
    auto it = pageTable.find(pageId);
    if (it != pageTable.end()) {
      Frame &frame = frames[it->second];

      // Cannot delete a pinned page.
      if (!frame.isUnpinned()) {
        logger.error("cannot delete pinned page {}", pageId);
        return false;
      }

      // Reset the frame.
      frame.reset(nullptr);
      pageTable.erase(it);
      replacer.pin(it->second);
    }

    // Delete from disk.
    if (!diskManager.deallocatePage(pageId)) {
      logger.error("failed to deallocate page {} from the disk", pageId);
      return false;
    }

    logger.info("deleted page {}", pageId);
    return true;
  }

  bool BufferPool::unpinPage(uint32_t pageId, bool isDirty) {
    std::lock_guard lock(mutex);

    // Find page in pool.
    auto it = pageTable.find(pageId);
    if (it == pageTable.end()) {
      logger.error("cannot unpin page {}, not found", pageId);
      return false;
    }

    Frame &frame = frames[it->second];
    frame.unpin();
    if (isDirty) {
      frame.mark();
    }

    // If completely unpinned, make available for replacement.
    if (frame.isUnpinned()) {
      replacer.unpin(it->second);
    }

    logger.debug("unpinned page {} (dirty: {})", pageId, isDirty);
    return true;
  }

  bool BufferPool::flushPage(uint32_t pageId) {
    std::lock_guard lock(mutex);

    // Find page in pool.
    auto it = pageTable.find(pageId);
    if (it == pageTable.end()) {
      logger.error("cannot flush page {}, not found", pageId);
      return false;
    }

    Frame &frame = frames[it->second];

    // Only flush if dirty.
    if (frame.isDirty()) {
      if (!diskManager.flushPage(*frame.getPage())) {
        logger.error("failed to flush page {} to the disk", pageId);
        return false;
      }

      frame.unmark();
    }

    logger.debug("flushed page {}", pageId);
    return true;
  }

  void BufferPool::flushAll() {
    std::lock_guard lock(mutex);

    for (const auto &entry : pageTable) {
      Frame &frame = frames[entry.second];
      if (frame.isDirty()) {
        if (!diskManager.flushPage(*frame.getPage())) {
          logger.error("failed to flush page {} to the disk", entry.first);
          continue;
        }

        frame.unmark();
      }
    }

    logger.info("flushed all pages");
  }

  std::optional<size_t> BufferPool::findVictim() {
    // First try to find an unused frame.
    for (size_t i = 0; i < frames.size(); i++) {
      if (frames[i].getPage() == nullptr) {
        return i;
      }
    }

    // Otherwise use the replacement policy.
    return replacer.victim();
  }

  bool BufferPool::evictFrame(size_t frameId) {
    Frame &frame = frames[frameId];
    if (frame.getPage() == nullptr) {
      return true;
    }

    if (!frame.isUnpinned()) {
      return false;
    }

    if (frame.isDirty()) {
      if (!diskManager.flushPage(*frame.getPage())) {
        return false;
      }
    }

    pageTable.erase(frame.id());
    frame.reset(nullptr);

    return true;
  }
} // namespace pulse::cache
