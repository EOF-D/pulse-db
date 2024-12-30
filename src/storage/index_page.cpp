/**
 * @file src/storage/index_page.cpp
 * @brief Implements the index page class.
 */

#include "pulsedb/storage/index_page.hpp"
#include <algorithm>
#include <cstring>

namespace pulse::storage {
  IndexPage::IndexPage(uint32_t pageId, bool isLeaf, uint16_t level) noexcept
      : Page(pageId, PageType::INDEX) {
    // Initialize the header.
    auto *header = indexHeader();
    header->isLeaf = isLeaf;
    header->level = level;

    header->nextPageId = 0;
    header->prevPageId = 0;
    header->parentId = 0;

    // Update free space to account for extended header size.
    header->freeSpace = MAX_FREE_SPACE;
  }

  std::optional<uint32_t> IndexPage::lookup(uint64_t key) const noexcept {
    // Binary search for key.
    const IndexEntry *begin = entries();
    const IndexEntry *end = begin + itemCount();

    auto it = std::lower_bound(begin, end, key, [](const IndexEntry &entry, uint64_t key) {
      return entry.key < key;
    });

    // If key is found, return pageId.
    if (it != end && it->key == key) {
      return it->pageId;
    }

    // If this is a leaf, key not found.
    if (indexHeader()->isLeaf) {
      return std::nullopt;
    }

    // If internal node, return pageId of child that could contain key.
    if (it == begin) {
      return begin->pageId;
    }

    return (it - 1)->pageId;
  }

  bool IndexPage::insertKey(uint64_t key, uint32_t pageId) {
    // Check if we have space for new entry.
    if (indexHeader()->freeSpace < sizeof(IndexEntry)) {
      return false;
    }

    // Find insert position.
    IndexEntry *pos = findInsertPosition(key);

    // Shift existing entries right.
    if (pos != entries() + itemCount()) {
      std::memmove(pos + 1, pos, (itemCount() - (pos - entries())) * sizeof(IndexEntry));
    }

    // Insert new entry.
    pos->key = key;
    pos->pageId = pageId;
    pos->offset = 0; // No variable length data yet.

    // Update page header.
    indexHeader()->itemCount++;
    indexHeader()->freeSpace -= sizeof(IndexEntry);

    return true;
  }

  bool IndexPage::removeKey(uint64_t key) {
    IndexEntry *begin = entries();
    IndexEntry *end = begin + itemCount();

    auto it = std::lower_bound(begin, end, key, [](const IndexEntry &entry, uint64_t key) {
      return entry.key < key;
    });

    // Key not found.
    if (it == end || it->key != key) {
      return false;
    }

    // Shift entries left.
    std::memmove(it, it + 1, (end - it - 1) * sizeof(IndexEntry));

    // Update page header.
    indexHeader()->itemCount--;
    indexHeader()->freeSpace += sizeof(IndexEntry);

    return true;
  }

  std::vector<uint32_t> IndexPage::getRange(uint64_t startKey, uint64_t endKey) const {
    std::vector<uint32_t> results;

    // Only leaf nodes store actual key-pageId pairs.
    if (!indexHeader()->isLeaf) {
      return results;
    }

    const IndexEntry *begin = entries();
    const IndexEntry *end = begin + itemCount();

    // Find start position.
    auto it = std::lower_bound(begin, end, startKey, [](const IndexEntry &entry, uint64_t key) {
      return entry.key < key;
    });

    // Collect all pageIds in range.
    while (it != end && it->key <= endKey) {
      results.push_back(it->pageId);
      ++it;
    }

    return results;
  }

  uint64_t IndexPage::split(IndexPage &newPage) {
    size_t mid = itemCount() / 2;
    IndexEntry *begin = entries();
    IndexEntry *splitPoint = begin + mid;
    IndexEntry *end = begin + itemCount();

    // Copy upper half to new page.
    size_t numEntries = end - splitPoint;
    std::memcpy(newPage.entries(), splitPoint, numEntries * sizeof(IndexEntry));

    // Update sibling links.
    newPage.setNextPage(nextPage());
    newPage.setPrevPage(id());
    setNextPage(newPage.id());

    if (newPage.nextPage() != 0) {
      // TODO: Implement with disk manager.
    }

    // Update headers.
    newPage.indexHeader()->itemCount = numEntries;
    newPage.indexHeader()->freeSpace -= numEntries * sizeof(IndexEntry);

    indexHeader()->itemCount = mid;
    indexHeader()->freeSpace += numEntries * sizeof(IndexEntry);

    // Return median key.
    return splitPoint->key;
  }

  bool IndexPage::merge(IndexPage &rightSibling) {
    size_t totalEntries = itemCount() + rightSibling.itemCount();

    // Check if entries will fit.
    if (totalEntries > maxEntries()) {
      return false;
    }

    // Copy entries from right sibling.
    std::memcpy(
        entries() + itemCount(),
        rightSibling.entries(),
        rightSibling.itemCount() * sizeof(IndexEntry)
    );

    // Update sibling links.
    this->setNextPage(rightSibling.nextPage());
    if (rightSibling.nextPage() != 0) {
      // TODO: Implement with disk manager.
    }

    // Update header.
    indexHeader()->itemCount = totalEntries;
    indexHeader()->freeSpace -= rightSibling.itemCount() * sizeof(IndexEntry);

    return true;
  }

  IndexEntry *IndexPage::findInsertPosition(uint64_t key) noexcept {
    IndexEntry *begin = entries();
    IndexEntry *end = begin + itemCount();

    return std::lower_bound(begin, end, key, [](const IndexEntry &entry, uint64_t key) {
      return entry.key < key;
    });
  }
} // namespace pulse::storage
