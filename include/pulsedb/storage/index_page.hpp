/**
 * @file include/pulsedb/storage/index_page.hpp
 * @brief The IndexPage class used in the storage system. This class represents a B+ tree node for
 * indexing.
 *
 * Index Page Layout (B+ tree node):
 * +---------------------------------+ 0x0000
 * | IndexHeader (28 bytes)          |
 * |   [Base PageHeader]             | -- First 13 bytes (0x0D).
 * |   isLeaf:     bool              | -- Leaf node indicator.
 * |   nextPageId: uint32_t          | -- Next sibling page.
 * |   prevPageId: uint32_t          | -- Previous sibling page.
 * |   parentId:   uint32_t          | -- Parent node page.
 * |   level:      uint16_t          | -- Tree level (0 for leaf).
 * +---------------------------------+ 0x001C
 * | IndexEntry Array                | -- Sorted key-pageId pairs.
 * |   [Variable number of entries:] |
 * |   struct IndexEntry {           |
 * |     key:    uint64_t            | -- Index key.
 * |     pageId: uint32_t            | -- Child page ID.
 * |     offset: uint16_t            | -- Variable-length data offset.
 * |   }                             |
 * +---------------------------------+ 0x1000
 */

#ifndef PULSEDB_STORAGE_INDEX_PAGE_HPP
#define PULSEDB_STORAGE_INDEX_PAGE_HPP

#include "pulsedb/storage/page.hpp"
#include <optional>
#include <vector>

/**
 * @namespace pulse::storage
 * @brief The namespace for the storage system.
 */
namespace pulse::storage {
#pragma pack(push, 1)
  /**
   * @struct IndexHeader
   * @brief Extended header for index pages.
   */
  struct IndexHeader : public PageHeader {
    bool isLeaf;         /**< Whether this is a leaf node */
    uint32_t nextPageId; /**< Next sibling for leaf nodes (0 if none) */
    uint32_t prevPageId; /**< Previous sibling for leaf nodes (0 if none) */
    uint32_t parentId;   /**< Parent node ID (0 if root) */
    uint16_t level;      /**< Level in tree (0 for leaf) */
  };

  /**
   * @struct IndexEntry
   * @brief Represents a key-pageId pair in the entry array.
   */
  struct IndexEntry {
    uint64_t key;    /**< Index key. */
    uint32_t pageId; /**< Child page ID. */
    uint16_t offset; /**< Offset to variable length data if any. */
  };
#pragma pack(pop)
} // namespace pulse::storage

namespace pulse::storage {
  /**
   * @class IndexPage
   * @brief Represents a B+ tree node, used for indexing.
   */
  class IndexPage : public Page {
  public:
    static const uint32_t INDEX_HEADER_SIZE = sizeof(IndexHeader); /**< Size of index header. */
    static const uint32_t MAX_FREE_SPACE =
        PAGE_SIZE - INDEX_HEADER_SIZE; /**< Maximum free space. */

  public:
    /**
     * @brief Construct a new index page with the given ID.
     * @param pageId Page ID.
     * @param isLeaf Whether the page is a leaf node.
     * @param level Level in the tree.
     */
    explicit IndexPage(uint32_t pageId, bool isLeaf, uint16_t level = 0) noexcept;

    /**
     * @brief Looks up the key in the index page.
     * @param key Key to look up.
     * @return Page ID associated with key, nullopt if not found.
     */
    [[nodiscard]] std::optional<uint32_t> lookup(uint64_t key) const noexcept;

    /**
     * @brief Insert new key-pageId pair.
     * @param key Key to insert.
     * @param pageId Page ID to associate with key.
     * @return True if inserted, false if page is full.
     */
    bool insertKey(uint64_t key, uint32_t pageId);

    /**
     * @brief Remove key-pageId pair.
     * @param key Key to remove.
     * @return True if removed, false if not.
     */
    bool removeKey(uint64_t key);

    /**
     * @brief Get range of page IDs between keys.
     * @param startKey Start of range (inclusive).
     * @param endKey End of range (inclusive).
     * @return Vector of page IDs in range.
     */
    [[nodiscard]] std::vector<uint32_t> getRange(uint64_t startKey, uint64_t endKey) const;

    /**
     * @brief Getters for the index page class.
     * @{
     */

    /**
     * @brief Get If node is leaf node
     * @return True if the node is a leaf node, otherwise false.
     */
    [[nodiscard]] bool isLeaf() const noexcept { return indexHeader()->isLeaf; }

    /**
     * @brief Get the next page ID.
     * @return The next page ID.
     */
    [[nodiscard]] uint32_t nextPage() const noexcept { return indexHeader()->nextPageId; }

    /**
     * @brief Get the previous page ID.
     * @return The previous page ID.
     */
    [[nodiscard]] uint32_t prevPage() const noexcept { return indexHeader()->prevPageId; }

    /**
     * @brief Get the parent's page ID.
     * @return The parent's page ID.
     */
    [[nodiscard]] uint32_t parentPage() const noexcept { return indexHeader()->parentId; }

    /**
     * @brief Get the level in the tree.
     * @return The level in tree.
     */
    [[nodiscard]] uint16_t level() const noexcept { return indexHeader()->level; }

    /**
     * @brief Get the maximum entries a node can hold.
     * @return Maximum entry count.
     */
    [[nodiscard]] static constexpr size_t maxEntries() noexcept {
      return MAX_FREE_SPACE / sizeof(IndexEntry);
    }

    /**
     * @brief Get the minimum entries for non-root node.
     * @return Minimum entry count.
     */
    [[nodiscard]] static constexpr size_t minEntries() noexcept { return maxEntries() / 2; }

    /** @} */

    /**
     * @brief Setters for the index page class.
     * @{
     */

    /**
     * @brief Set the next page ID.
     * @param pageId Next page ID.
     */
    void setNextPage(uint32_t pageId) noexcept { indexHeader()->nextPageId = pageId; }

    /**
     * @brief Set the previous page ID.
     * @param pageId Previous page ID.
     */
    void setPrevPage(uint32_t pageId) noexcept { indexHeader()->prevPageId = pageId; }

    /**
     * @brief Set the parent's page ID.
     * @param pageId Parent's page ID.
     */
    void setParentPage(uint32_t pageId) noexcept { indexHeader()->parentId = pageId; }

    /** @} */

    /**
     * @brief Check if the node needs splitting.
     * @return True if overflow, otherwise false.
     */
    [[nodiscard]] bool isOverflow() const noexcept { return itemCount() >= maxEntries(); }

    /**
     * @brief Check if the node is under-utilized.
     * @return True if underflow, otherwise false.
     */
    [[nodiscard]] bool isUnderflow() const noexcept { return itemCount() <= minEntries(); }

    /**
     * @brief Split the node into two.
     * @param newPage New page to split into.
     * @return Median key after split.
     */
    uint64_t split(IndexPage &newPage);

    /**
     * @brief Merge with the right sibling.
     * @param rightSibling The right sibling to merge with.
     * @return True if merged, false if not.
     */
    bool merge(IndexPage &rightSibling);

  private:
    /**
     * @brief Find insertion position for key.
     * @param key Key to insert.
     * @return Iterator to insertion position.
     */
    [[nodiscard]] IndexEntry *findInsertPosition(uint64_t key) noexcept;

    /**
     * @brief Const & non-const getters for index page header.
     * @{
     */

    /**
     * @brief Get pointer to index page header.
     * @return Pointer to index header.
     */
    [[nodiscard]] IndexHeader *indexHeader() noexcept { return header<IndexHeader>(); }

    /**
     * @brief Get const pointer to index page header.
     * @return Const pointer to index header.
     */
    [[nodiscard]] const IndexHeader *indexHeader() const noexcept { return header<IndexHeader>(); }

    /** @} */

    /**
     * @brief Const & non-const getters for entries array.
     * @{
     */

    /**
     * @brief Get pointer to entries array.
     * @return Pointer to entries.
     */
    [[nodiscard]] IndexEntry *entries() noexcept {
      return reinterpret_cast<IndexEntry *>(data + INDEX_HEADER_SIZE);
    }

    /**
     * @brief Get const pointer to entries array.
     * @return Const pointer to entries.
     */
    [[nodiscard]] const IndexEntry *entries() const noexcept {
      return reinterpret_cast<const IndexEntry *>(data + INDEX_HEADER_SIZE);
    }

    /** @} */
  };

} // namespace pulse::storage

#endif // PULSEDB_STORAGE_INDEX_PAGE_HPP
