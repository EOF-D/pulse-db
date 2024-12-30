/**
 * @file include/pulsedb/storage/data_page.hpp
 * @brief The DataPage class used in the storage system.
 *
 * Data Page Layout (Slotted page):
 * +---------------------------------+ 0x0000
 * | DataHeader (23 bytes)           |
 * |   [Base PageHeader]             | -- First 13 bytes.
 * |   freeSpaceOffset: uint16_t     | -- Start of free space.
 * |   firstSlotOffset: uint16_t     | -- First slot location.
 * |   firstFreeSlot:   uint16_t     | -- First deleted slot.
 * |   slotCount:       uint16_t     | -- Total number of slots.
 * |   directoryCount:  uint16_t     | -- Number of dir entries.
 * +---------------------------------+ 0x0017
 * | SlotPair Directory              | -- Maps keys to slots.
 * |   [Variable number of pairs:]   |
 * |   struct SlotPair {             |
 * |     key:    uint32_t            | -- Record key.
 * |     slotId: uint16_t            | -- Slot index.
 * |   }                             |
 * +---------------------------------+ VARIES ->
 * | SlotEntry Array                 | -- Maps slots to records.
 * |   [Variable number of entries:] |
 * |   struct SlotEntry {            |
 * |     offset: uint16_t            | -- Record offset.
 * |     length: uint16_t            | -- Record length.
 * |     flags:  uint16_t            | -- Status flags.
 * |   }                             |
 * +---------------------------------+ <- VARIES
 * | Variable Length Records         | -- Actual record data.
 * |   [Each record:]                |
 * |   struct RecordHeader {         |
 * |     length: uint16_t            | -- Data length.
 * |     type:   uint16_t            | -- Record type.
 * |   }                             |
 * |   [Record Data]                 | -- Variable length.
 * +---------------------------------+ 0x1000
 */

#ifndef PULSEDB_STORAGE_DATA_PAGE_HPP
#define PULSEDB_STORAGE_DATA_PAGE_HPP

#include "pulsedb/storage/page.hpp"
#include <optional>

/**
 * @namespace pulse::storage
 * @brief The namespace for the storage system.
 */
namespace pulse::storage {
#pragma pack(push, 1)
  /**
   * @struct DataHeader
   * @brief Extended header for data pages.
   */
  struct DataHeader : public PageHeader {
    uint16_t freeSpaceOffset; /**< Offset to start of free space. */
    uint16_t firstSlotOffset; /**< Offset to start of free slot directory. */
    uint16_t firstFreeSlot;   /**< Index of first free slot. */
    uint16_t slotCount;       /**< Total number of slots. */
    uint16_t directoryCount;  /**< Total number of directory entries. */
  };

  /**
   * @struct SlotPair
   * @brief Key to slot ID pair for slot directory.
   */
  struct SlotPair {
    uint32_t key;    /**< Key for slot directory. */
    uint16_t slotId; /**< Slot ID for directory. */
  };

  /**
   * @struct SlotEntry
   * @brief Directory slot pointing to record location.
   */
  struct SlotEntry {
    uint16_t offset; /**< Offset to record data. */
    uint16_t length; /**< Length of record data. */
    uint16_t flags;  /**< Record flags (i.e. deleted). */
  };

  /**
   * @struct RecordHeader
   * @brief Header for variable length records.
   */
  struct RecordHeader {
    uint16_t length; /**< Length of record data. */
    uint16_t type;   /**< Record type identifier. */
  };
#pragma pack(pop)
} // namespace pulse::storage

namespace pulse::storage {
  /**
   * @class SlotFlags
   * @brief Helper class for managing slot flags.
   */
  class SlotFlags {
  public:
    static constexpr uint16_t NONE = 0x0000;    /**< No flag set. */
    static constexpr uint16_t DELETED = 0x0001; /**< Record is deleted. */

    // TODO: Add more flags as needed.

    /**
     * @brief Check if flag is set.
     * @param flags Flags to check from.
     * @param flag Flag to check.
     * @return True if set, false if not.
     */
    [[nodiscard]] static bool isSet(uint16_t flags, uint16_t flag) noexcept {
      return (flags & flag) == flag;
    }

    /**
     * @brief Set a flag.
     * @param flags Flags to modify from.
     * @param flag Flag to set.
     * @return New flags with the flag set.
     */
    static uint16_t set(uint16_t flags, uint16_t flag) noexcept { return flags | flag; }

    /**
     * @brief Clear a flag.
     * @param flags Flags to modify from.
     * @param flag Flag to clear.
     * @return New flags with the flag cleared.
     */
    static uint16_t clear(uint16_t flags, uint16_t flag) noexcept { return flags & ~flag; }

    /**
     * @brief Toggle a  flag.
     * @param flags Flags to modify from.
     * @param flag Flag to toggle.
     * @return New flags with the flag toggled.
     */
    static uint16_t toggle(uint16_t flags, uint16_t flag) noexcept { return flags ^ flag; }
  };

  /**
   * @class DataPage
   * @brief Represents a data page in the storage system. Used for storing records.
   */
  class DataPage : public Page {
  public:
    static const uint32_t DATA_HEADER_SIZE = sizeof(DataHeader);     /**< Size of data header. */
    static const uint32_t RECORD_HEADER_SIZE = sizeof(RecordHeader); /**< Size of record header. */
    static const uint32_t MAX_FREE_SPACE =
        PAGE_SIZE - DATA_HEADER_SIZE; /**< Maximum free space. */

    static const uint32_t SLOT_SIZE = sizeof(SlotEntry); /**< Size of slot entry. */
    static const uint32_t PAIR_SIZE = sizeof(SlotPair);  /**< Size of slot directory pair. */
    static const uint16_t INVALID_SLOT = 0xFFFF;         /**< Invalid slot index. */

  public:
    /**
     * @brief Constructs a new data page with the given ID.
     * @param pageId The ID of the page.
     */
    explicit DataPage(uint32_t pageId) noexcept;

    /**
     * @brief Insert a record into the page.
     * @param key The key paired with the record.
     * @param data The record data.
     * @param length The length of the record data.
     * @param type The record type.
     */
    [[nodiscard]] std::optional<uint16_t>
    insertRecord(uint32_t key, const void *data, uint16_t length, uint16_t type);

    /**
     * @brief Delete the record at the given slot ID.
     * @param slotId The slot of the ID to delete.
     * @return True if deleted, otherwise false.
     */
    bool deleteRecord(uint16_t slotId);

    /**
     * @brief Get the record at the given slot ID.
     * @param slotId The slot ID to get the record from.
     * @return A pairing of the record data and it's length, if not found nullopt.
     */
    std::optional<std::pair<const void *, uint16_t>> getRecord(uint16_t slotId) const;

    /**
     * @brief Get record type at the given slot ID.
     * @param slotId Slot ID to get the record type from.
     * @return Record type if found, nullopt if not.
     */
    std::optional<uint16_t> getRecordType(uint16_t slotId) const;

    /**
     * @brief Check if the slot at the given ID has a flag.
     * @param slotId Slot ID to check.
     * @param flag Flag to check.
     * @return True if flag is set, otherwise false.
     */
    bool hasFlag(uint16_t slotId, uint16_t flag) const noexcept;

    /**
     * @brief Set a flag for the slot at the given ID.
     * @param slotId Slot ID to set the flag for.
     * @param flag Flag to set.
     * @return True if set, false if invalid.
     */
    bool setFlag(uint16_t slotId, uint16_t flag) noexcept;

    /**
     * @brief Clear a flag for the slot at the given ID.
     * @param slotId Slot ID to clear the flag for.
     * @param flag Flag to clear.
     * @return True if cleared, false if invalid.
     */
    bool clearFlag(uint16_t slotId, uint16_t flag) noexcept;

    /**
     * @brief Compact the page by removing deleted records.
     * @return Number of bytes freed.
     */
    uint16_t compact();

    /**
     * @brief Check if compaction needed.
     * @return True if fragmentation exceeds threshold.
     */
    [[nodiscard]] bool needsCompact() const noexcept;

    /**
     * @brief Get space needed for record.
     * @param length Record data length.
     * @return Total space needed including headers.
     */
    [[nodiscard]] static uint16_t spaceNeeded(uint16_t length) noexcept {
      return SLOT_SIZE + RECORD_HEADER_SIZE + length;
    }

    /**
     * @brief Find a free slot for new record.
     * @return Slot ID if found, nullopt if full.
     */
    [[nodiscard]] std::optional<uint16_t> findFreeSlot() noexcept;

    /**
     * @brief Get slot ID from key.
     * @param key Key to search for.
     * @return Slot ID if found, nullopt if not.
     */
    [[nodiscard]] std::optional<uint16_t> getSlotId(uint32_t key) const;

    /**
     * @brief Insert key to slot ID pair.
     * @param key Key to insert.
     * @param slotId Slot ID to insert.
     * @return True if inserted, false if full.
     */
    bool insertPair(uint32_t key, uint16_t slotId);

    /**
     * @brief Remove the last key to slot pair.
     */
    void removeLastPair() {
      if (dataHeader()->directoryCount > 0) {
        dataHeader()->directoryCount--;
      }
    }

    /**
     * @brief Getters for the data page class.
     * @{
     */

    /**
     * @brief Get the amount of slots in the page.
     * @return The amount of slots.
     */
    [[nodiscard]] uint16_t slotCount() const noexcept { return dataHeader()->slotCount; }

    /**
     * @brief Get directory count.
     * @return The number of directory entries.
     */
    [[nodiscard]] uint16_t directoryCount() const noexcept { return dataHeader()->directoryCount; }

    /** @} */

  private:
    /**
     * @brief Allocate space for record.
     * @param size Bytes needed.
     * @return Offset if allocated, nullopt if no full.
     */
    [[nodiscard]] std::optional<uint16_t> allocateSpace(uint16_t size) noexcept;

    /**
     * @brief Const & non-const getters for data page header.
     * @{
     */

    /**
     * @brief Get pointer to data page header.
     * @return Pointer to data header.
     */
    [[nodiscard]] DataHeader *dataHeader() noexcept { return header<DataHeader>(); }

    /**
     * @brief Get const pointer to data page header.
     * @return Const pointer to data header.
     */
    [[nodiscard]] const DataHeader *dataHeader() const noexcept { return header<DataHeader>(); }

    /** @} */

    /**
     * @brief Const & non-const getters for slot directory.
     * @{
     */

    /**
     * @brief Get pointer to slot directory.
     * @return Pointer to slots.
     */
    [[nodiscard]] SlotPair *slotsDirectory() noexcept {
      return reinterpret_cast<SlotPair *>(data + DATA_HEADER_SIZE);
    }

    /**
     * @brief Get const pointer to slot directory.
     * @return Const pointer to slots.
     */
    [[nodiscard]] const SlotPair *slotsDirectory() const noexcept {
      return reinterpret_cast<const SlotPair *>(data + DATA_HEADER_SIZE);
    }

    /** @} */

    /**
     * @brief Const & non-const getters for slot array.
     * @{
     */

    /**
     * @brief Get pointer to slot array.
     * @return Pointer to slots.
     */
    [[nodiscard]] SlotEntry *slots() noexcept {
      return reinterpret_cast<SlotEntry *>(
          data + DATA_HEADER_SIZE + (dataHeader()->directoryCount * PAIR_SIZE)
      );
    }

    /**
     * @brief Get const pointer to slot array.
     * @return Const pointer to slots.
     */
    [[nodiscard]] const SlotEntry *slots() const noexcept {
      return reinterpret_cast<SlotEntry *>(
          data + DATA_HEADER_SIZE + (dataHeader()->directoryCount * PAIR_SIZE)
      );
    }

    /** @} */
  };
} // namespace pulse::storage

#endif // PULSEDB_STORAGE_DATA_PAGE_HPP
