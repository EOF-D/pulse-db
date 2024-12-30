/**
 * @file src/storage/data_page.cpp
 * @brief Implements the data page class.
 */

#include "pulsedb/storage/data_page.hpp"
#include <cstring>
#include <vector>

namespace pulse::storage {
  DataPage::DataPage(uint32_t pageId) noexcept : Page(pageId, PageType::DATA) {
    // Initialize the header.
    auto *header = dataHeader();
    header->freeSpaceOffset = PAGE_SIZE;
    header->freeSpace = MAX_FREE_SPACE;

    header->firstSlotOffset = DATA_HEADER_SIZE;
    header->firstFreeSlot = INVALID_SLOT;

    header->slotCount = 0;
  }

  std::optional<uint16_t>
  DataPage::insertRecord(uint32_t key, const void *data, uint16_t length, uint16_t type) {
    // Calculate total space needed including directory pair.
    const uint16_t recordSpace = spaceNeeded(length);
    const uint16_t totalSpace = recordSpace + DataPage::PAIR_SIZE;

    // Check if there's enough space for both record and directory entry.
    if (!hasSpace(totalSpace)) {
      return std::nullopt;
    }

    // Get a free slot.
    auto slotId = findFreeSlot();
    if (!slotId) {
      return std::nullopt;
    }

    // Add directory entry.
    if (!insertPair(key, *slotId)) {
      return std::nullopt;
    }

    // Allocate space for the record.
    auto offset = allocateSpace(length + RECORD_HEADER_SIZE);
    if (!offset) {
      removeLastPair(); // Rollback if allocation failed.
      return std::nullopt;
    }

    // Initialize the record header.
    auto *recordHeader = reinterpret_cast<RecordHeader *>(this->data + *offset);
    recordHeader->length = length;
    recordHeader->type = type;

    // Copy the data into the page.
    std::memcpy(this->data + *offset + RECORD_HEADER_SIZE, data, length);

    // Update slot entry.
    auto &slot = slots()[*slotId];
    slot.offset = *offset;
    slot.length = length + RECORD_HEADER_SIZE;
    slot.flags = SlotFlags::NONE;

    dataHeader()->freeSpace -= totalSpace;
    dataHeader()->itemCount++;

    return slotId;
  }

  bool DataPage::deleteRecord(uint16_t slotId) {
    if (slotId >= dataHeader()->slotCount) {
      return false;
    }

    // Check if the slot is already deleted.
    auto &slot = slots()[slotId];
    if (SlotFlags::isSet(slot.flags, SlotFlags::DELETED)) {
      return false;
    }

    // Mark the slot as deleted.
    slot.flags = SlotFlags::set(slot.flags, SlotFlags::DELETED);
    slot.offset = dataHeader()->firstFreeSlot;

    dataHeader()->firstFreeSlot = slotId;
    dataHeader()->itemCount--;

    return true;
  }

  std::optional<std::pair<const void *, uint16_t>> DataPage::getRecord(uint16_t slotId) const {
    if (slotId >= dataHeader()->slotCount) {
      return std::nullopt;
    }

    const auto &slot = slots()[slotId];
    if (SlotFlags::isSet(slot.flags, SlotFlags::DELETED)) {
      return std::nullopt;
    }

    const auto *recordHeader = reinterpret_cast<const RecordHeader *>(data + slot.offset);
    return std::make_pair(data + slot.offset + RECORD_HEADER_SIZE, recordHeader->length);
  }

  std::optional<uint16_t> DataPage::getRecordType(uint16_t slotId) const {
    if (slotId >= dataHeader()->slotCount) {
      return std::nullopt;
    }

    const auto &slot = slots()[slotId];
    if (SlotFlags::isSet(slot.flags, SlotFlags::DELETED)) {
      return std::nullopt;
    }

    const auto *recordHeader = reinterpret_cast<const RecordHeader *>(data + slot.offset);
    return recordHeader->type;
  }

  bool DataPage::hasFlag(uint16_t slotId, uint16_t flag) const noexcept {
    if (slotId >= dataHeader()->slotCount) {
      return false;
    }

    return SlotFlags::isSet(slots()[slotId].flags, flag);
  }

  bool DataPage::setFlag(uint16_t slotId, uint16_t flag) noexcept {
    if (slotId >= dataHeader()->slotCount) {
      return false;
    }

    slots()[slotId].flags = SlotFlags::set(slots()[slotId].flags, flag);
    return true;
  }

  bool DataPage::clearFlag(uint16_t slotId, uint16_t flag) noexcept {
    if (slotId >= dataHeader()->slotCount) {
      return false;
    }

    slots()[slotId].flags = SlotFlags::clear(slots()[slotId].flags, flag);
    return true;
  }

  uint16_t DataPage::compact() {
    uint16_t bytesFreed = 0;
    uint16_t writeOffset = PAGE_SIZE;

    uint16_t totalSpace = 0;
    std::vector<uint8_t> tempData(PAGE_SIZE);

    for (uint16_t i = 0; i < dataHeader()->slotCount; i++) {
      auto &slot = slots()[i];
      if (!SlotFlags::isSet(slot.flags, SlotFlags::DELETED)) {
        totalSpace += slot.length;
        writeOffset -= slot.length;

        std::memcpy(tempData.data() + writeOffset, data + slot.offset, slot.length);
        slot.offset = writeOffset;
      }
    }

    bytesFreed = dataHeader()->freeSpaceOffset - writeOffset;

    // Only copy back if we actually freed space.
    if (bytesFreed > 0) {
      std::memcpy(data + writeOffset, tempData.data() + writeOffset, PAGE_SIZE - writeOffset);
      dataHeader()->freeSpaceOffset = writeOffset;
      dataHeader()->freeSpace += bytesFreed; // Update the page's free space counter.
    }

    // Rebuild the free slot list.
    dataHeader()->firstFreeSlot = INVALID_SLOT;
    uint16_t lastFree = INVALID_SLOT;

    for (uint16_t i = 0; i < dataHeader()->slotCount; i++) {
      if (SlotFlags::isSet(slots()[i].flags, SlotFlags::DELETED)) {
        if (lastFree == INVALID_SLOT) {
          dataHeader()->firstFreeSlot = i;
        }

        else {
          slots()[lastFree].offset = i;
        }

        lastFree = i;
      }
    }

    return bytesFreed;
  }

  bool DataPage::needsCompact() const noexcept {
    const uint16_t usedSpace = PAGE_SIZE - dataHeader()->freeSpace;
    uint16_t actualData = itemCount() * RECORD_HEADER_SIZE;

    for (uint16_t i = 0; i < dataHeader()->slotCount; i++) {
      const auto &slot = slots()[i];
      if (!SlotFlags::isSet(slot.flags, SlotFlags::DELETED)) {
        actualData += slot.length;
      }
    }

    // Calculate if we can compact.
    // Check if fragmentation is greater than 25%.
    return usedSpace > 0 && (usedSpace - actualData) * 4 > usedSpace;
  }

  std::optional<uint16_t> DataPage::findFreeSlot() noexcept {
    // First check for reusable slots.
    if (dataHeader()->firstFreeSlot != INVALID_SLOT) {
      uint16_t slotId = dataHeader()->firstFreeSlot;
      dataHeader()->firstFreeSlot = slots()[slotId].offset;

      return slotId;
    }

    // Calculate space needed for new slot.
    const uint16_t slotsOffset = DATA_HEADER_SIZE + (dataHeader()->directoryCount * PAIR_SIZE);
    const uint16_t newSlotOffset = slotsOffset + (dataHeader()->slotCount * SLOT_SIZE);

    // Check if we have space for new slot.
    if (newSlotOffset + SLOT_SIZE >= dataHeader()->freeSpaceOffset) {
      return std::nullopt;
    }

    return dataHeader()->slotCount++;
  }

  [[nodiscard]] std::optional<uint16_t> DataPage::getSlotId(uint32_t key) const {
    // TODO: Implement a better search algorithm.
    const auto *dir = slotsDirectory();
    for (uint16_t i = 0; i < dataHeader()->directoryCount; i++) {
      if (dir[i].key == key) {
        return dir[i].slotId;
      }
    }

    return std::nullopt;
  }

  bool DataPage::insertPair(uint32_t key, uint16_t slotId) {
    // Check if we have space for new directory entry.
    const uint16_t newDirOffset = DATA_HEADER_SIZE + (dataHeader()->directoryCount * PAIR_SIZE);
    if (newDirOffset + PAIR_SIZE >= dataHeader()->freeSpaceOffset) {
      return false;
    }

    // Insert the new pair.
    auto *dir = slotsDirectory();
    dir[dataHeader()->directoryCount].key = key;
    dir[dataHeader()->directoryCount].slotId = slotId;
    dataHeader()->directoryCount++;

    return true;
  }

  std::optional<uint16_t> DataPage::allocateSpace(uint16_t size) noexcept {
    auto *header = dataHeader();

    const uint16_t newOffset = header->freeSpaceOffset - size;
    const uint16_t slotsEnd =
        DATA_HEADER_SIZE + (header->directoryCount * PAIR_SIZE) + (header->slotCount * SLOT_SIZE);

    // Check if there is enough space.
    if (newOffset < slotsEnd) {
      return std::nullopt;
    }

    header->freeSpaceOffset = newOffset;
    return newOffset;
  }
} // namespace pulse::storage
