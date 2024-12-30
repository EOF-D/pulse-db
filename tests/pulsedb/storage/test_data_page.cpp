/**
 * @file tests/pulsedb/storage/test_data_page.cpp
 * @brief Test cases for DataPage class.
 */

#include "pulsedb/storage/data_page.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("DataPage basic operations", "[storage][data_page]") {
  SECTION("constructor") {
    DataPage page(1);

    // Verify the page header values.
    REQUIRE(page.id() == 1);
    REQUIRE(page.type() == PageType::DATA);
    REQUIRE(page.slotCount() == 0);
    REQUIRE(page.directoryCount() == 0);
    REQUIRE(page.itemCount() == 0);
    REQUIRE(page.freeSpace() == DataPage::MAX_FREE_SPACE);
  }
}

TEST_CASE("DataPage record operations", "[storage][data_page]") {
  SECTION("insert and retrieve") {
    DataPage page(1);
    const char *testData = "foo";
    const uint16_t dataLen = 3;

    // Insert test record.
    auto slot = page.insertRecord(1, testData, dataLen, 1);
    REQUIRE(slot);
    REQUIRE(page.itemCount() == 1);
    REQUIRE(page.slotCount() == 1);
    REQUIRE(page.directoryCount() == 1);

    // Verify retrieval.
    auto record = page.getRecord(*slot);
    REQUIRE(record);
    REQUIRE(record->second == dataLen);
    REQUIRE(std::memcmp(record->first, testData, dataLen) == 0);

    // Verify type.
    auto type = page.getRecordType(*slot);
    REQUIRE(type);
    REQUIRE(*type == 1);
  }

  SECTION("key lookup") {
    DataPage page(1);
    const char *testData = "bar";
    const uint16_t dataLen = 3;

    // Insert and lookup by key.
    auto slot = page.insertRecord(1, testData, dataLen, 1);
    REQUIRE(slot);

    auto foundSlot = page.getSlotId(1);
    REQUIRE(foundSlot);
    REQUIRE(*foundSlot == *slot);
  }

  SECTION("record deletion") {
    DataPage page(1);
    const char *testData = "baz";
    const uint16_t dataLen = 3;

    // Insert and delete record.
    auto slot = page.insertRecord(1, testData, dataLen, 1);
    REQUIRE(slot);
    REQUIRE(page.deleteRecord(*slot));

    // Verify deletion.
    REQUIRE(page.itemCount() == 0);
    REQUIRE_FALSE(page.getRecord(*slot));
    REQUIRE(page.hasFlag(*slot, SlotFlags::DELETED));
  }
}

TEST_CASE("DataPage space management", "[storage][data_page]") {
  SECTION("space needed calculation") {
    const uint16_t dataLen = 100;
    const uint16_t expectedSpace = DataPage::SLOT_SIZE + DataPage::RECORD_HEADER_SIZE + dataLen;
    REQUIRE(DataPage::spaceNeeded(dataLen) == expectedSpace);
  }

  SECTION("full page handling") {
    DataPage page(1);
    std::string large(DataPage::MAX_FREE_SPACE, '0');

    // Attempt to insert oversized record.
    auto slot = page.insertRecord(1, large.c_str(), large.length(), 1);
    REQUIRE_FALSE(slot);
  }

  SECTION("space tracking") {
    DataPage page(1);
    const uint16_t recordSize = 100;

    std::string data(recordSize, '0');
    const uint16_t initialFree = page.freeSpace();

    // Calculate total space needed including all overhead.
    const uint16_t spaceNeeded =
        DataPage::spaceNeeded(recordSize); // Record + slot + record header.

    // Insert record and verify space update.
    auto slot = page.insertRecord(1, data.c_str(), recordSize, 1);
    REQUIRE(slot);

    REQUIRE(page.freeSpace() < initialFree);
    REQUIRE(page.freeSpace() == initialFree - (spaceNeeded + DataPage::PAIR_SIZE));
  }
}

TEST_CASE("DataPage fragmentation operations", "[storage][data_page]") {
  SECTION("compaction needed check") {
    DataPage page(1);
    const uint16_t recordSize = 100;
    std::string data(recordSize, '0');

    // Fill page with records.
    std::vector<uint16_t> slots;
    for (uint32_t i = 0; i < 10; i++) {
      auto slot = page.insertRecord(i, data.c_str(), recordSize, 1);
      REQUIRE(slot);

      slots.push_back(*slot);
    }

    // Create fragmentation.
    for (size_t i = 0; i < slots.size(); i += 2) {
      REQUIRE(page.deleteRecord(slots[i]));
    }

    REQUIRE(page.needsCompact());
  }

  SECTION("compaction execution") {
    DataPage page(1);
    const uint16_t recordSize = 100;
    std::string data(recordSize, '0');

    // Fill page with records.
    std::vector<uint16_t> slots;
    for (uint32_t i = 0; i < 10; i++) {
      auto slot = page.insertRecord(i, data.c_str(), recordSize, 1);
      REQUIRE(slot);

      slots.push_back(*slot);
    }

    // Create fragmentation.
    for (size_t i = 0; i < slots.size(); i += 2) {
      REQUIRE(page.deleteRecord(slots[i]));
    }

    // Perform compaction.
    uint16_t freed = page.compact();
    REQUIRE(freed > 0);

    // Verify remaining records.
    for (size_t i = 1; i < slots.size(); i += 2) {
      auto record = page.getRecord(slots[i]);

      REQUIRE(record);
      REQUIRE(record->second == recordSize);
    }
  }
}

TEST_CASE("DataPage slot flag operations", "[storage][data_page]") {
  SECTION("flag manipulation") {
    DataPage page(1);
    const char *testData = "foo";
    const uint16_t dataLen = 3;

    // Insert record for flag testing.
    auto slot = page.insertRecord(1, testData, dataLen, 1);
    REQUIRE(slot);

    // Test flag operations.
    REQUIRE_FALSE(page.hasFlag(*slot, SlotFlags::DELETED));
    REQUIRE(page.setFlag(*slot, SlotFlags::DELETED));
    REQUIRE(page.hasFlag(*slot, SlotFlags::DELETED));
    REQUIRE(page.clearFlag(*slot, SlotFlags::DELETED));
    REQUIRE_FALSE(page.hasFlag(*slot, SlotFlags::DELETED));
  }

  SECTION("deleted flag behavior") {
    DataPage page(1);
    const char *testData = "bar";
    const uint16_t dataLen = 3;

    auto slot = page.insertRecord(1, testData, dataLen, 1);
    REQUIRE(slot);

    REQUIRE_FALSE(page.hasFlag(*slot, SlotFlags::DELETED));
    REQUIRE(page.deleteRecord(*slot));

    REQUIRE(page.hasFlag(*slot, SlotFlags::DELETED));
  }
}

TEST_CASE("DataPage slot reuse", "[storage][data_page]") {
  SECTION("deleted slot reuse") {
    DataPage page(1);
    const char *testData = "baz";
    const uint16_t dataLen = 3;

    // Create and delete a slot.
    auto slot1 = page.insertRecord(1, testData, dataLen, 1);
    REQUIRE(slot1);
    REQUIRE(page.deleteRecord(*slot1));

    // New insertion should reuse deleted slot.
    auto slot2 = page.insertRecord(2, testData, dataLen, 1);
    REQUIRE(slot2);
    REQUIRE(*slot2 == *slot1);
  }
}

TEST_CASE("DataPage error handling", "[storage][data_page]") {
  DataPage page(1);

  SECTION("invalid slot operations") {
    REQUIRE_FALSE(page.getRecord(0));
    REQUIRE_FALSE(page.getRecordType(0));
    REQUIRE_FALSE(page.deleteRecord(0));
    REQUIRE_FALSE(page.getSlotId(0));
  }

  SECTION("empty record handling") {
    auto slot = page.insertRecord(1, "", 0, 1);
    REQUIRE(slot);

    auto record = page.getRecord(*slot);
    REQUIRE(record);
    REQUIRE(record->second == 0);
  }
}
