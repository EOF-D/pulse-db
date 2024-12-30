/**
 * @file tests/pulsedb/storage/test_disk_manager.cpp
 * @brief Test cases for DiskManager class.
 */

#include "pulsedb/storage/data_page.hpp"
#include "pulsedb/storage/disk_manager.hpp"
#include "pulsedb/storage/index_page.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;
namespace fs = std::filesystem;

TEST_CASE("DiskManager construction", "[storage][disk_manager]") {
  const fs::path testPath = "test.db";

  // Cleanup any existing test file.
  if (fs::exists(testPath)) {
    fs::remove(testPath);
  }

  SECTION("new database creation") {
    DiskManager dm(testPath, true);
    REQUIRE(fs::exists(testPath));
    REQUIRE(dm.pageCount() == 0);
    REQUIRE(dm.fileSize() >= sizeof(DatabaseHeader));
  }

  SECTION("opening non-existent database") {
    REQUIRE_THROWS_AS(DiskManager(testPath, false), std::runtime_error);
  }

  SECTION("reopening existing database") {
    {
      DiskManager dm(testPath, true);
      REQUIRE(dm.pageCount() == 0);
    }

    DiskManager dm(testPath, false);
    REQUIRE(dm.pageCount() == 0);
  }

  // Cleanup.
  fs::remove(testPath);
}

TEST_CASE("DiskManager page allocation", "[storage][disk_manager]") {
  const fs::path testPath = "test.db";
  if (fs::exists(testPath)) {
    fs::remove(testPath);
  }

  SECTION("sequential allocation") {
    DiskManager dm(testPath, true);

    // Allocate several pages.
    uint32_t id1 = dm.allocatePage();
    uint32_t id2 = dm.allocatePage();
    uint32_t id3 = dm.allocatePage();

    REQUIRE(id1 == 0);
    REQUIRE(id2 == 1);
    REQUIRE(id3 == 2);
    REQUIRE(dm.pageCount() == 3);
  }

  SECTION("deallocation and reuse") {
    DiskManager dm(testPath, true);

    // Allocate and deallocate pages.
    uint32_t id1 = dm.allocatePage();
    uint32_t id2 = dm.allocatePage();

    REQUIRE(dm.deallocatePage(id1));

    // New allocation should reuse the deallocated page.
    uint32_t id3 = dm.allocatePage();
    REQUIRE(id3 == id1);
  }

  SECTION("invalid deallocation") {
    DiskManager dm(testPath, true);
    REQUIRE_FALSE(dm.deallocatePage(DiskManager::INVALID_PAGE_ID));
    REQUIRE_FALSE(dm.deallocatePage(1000)); // Non-existent page.
  }

  // Cleanup.
  fs::remove(testPath);
}

TEST_CASE("DiskManager page I/O", "[storage][disk_manager]") {
  const fs::path testPath = "test.db";
  if (fs::exists(testPath)) {
    fs::remove(testPath);
  }

  SECTION("data page write and read") {
    DiskManager dm(testPath, true);
    uint32_t pageId = dm.allocatePage();

    // Create and write a data page.
    {
      DataPage page(pageId);
      const char *testData = "foobarbaz";

      auto slot = page.insertRecord(1, testData, strlen(testData) + 1, 1);
      REQUIRE(slot);
      REQUIRE(dm.flushPage(page));
    }

    // Read and verify the page
    auto readPage = dm.fetchPage(pageId);
    REQUIRE(readPage != nullptr);
    REQUIRE(readPage->type() == PageType::DATA);
    REQUIRE(readPage->id() == pageId);

    auto *dataPage = static_cast<DataPage *>(readPage.get());
    auto slotId = dataPage->getSlotId(1);
    REQUIRE(slotId);

    auto record = dataPage->getRecord(*slotId);
    REQUIRE(record);
    REQUIRE(strcmp(static_cast<const char *>(record->first), "foobarbaz") == 0);
  }

  SECTION("index page write and read") {
    DiskManager dm(testPath, true);
    uint32_t pageId = dm.allocatePage();

    // Create and write an index page.
    {
      IndexPage page(pageId, true);
      REQUIRE(page.insertKey(1, 100));
      REQUIRE(page.insertKey(2, 200));
      REQUIRE(dm.flushPage(page));
    }

    // Read and verify the page.
    auto readPage = dm.fetchPage(pageId);
    REQUIRE(readPage != nullptr);
    REQUIRE(readPage->type() == PageType::INDEX);
    REQUIRE(readPage->id() == pageId);

    auto *indexPage = static_cast<IndexPage *>(readPage.get());
    auto pageId1 = indexPage->lookup(1);
    auto pageId2 = indexPage->lookup(2);

    REQUIRE(pageId1);
    REQUIRE(pageId2);
    REQUIRE(*pageId1 == 100);
    REQUIRE(*pageId2 == 200);
  }

  SECTION("invalid page reads") {
    DiskManager dm(testPath, true);
    REQUIRE(dm.fetchPage(DiskManager::INVALID_PAGE_ID) == nullptr);
    REQUIRE(dm.fetchPage(1000) == nullptr); // Non-existent page.
  }

  // Cleanup.
  fs::remove(testPath);
}

TEST_CASE("DiskManager persistence", "[storage][disk_manager]") {
  const fs::path testPath = "test.db";
  if (fs::exists(testPath)) {
    fs::remove(testPath);
  }

  SECTION("database state persistence") {
    uint32_t pageId;

    // Create and populate database.
    {
      DiskManager dm(testPath, true);
      pageId = dm.allocatePage();

      DataPage page(pageId);
      const char *testData = "foobarbaz";

      auto slot = page.insertRecord(1, testData, strlen(testData) + 1, 1);
      REQUIRE(slot);
      REQUIRE(dm.flushPage(page));
      REQUIRE(dm.sync());
    }

    // Reopen and verify.
    {
      DiskManager dm(testPath, false);
      REQUIRE(dm.pageCount() == 1);

      auto readPage = dm.fetchPage(pageId);
      REQUIRE(readPage != nullptr);

      auto *dataPage = static_cast<DataPage *>(readPage.get());
      auto slotId = dataPage->getSlotId(1);
      REQUIRE(slotId);

      auto record = dataPage->getRecord(*slotId);
      REQUIRE(record);
      REQUIRE(strcmp(static_cast<const char *>(record->first), "foobarbaz") == 0);
    }
  }

  SECTION("sync behavior") {
    DiskManager dm(testPath, true);
    uint32_t pageId = dm.allocatePage();
    DataPage page(pageId);

    const char *testData = "foobarbaz";
    auto slot = page.insertRecord(1, testData, strlen(testData) + 1, 1);
    REQUIRE(slot);

    // Verify data is persisted after sync.
    REQUIRE(dm.flushPage(page));
    REQUIRE(dm.sync());

    auto readPage = dm.fetchPage(pageId);
    REQUIRE(readPage != nullptr);

    auto *dataPage = static_cast<DataPage *>(readPage.get());
    auto slotId = dataPage->getSlotId(1);
    REQUIRE(slotId);

    auto record = dataPage->getRecord(*slotId);
    REQUIRE(record);
    REQUIRE(strcmp(static_cast<const char *>(record->first), "foobarbaz") == 0);
  }

  // Cleanup.
  fs::remove(testPath);
}

TEST_CASE("DiskManager move semantics", "[storage][disk_manager]") {
  const fs::path testPath = "test.db";
  if (fs::exists(testPath)) {
    fs::remove(testPath);
  }

  SECTION("move construction") {
    DiskManager dm1(testPath, true);
    uint32_t pageId = dm1.allocatePage();

    // Create and write a data page.
    {
      DataPage page(pageId);
      const char *testData = "foobarbaz";
      auto slot = page.insertRecord(1, testData, strlen(testData) + 1, 1);

      REQUIRE(slot);
      REQUIRE(dm1.flushPage(page));
      REQUIRE(dm1.sync());
    }

    DiskManager dm2(std::move(dm1));
    auto page = dm2.fetchPage(pageId);
    REQUIRE(page != nullptr);

    // If we get the page, verify its contents.
    if (page != nullptr) {
      REQUIRE(page->type() == PageType::DATA);

      auto *dataPage = static_cast<DataPage *>(page.get());
      auto slotId = dataPage->getSlotId(1);
      REQUIRE(slotId);

      auto record = dataPage->getRecord(*slotId);
      REQUIRE(record);
      REQUIRE(strcmp(static_cast<const char *>(record->first), "foobarbaz") == 0);
    }
  }

  SECTION("move assignment") {
    DiskManager dm1(testPath, true);
    uint32_t pageId = dm1.allocatePage();

    // Create and write a data page.
    {
      DataPage page(pageId);
      const char *testData = "foobarbaz";
      auto slot = page.insertRecord(1, testData, strlen(testData) + 1, 1);

      REQUIRE(slot);
      REQUIRE(dm1.flushPage(page));
      REQUIRE(dm1.sync());
    }

    DiskManager dm2("temp.db", true);
    dm2 = std::move(dm1);
    auto page = dm2.fetchPage(pageId);
    REQUIRE(page != nullptr);

    // Verify contents if we get the page.
    if (page != nullptr) {
      REQUIRE(page->type() == PageType::DATA);

      auto *dataPage = static_cast<DataPage *>(page.get());
      auto slotId = dataPage->getSlotId(1);
      REQUIRE(slotId);

      auto record = dataPage->getRecord(*slotId);
      REQUIRE(record);
      REQUIRE(strcmp(static_cast<const char *>(record->first), "foobarbaz") == 0);
    }
  }

  // Cleanup.
  fs::remove(testPath);
  if (fs::exists("temp.db")) {
    fs::remove("temp.db");
  }
}
