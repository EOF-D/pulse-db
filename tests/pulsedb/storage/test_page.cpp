#include "pulsedb/storage/page.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("Page operations", "[storage][page]") {
  SECTION("Creation") {
    const uint32_t testId = 1;
    Page page(testId);

    REQUIRE(page.pageId() == testId);
    REQUIRE(page.dataSize() == 0);
  }

  SECTION("Read & write") {
    Page page(1);
    const char testData[] = "Hello, pulse-db!";
    const uint32_t dataSize = sizeof(testData) - 1; // Exclude null terminator.

    REQUIRE(page.write(0, testData, dataSize));
    REQUIRE(page.dataSize() == dataSize);

    char buffer[16];
    uint32_t bytesRead = page.read(0, buffer, dataSize);

    REQUIRE(bytesRead == dataSize);
    REQUIRE(memcmp(buffer, testData, dataSize) == 0);
  }
}

TEST_CASE("Page boundaries", "[storage][page]") {
  Page page(1);

  SECTION("Write at page limit") {
    char data[Page::MAX_DATA_SIZE];
    std::memset(data, '!', Page::MAX_DATA_SIZE);

    REQUIRE(page.write(0, data, Page::MAX_DATA_SIZE));
    REQUIRE(page.dataSize() == Page::MAX_DATA_SIZE);
  }

  SECTION("Write beyond page size") {
    char data[Page::MAX_DATA_SIZE + 1];
    std::memset(data, '!', Page::MAX_DATA_SIZE + 1);

    REQUIRE_FALSE(page.write(0, data, Page::MAX_DATA_SIZE + 1));
    REQUIRE(page.dataSize() == 0);
  }

  SECTION("Write at offset near boundary") {
    char data[10];
    std::memset(data, '!', 10);

    REQUIRE(page.write(Page::MAX_DATA_SIZE - 10, data, 10));
    REQUIRE_FALSE(page.write(Page::MAX_DATA_SIZE - 5, data, 10));
  }
}

TEST_CASE("Page data integrity", "[storage][page]") {
  Page page(1);

  SECTION("Multiple writes & reads") {
    const char data1[] = "foo";
    const char data2[] = "bar";
    const uint32_t size1 = sizeof(data1) - 1; // Exclude null terminator.
    const uint32_t size2 = sizeof(data2) - 1;

    // Write first chunk.
    REQUIRE(page.write(0, data1, size1));

    // Write second chunk.
    REQUIRE(page.write(size1, data2, size2));

    // Read and verify first chunk.
    char buffer1[16];
    REQUIRE(page.read(0, buffer1, size1) == size1);
    REQUIRE(memcmp(buffer1, data1, size1) == 0);

    // Read and verify second chunk.
    char buffer2[16];
    REQUIRE(page.read(size1, buffer2, size2) == size2);
    REQUIRE(memcmp(buffer2, data2, size2) == 0);
  }

  SECTION("Overwrite data") {
    const char initialData[] = "initial data";
    const char newData[] = "new data";
    const uint32_t initialSize = sizeof(initialData) - 1;
    const uint32_t newSize = sizeof(newData) - 1;

    // Write initial data.
    REQUIRE(page.write(0, initialData, initialSize));

    // Overwrite with new data.
    REQUIRE(page.write(0, newData, newSize));

    // Read and verify
    char buffer[16];
    REQUIRE(page.read(0, buffer, newSize) == newSize);
    REQUIRE(memcmp(buffer, newData, newSize) == 0);
  }
}

TEST_CASE("Page reading", "[storage][page]") {
  Page page(1);

  SECTION("Reading from empty page") {
    char buffer[16];
    REQUIRE(page.read(0, buffer, sizeof(buffer)) == 0);
  }

  SECTION("Reading beyond written data") {
    const char data[] = "test data";
    const uint32_t dataSize = sizeof(data) - 1;

    REQUIRE(page.write(0, data, dataSize));

    char buffer[16];
    REQUIRE(page.read(dataSize, buffer, sizeof(buffer)) == 0);
  }

  SECTION("Reading with offset") {
    const char data[] = "test data for offset reading";
    const char expectedData[] = "data ";

    const uint32_t dataSize = sizeof(data) - 1;
    const uint32_t readOffset = 5;
    const uint32_t readSize = 5;

    REQUIRE(page.write(0, data, dataSize));

    char buffer[sizeof(expectedData)];
    REQUIRE(page.read(readOffset, buffer, readSize) == readSize);
    REQUIRE(memcmp(buffer, expectedData, readSize) == 0);
  }
}
