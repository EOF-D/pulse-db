/**
 * @file tests/pulsedb/storage/page_test.cpp
 * @brief Test cases for Page base class.
 */

#include "pulsedb/storage/page.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("Page basic operations", "[storage][page]") {
  SECTION("constructor") {
    const PageType testType = PageType::INDEX;
    const uint32_t testId = 1;
    Page page(testId, testType);

    // Verify the page header values.
    REQUIRE(page.id() == testId);
    REQUIRE(page.type() == testType);
    REQUIRE(page.lsn() == 0);
    REQUIRE(page.freeSpace() == Page::MAX_FREE_SPACE);
    REQUIRE(page.itemCount() == 0);
  }

  SECTION("move constructor") {
    Page orig(1, PageType::INDEX);
    const uint32_t origId = orig.id();
    const PageType origType = orig.type();

    Page target(std::move(orig));

    // Verify the page header values.
    REQUIRE(target.id() == origId);
    REQUIRE(target.type() == origType);
    REQUIRE(target.freeSpace() == Page::MAX_FREE_SPACE);
    REQUIRE(target.itemCount() == 0);
  }

  SECTION("move assignment") {
    Page orig(1, PageType::INDEX);
    const uint32_t origId = orig.id();
    const PageType origType = orig.type();

    Page target(2, PageType::INDEX);
    target = std::move(orig);

    // Verify the page header values.
    REQUIRE(target.id() == origId);
    REQUIRE(target.type() == origType);
    REQUIRE(target.freeSpace() == Page::MAX_FREE_SPACE);
    REQUIRE(target.itemCount() == 0);
  }
}

TEST_CASE("Page space managment", "[storage][page]") {
  SECTION("has space") {
    Page page(1, PageType::INDEX);

    // Verify hasSpace() method is working.
    REQUIRE(page.hasSpace(100));
    REQUIRE(page.hasSpace(Page::MAX_FREE_SPACE));
    REQUIRE_FALSE(page.hasSpace(Page::MAX_FREE_SPACE + 1));
  }

  SECTION("size constants") {
    REQUIRE(Page::PAGE_SIZE == 4096);
    REQUIRE(Page::HEADER_SIZE == sizeof(PageHeader));
    REQUIRE(Page::MAX_FREE_SPACE == Page::PAGE_SIZE - Page::HEADER_SIZE);
  }
}

TEST_CASE("Page type handling", "[storage][page]") {
  SECTION("different page types") {
    Page dataPage(1, PageType::DATA);
    Page indexPage(2, PageType::INDEX);
    Page invalidPage(3, PageType::INVALID);

    // Verify the page types.
    REQUIRE(dataPage.type() == PageType::DATA);
    REQUIRE(indexPage.type() == PageType::INDEX);
    REQUIRE(invalidPage.type() == PageType::INVALID);
  }
}
