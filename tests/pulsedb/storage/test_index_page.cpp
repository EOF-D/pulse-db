/**
 * @file tests/pulsedb/storage/test_index_page.cpp
 * @brief Test cases for IndexPage class.
 */

#include "pulsedb/storage/index_page.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("IndexPage basic operations", "[storage][index_page]") {
  SECTION("leaf constructor") {
    IndexPage page(1, true);

    // Verify the page header values.
    REQUIRE(page.id() == 1);
    REQUIRE(page.type() == PageType::INDEX);
    REQUIRE(page.isLeaf() == true);
    REQUIRE(page.level() == 0);
    REQUIRE(page.nextPage() == 0);
    REQUIRE(page.prevPage() == 0);
    REQUIRE(page.parentPage() == 0);
    REQUIRE(page.itemCount() == 0);
  }

  SECTION("internal node constructor") {
    const uint16_t level = 2;
    IndexPage page(1, false, level);

    // Verify internal node properties.
    REQUIRE(page.isLeaf() == false);
    REQUIRE(page.level() == level);
  }
}

TEST_CASE("IndexPage key operations", "[storage][index_page]") {
  SECTION("insert and lookup") {
    IndexPage page(1, true);

    // Insert test keys.
    REQUIRE(page.insertKey(10, 100));
    REQUIRE(page.insertKey(20, 200));
    REQUIRE(page.insertKey(30, 300));

    // Verify lookups return correct values.
    auto result1 = page.lookup(10);
    REQUIRE(result1);
    REQUIRE(*result1 == 100);

    auto result2 = page.lookup(20);
    REQUIRE(result2);
    REQUIRE(*result2 == 200);

    auto result3 = page.lookup(30);
    REQUIRE(result3);
    REQUIRE(*result3 == 300);
  }

  SECTION("missing key lookup") {
    IndexPage page(1, true);
    REQUIRE(page.insertKey(10, 100));

    // Verify lookup of non-existent key.
    auto result = page.lookup(40);
    REQUIRE_FALSE(result);
  }

  SECTION("key removal") {
    IndexPage page(1, true);

    // Insert and remove key.
    REQUIRE(page.insertKey(10, 100));
    REQUIRE(page.removeKey(10));

    // Verify key was removed.
    REQUIRE_FALSE(page.lookup(10));
    REQUIRE_FALSE(page.removeKey(10)); // Already removed.
  }
}

TEST_CASE("IndexPage range operations", "[storage][index_page]") {
  SECTION("exact range") {
    IndexPage page(1, true);

    // Insert test keys.
    REQUIRE(page.insertKey(10, 100));
    REQUIRE(page.insertKey(20, 200));
    REQUIRE(page.insertKey(30, 300));

    // Verify exact range query.
    auto result = page.getRange(10, 30);
    REQUIRE(result.size() == 3);
    REQUIRE(result[0] == 100);
    REQUIRE(result[1] == 200);
    REQUIRE(result[2] == 300);
  }

  SECTION("partial range") {
    IndexPage page(1, true);

    // Insert test keys.
    REQUIRE(page.insertKey(10, 100));
    REQUIRE(page.insertKey(20, 200));
    REQUIRE(page.insertKey(30, 300));

    // Verify partial range query.
    auto result = page.getRange(15, 25);
    REQUIRE(result.size() == 1);
    REQUIRE(result[0] == 200);
  }

  SECTION("empty range") {
    IndexPage page(1, true);

    // Verify empty range query.
    auto result = page.getRange(0, 100);
    REQUIRE(result.empty());
  }
}

TEST_CASE("IndexPage node operations", "[storage][index_page]") {
  SECTION("node splitting") {
    IndexPage page1(1, true);
    page1.setNextPage(3);

    // Fill first page to capacity.
    for (uint64_t i = 0; i < IndexPage::maxEntries(); ++i) {
      REQUIRE(page1.insertKey(i * 10, i * 100));
    }

    // Verify split operation.
    REQUIRE(page1.isOverflow());
    IndexPage page2(2, true);
    uint64_t medianKey = page1.split(page2);

    // Verify post-split state.
    REQUIRE_FALSE(page1.isOverflow());
    REQUIRE(page1.itemCount() <= IndexPage::maxEntries() / 2);
    REQUIRE(page2.itemCount() <= IndexPage::maxEntries() / 2);
    REQUIRE(page2.nextPage() == 3);
    REQUIRE(page2.prevPage() == page1.id());
    REQUIRE(page1.nextPage() == page2.id());
  }

  SECTION("node merging") {
    IndexPage page1(1, true);
    IndexPage page2(2, true);

    // Populate both pages.
    for (uint64_t i = 0; i < 5; ++i) {
      REQUIRE(page1.insertKey(i * 10, i * 100));
      REQUIRE(page2.insertKey((i + 5) * 10, (i + 5) * 100));
    }

    // Verify merge operation.
    page2.setNextPage(3);
    REQUIRE(page1.merge(page2));
    REQUIRE(page1.itemCount() == 10);
    REQUIRE(page1.nextPage() == 3);
  }

  SECTION("node occupancy checks") {
    IndexPage page(1, true);

    // Verify initial state.
    REQUIRE_FALSE(page.isOverflow());

    // Fill to minimum entries.
    const auto minEntries = IndexPage::minEntries();
    for (uint64_t i = 0; i <= minEntries; ++i) {
      REQUIRE(page.insertKey(i * 10, i * 100));
    }

    // Verify minimum occupancy state.
    REQUIRE_FALSE(page.isOverflow());
    REQUIRE_FALSE(page.isUnderflow());

    // Fill to maximum entries.
    for (uint64_t i = minEntries + 1; i < IndexPage::maxEntries(); ++i) {
      REQUIRE(page.insertKey(i * 10, i * 100));
    }

    // Verify maximum occupancy state.
    REQUIRE(page.isOverflow());
    REQUIRE_FALSE(page.isUnderflow());
  }
}

TEST_CASE("IndexPage sibling management", "[storage][index_page]") {
  IndexPage page(1, true);

  SECTION("next sibling") {
    page.setNextPage(2);
    REQUIRE(page.nextPage() == 2);
  }

  SECTION("previous sibling") {
    page.setPrevPage(3);
    REQUIRE(page.prevPage() == 3);
  }

  SECTION("parent node") {
    page.setParentPage(4);
    REQUIRE(page.parentPage() == 4);
  }
}

TEST_CASE("IndexPage capacity management", "[storage][index_page]") {
  SECTION("entry limits") {
    REQUIRE(IndexPage::maxEntries() == IndexPage::MAX_FREE_SPACE / sizeof(IndexEntry));
    REQUIRE(IndexPage::minEntries() == IndexPage::maxEntries() / 2);
  }

  SECTION("overflow handling") {
    IndexPage page(1, true);

    // Fill to maximum capacity.
    for (uint64_t i = 0; i < IndexPage::maxEntries(); ++i) {
      REQUIRE(page.insertKey(i * 10, i * 100));
    }

    // Verify overflow handling.
    REQUIRE_FALSE(page.insertKey(999, 9999));
  }
}
