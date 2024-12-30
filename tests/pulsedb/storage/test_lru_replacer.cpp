/**
 * @file tests/pulsedb/storage/test_lru_replacer.cpp
 * @brief Test cases for LRUReplacer class.
 */

#include "pulsedb/storage/replacer.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("LRUReplacer basic operations", "[storage][replacer]") {
  SECTION("constructor") {
    LRUReplacer replacer(5);
    REQUIRE(replacer.size() == 0);
  }

  SECTION("empty state") {
    LRUReplacer replacer(5);
    auto victim = replacer.victim();
    REQUIRE_FALSE(victim);
  }
}

TEST_CASE("LRUReplacer record and victim operations", "[storage][replacer]") {
  LRUReplacer replacer(5);

  SECTION("basic record and victim") {
    replacer.recordAccess(1);
    replacer.recordAccess(2);
    replacer.recordAccess(3);

    REQUIRE(replacer.size() == 3);

    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 1);
    REQUIRE(replacer.size() == 2);
  }

  SECTION("LRU order") {
    replacer.recordAccess(1);
    replacer.recordAccess(2);
    replacer.recordAccess(3);
    replacer.recordAccess(1);

    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 2);
  }

  SECTION("capacity handling") {
    // Fill beyond capacity.
    for (size_t i = 0; i < 7; i++) {
      replacer.recordAccess(i);
    }

    REQUIRE(replacer.size() == 5);

    // Verify oldest entries were removed.
    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 2); // First two entries should have been evicted.
  }
}

TEST_CASE("LRUReplacer remove operations", "[storage][replacer]") {
  LRUReplacer replacer(5);

  SECTION("remove from middle") {
    replacer.recordAccess(1);
    replacer.recordAccess(2);
    replacer.recordAccess(3);

    replacer.remove(2);
    REQUIRE(replacer.size() == 2);

    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 1);
  }

  SECTION("remove non-existent") {
    replacer.recordAccess(1);
    replacer.recordAccess(2);

    replacer.remove(3);
    REQUIRE(replacer.size() == 2);

    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 1);
  }

  SECTION("remove and reinsert") {
    replacer.recordAccess(1);
    replacer.recordAccess(2);

    replacer.remove(1);
    replacer.recordAccess(1);

    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 2);
  }
}

TEST_CASE("LRUReplacer concurrent operations", "[storage][replacer]") {
  LRUReplacer replacer(5);

  SECTION("multiple accesses same frame") {
    replacer.recordAccess(1);
    replacer.recordAccess(1);
    replacer.recordAccess(1);

    REQUIRE(replacer.size() == 1);

    auto victim = replacer.victim();
    REQUIRE(victim);
    REQUIRE(*victim == 1);
    REQUIRE(replacer.size() == 0);
  }
}
