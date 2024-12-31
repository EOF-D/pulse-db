/**
 * @file tests/pulsedb/storage/test_lru_replacer.cpp
 * @brief Test cases for LRUReplacer class.
 */

#include "pulsedb/storage/replacer.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("LRUReplacer basic operations", "[storage][replacer][lru]") {
  LRUReplacer replacer;

  SECTION("initial state") {
    // Should return nullopt when empty.
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("single frame") {
    replacer.unpin(1);
    auto victim = replacer.victim();

    REQUIRE(victim);
    REQUIRE(*victim == 1);
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("pin unpinned frame") {
    replacer.unpin(1);
    replacer.pin(1);
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("pin non-existent frame") {
    replacer.pin(1);
    REQUIRE_FALSE(replacer.victim());
  }
}

TEST_CASE("LRUReplacer LRU ordering", "[storage][replacer][lru]") {
  LRUReplacer replacer;

  SECTION("basic LRU order") {
    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);

    REQUIRE(*replacer.victim() == 1);
    REQUIRE(*replacer.victim() == 2);
    REQUIRE(*replacer.victim() == 3);
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("reorder with access") {
    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);
    replacer.unpin(1);

    REQUIRE(*replacer.victim() == 2);
    REQUIRE(*replacer.victim() == 3);
    REQUIRE(*replacer.victim() == 1);
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("multiple reorders") {
    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);

    replacer.unpin(2);
    replacer.unpin(1);

    REQUIRE(*replacer.victim() == 3);
    REQUIRE(*replacer.victim() == 2);
    REQUIRE(*replacer.victim() == 1);
  }
}

TEST_CASE("LRUReplacer concurrent frame handling", "[storage][replacer][lru]") {
  LRUReplacer replacer;

  SECTION("duplicate unpins") {
    replacer.unpin(1);
    replacer.unpin(1);
    replacer.unpin(1);

    REQUIRE(*replacer.victim() == 1);
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("pin while present multiple times") {
    replacer.unpin(1);
    replacer.unpin(1);
    replacer.pin(1);

    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("mixed operations") {
    replacer.unpin(1);
    replacer.unpin(2);
    replacer.pin(1);
    replacer.unpin(3);
    replacer.unpin(1);

    REQUIRE(*replacer.victim() == 2);
    REQUIRE(*replacer.victim() == 3);
    REQUIRE(*replacer.victim() == 1);
    REQUIRE_FALSE(replacer.victim());
  }
}

TEST_CASE("LRUReplacer edge cases", "[storage][replacer]") {
  LRUReplacer replacer;

  SECTION("large frame ids") {
    size_t largeId = std::numeric_limits<size_t>::max();
    replacer.unpin(largeId);

    REQUIRE(*replacer.victim() == largeId);
  }

  SECTION("alternating pin/unpin") {
    replacer.unpin(1);
    replacer.pin(1);
    replacer.unpin(1);
    replacer.pin(1);
    replacer.unpin(1);

    REQUIRE(*replacer.victim() == 1);
    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("pin after victim") {
    replacer.unpin(1);
    auto victim = replacer.victim();
    replacer.pin(1);

    REQUIRE_FALSE(replacer.victim());
  }
}

TEST_CASE("LRUReplacer stress test", "[storage][replacer]") {
  LRUReplacer replacer;

  SECTION("many frames") {
    const size_t numFrames = 1000;
    for (size_t i = 0; i < numFrames; i++) {
      replacer.unpin(i);
    }

    // Verify all frames come out in order.
    for (size_t i = 0; i < numFrames; i++) {
      auto victim = replacer.victim();
      REQUIRE(victim);
      REQUIRE(*victim == i);
    }

    REQUIRE_FALSE(replacer.victim());
  }

  SECTION("repeated operations") {
    const size_t iterations = 1000;
    const size_t numFrames = 5;

    // Repeatedly add and remove frames.
    for (size_t i = 0; i < iterations; i++) {
      replacer.unpin(i % numFrames);

      // Pin every third frame.
      if (i % 3 == 0) {
        replacer.pin(i % numFrames);
      }
    }

    // Verify remaining frames can be removed.
    while (auto victim = replacer.victim()) {
      REQUIRE(*victim < numFrames);
    }
  }
}
