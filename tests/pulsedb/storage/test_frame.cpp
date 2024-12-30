/**
 * @file tests/pulsedb/storage/test_frame.cpp
 * @brief Test cases for Frame class.
 */

#include "pulsedb/storage/data_page.hpp"
#include "pulsedb/storage/frame.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace pulse::storage;

TEST_CASE("Frame basic operations", "[storage][frame]") {
  SECTION("constructor") {
    Frame frame;

    REQUIRE(frame.id() == 0);
    REQUIRE(frame.pins() == 0);
    REQUIRE_FALSE(frame.isDirty());
    REQUIRE(frame.getPage() == nullptr);
    REQUIRE(frame.isUnpinned());
  }

  SECTION("reset with page") {
    Frame frame;
    auto page = std::make_unique<DataPage>(1);
    auto *pagePtr = page.get();

    frame.reset(std::move(page));
    REQUIRE(frame.id() == 1);
    REQUIRE(frame.pins() == 0);
    REQUIRE_FALSE(frame.isDirty());
    REQUIRE(frame.getPage() == pagePtr);
    REQUIRE(frame.isUnpinned());
  }
}

TEST_CASE("Frame pin operations", "[storage][frame]") {
  Frame frame;

  SECTION("pin and unpin") {
    REQUIRE(frame.pin() == 1);
    REQUIRE(frame.pins() == 1);
    REQUIRE_FALSE(frame.isUnpinned());

    REQUIRE(frame.pin() == 2);
    REQUIRE(frame.pins() == 2);

    REQUIRE(frame.unpin() == 1);
    REQUIRE(frame.pins() == 1);
    REQUIRE_FALSE(frame.isUnpinned());

    REQUIRE(frame.unpin() == 0);
    REQUIRE(frame.pins() == 0);
    REQUIRE(frame.isUnpinned());
  }

  SECTION("unpin at zero") {
    REQUIRE(frame.unpin() == 0);
    REQUIRE(frame.pins() == 0);
  }
}

TEST_CASE("Frame dirty flag operations", "[storage][frame]") {
  Frame frame;

  SECTION("mark and unmark") {
    REQUIRE_FALSE(frame.isDirty());

    frame.mark();
    REQUIRE(frame.isDirty());

    frame.unmark();
    REQUIRE_FALSE(frame.isDirty());
  }
}
