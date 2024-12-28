#include "pulsedb/storage/page_heap.hpp"
#include <catch2/catch_test_macros.hpp>
#include <vector>

using namespace pulse::storage;

TEST_CASE("PageHeap operations", "[storage][heap]") {
  SECTION("Creation") {
    PageHeap heap;

    REQUIRE(heap.empty());
    REQUIRE(heap.size() == 0);
  }

  SECTION("Insert & extract") {
    PageHeap heap(2);

    heap.insert(2);
    heap.insert(1);

    REQUIRE(heap.size() == 2);
    REQUIRE(heap.minimum() == 1);
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.extractMin() == 2);
    REQUIRE(heap.empty());
  }
}

TEST_CASE("PageHeap min-heap property", "[storage][heap]") {
  PageHeap heap(5);

  SECTION("Insert in random order") {
    // Insert out of order.
    heap.insert(3);
    heap.insert(2);
    heap.insert(4);
    heap.insert(1);
    heap.insert(5);

    // Verify extract is in sorted order.
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.extractMin() == 2);
    REQUIRE(heap.extractMin() == 3);
    REQUIRE(heap.extractMin() == 4);
    REQUIRE(heap.extractMin() == 5);
    REQUIRE(heap.empty());
  }

  SECTION("Insert in reverse order") {
    std::vector<uint32_t> values = {10, 9, 8, 7, 6};
    for (auto value : values) {
      heap.insert(value);
    }

    // Extract and verify ascending order.
    for (size_t i = 6; i <= 10; ++i) {
      REQUIRE(heap.extractMin() == i);
    }
  }
}

TEST_CASE("PageHeap capacity", "[storage][heap]") {
  SECTION("Grow beyond initial capacity") {
    PageHeap heap(1);

    // Insert beyond initial capacity.
    for (uint32_t i = 0; i < 5; ++i) {
      heap.insert(i);
    }

    REQUIRE(heap.size() == 5);

    // Verify contents.
    for (uint32_t i = 0; i < 5; ++i) {
      REQUIRE(heap.extractMin() == i);
    }
  }
}

TEST_CASE("PageHeap edge cases", "[storage][heap]") {
  PageHeap heap(3);

  SECTION("Duplicate values") {
    heap.insert(1);
    heap.insert(1);
    heap.insert(1);

    REQUIRE(heap.size() == 3);
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.empty());
  }

  SECTION("Single element") {
    heap.insert(1);
    REQUIRE(heap.minimum() == 1);
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.empty());
  }

  SECTION("Extract from empty") {
    heap.insert(1);
    REQUIRE(heap.extractMin() == 1);
    REQUIRE(heap.empty());

    // Insert after empty.
    heap.insert(2);
    REQUIRE(heap.minimum() == 2);
  }
}
