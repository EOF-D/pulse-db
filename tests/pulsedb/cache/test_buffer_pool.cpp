/**
 * @file tests/pulsedb/cache/test_buffer_pool.cpp
 * @brief Test cases for BufferPool class.
 */

#include "pulsedb/cache/buffer_pool.hpp"
#include "pulsedb/storage/index_page.hpp"
#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <random>
#include <thread>

using namespace pulse::storage;
using namespace pulse::cache;
namespace fs = std::filesystem;

const fs::path testPath = "test.db";
const size_t poolSize = 10;

void cleanup() {
  if (fs::exists(testPath)) {
    fs::remove(testPath);
  }
}

TEST_CASE("BufferPool basic operations", "[cache][buffer_pool]") {
  cleanup();
  DiskManager dm(testPath, true);
  BufferPool pool(dm, poolSize);

  SECTION("constructor") { REQUIRE(pool.size() == 0); }

  SECTION("create and fetch page") {
    auto *page = pool.createPage(PageType::DATA);
    REQUIRE(page != nullptr);
    REQUIRE(page->type() == PageType::DATA);

    auto *fetchedPage = pool.fetchPage(page->id());
    REQUIRE(fetchedPage != nullptr);
    REQUIRE(fetchedPage == page);
    REQUIRE(pool.size() == 1);

    pool.unpinPage(page->id(), false);
    pool.unpinPage(page->id(), false);
  }

  SECTION("create different page types") {
    auto *dataPage = pool.createPage(PageType::DATA);
    REQUIRE(dataPage != nullptr);
    REQUIRE(dataPage->type() == PageType::DATA);

    auto *leafPage = pool.createPage(PageType::INDEX, true, 0);
    REQUIRE(leafPage != nullptr);
    REQUIRE(leafPage->type() == PageType::INDEX);

    auto *leafIndexPage = static_cast<IndexPage *>(leafPage);
    REQUIRE(leafIndexPage->isLeaf() == true);
    REQUIRE(leafIndexPage->level() == 0);

    auto *internalPage = pool.createPage(PageType::INDEX, false, 1);
    REQUIRE(internalPage != nullptr);
    REQUIRE(internalPage->type() == PageType::INDEX);

    auto *internalIndexPage = static_cast<IndexPage *>(internalPage);
    REQUIRE(internalIndexPage->isLeaf() == false);
    REQUIRE(internalIndexPage->level() == 1);

    pool.unpinPage(dataPage->id(), false);
    pool.unpinPage(leafPage->id(), false);
    pool.unpinPage(internalPage->id(), false);
  }

  cleanup();
}

TEST_CASE("BufferPool page management", "[cache][buffer_pool]") {
  cleanup();
  DiskManager dm(testPath, true);
  BufferPool pool(dm, poolSize);

  SECTION("unpin and delete") {
    auto *page = pool.createPage(PageType::DATA);
    REQUIRE(page != nullptr);

    uint32_t pageId = page->id();

    REQUIRE_FALSE(pool.deletePage(pageId));
    REQUIRE(pool.unpinPage(pageId, false));
    REQUIRE(pool.deletePage(pageId));
    REQUIRE(pool.fetchPage(pageId) == nullptr);
  }

  SECTION("dirty page handling") {
    auto *page = pool.createPage(PageType::DATA);

    REQUIRE(page != nullptr);
    REQUIRE(pool.unpinPage(page->id(), true));
    REQUIRE(pool.flushPage(page->id()));
    REQUIRE(pool.deletePage(page->id()));
  }

  cleanup();
}

TEST_CASE("BufferPool replacement policy", "[cache][buffer_pool]") {
  cleanup();
  DiskManager dm(testPath, true);
  BufferPool pool(dm, poolSize);

  SECTION("page replacement") {
    std::vector<Page *> pages;

    for (size_t i = 0; i < poolSize; i++) {
      auto *page = pool.createPage(PageType::DATA);
      REQUIRE(page != nullptr);

      pages.push_back(page);
    }

    for (auto *page : pages) {
      REQUIRE(pool.unpinPage(page->id(), false));
    }

    // Create one more page to trigger replacement.
    auto *newPage = pool.createPage(PageType::DATA);
    REQUIRE(newPage != nullptr);

    for (auto *page : pages) {
      auto *fetched = pool.fetchPage(page->id());
      REQUIRE(fetched != nullptr);

      pool.unpinPage(fetched->id(), false);
    }

    pool.unpinPage(newPage->id(), false);
  }

  SECTION("no replacement of pinned pages") {
    for (size_t i = 0; i < poolSize; i++) {
      REQUIRE(pool.createPage(PageType::DATA) != nullptr);
    }

    REQUIRE(pool.createPage(PageType::DATA) == nullptr);
  }

  cleanup();
}

TEST_CASE("BufferPool concurrent access", "[cache][buffer_pool]") {
  cleanup();
  DiskManager dm(testPath, true);
  BufferPool pool(dm, poolSize);

  SECTION("parallel fetches") {
    auto *page = pool.createPage(PageType::DATA);
    REQUIRE(page != nullptr);

    uint32_t pageId = page->id();
    pool.unpinPage(pageId, false);

    const size_t numThreads = 10;
    std::vector<std::thread> threads;

    for (size_t i = 0; i < numThreads; i++) {
      threads.emplace_back([&pool, pageId]() {
        auto *fetchedPage = pool.fetchPage(pageId);
        REQUIRE(fetchedPage != nullptr);

        pool.unpinPage(pageId, false);
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }
  }

  SECTION("parallel creates and fetches") {
    const size_t numThreads = 5;
    std::vector<std::thread> threads;
    std::vector<uint32_t> pageIds(numThreads);

    for (size_t i = 0; i < numThreads; i++) {
      threads.emplace_back([&pool, &pageIds, i]() {
        auto *page = pool.createPage(PageType::DATA);
        REQUIRE(page != nullptr);

        pageIds[i] = page->id();
        pool.unpinPage(pageIds[i], false);
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    threads.clear();

    for (size_t i = 0; i < numThreads; i++) {
      threads.emplace_back([&pool, &pageIds, i]() {
        auto *page = pool.fetchPage(pageIds[i]);
        REQUIRE(page != nullptr);

        pool.unpinPage(pageIds[i], false);
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }
  }

  cleanup();
}

TEST_CASE("BufferPool edge cases", "[cache][buffer_pool]") {
  cleanup();
  DiskManager dm(testPath, true);
  BufferPool pool(dm, poolSize);

  SECTION("fetch non-existent page") { REQUIRE(pool.fetchPage(1000) == nullptr); }

  SECTION("invalid page operations") {
    REQUIRE_FALSE(pool.unpinPage(1000, false));
    REQUIRE_FALSE(pool.flushPage(1000));
    REQUIRE_FALSE(pool.deletePage(1000));
  }

  SECTION("create invalid page type") { REQUIRE(pool.createPage(PageType::INVALID) == nullptr); }

  SECTION("double unpin") {
    auto *page = pool.createPage(PageType::DATA);
    REQUIRE(page != nullptr);

    REQUIRE(pool.unpinPage(page->id(), false));
    REQUIRE(pool.unpinPage(page->id(), false));
  }

  SECTION("flush empty pool") {
    pool.flushAll();
    REQUIRE(pool.size() == 0);
  }

  cleanup();
}
