#include "pulsedb/storage/page_heap.hpp"
#include "pulsedb/utils/logger.hpp"

using namespace pulse;

int main() {
  utils::logging::setLevel(utils::LogLevel::DEBUG);
  const utils::Logger logger("main");

  storage::PageHeap heap(5);
  logger.debug("created heap w/ capacity {}", 5);

  // Insert pages.
  const uint32_t testPages[] = {2, 1, 4, 5, 3};
  for (uint32_t pageId : testPages) {
    heap.insert(pageId);
    logger.info("inserted page {}", pageId);
  }

  logger.debug("heap size: {}", heap.size());

  // Extract and verify minimum.
  while (!heap.empty()) {
    uint32_t minPage = heap.minimum();
    uint32_t extracted = heap.extractMin();

    if (minPage != extracted) {
      logger.error("min mismatch! min() = {}, extractMin() = {}", minPage, extracted);
      return 1;
    }

    logger.info("extracted min page {}", extracted);
  }

  // Verify heap is empty.
  if (!heap.empty()) {
    logger.error("expected heap to be empty");
    return 1;
  }

  logger.info("heap is empty");
  return 0;
}
