#include "pulsedb/storage/page.hpp"
#include "pulsedb/utils/logger.hpp"
#include <string>

using namespace pulse;

int main() {
  // Set up logging.
  utils::logging::setLevel(utils::LogLevel::ERROR);
  const utils::Logger logger("main");

  storage::Page page(1);
  logger.debug("created page: ID {}", page.pageId());

  // Write data.
  const char testData[] = "Hello, pulse-db!";
  const uint32_t dataSize = sizeof(testData) - 1; // Exclude null terminator.

  bool success = page.write(0, reinterpret_cast<const uint8_t *>(testData), dataSize);
  if (success) {
    logger.info("wrote {} bytes", dataSize);
  }

  else {
    logger.error("failed to write!");
    return 1;
  }

  // Read data.
  char buffer[dataSize];
  uint32_t bytesRead = page.read(0, buffer, dataSize);
  logger.info("read {} bytes", bytesRead);

  if (bytesRead == dataSize) {
    std::string readData(reinterpret_cast<char *>(buffer), bytesRead);
    logger.info("page {}: {}", page.pageId(), readData);

    // Verify data.
    if (readData != testData) {
      logger.error("data mismatch!");
      return 1;
    }
  }

  else {
    logger.error("bytes read mismatch! expected {} but got {}", sizeof(testData) - 1, bytesRead);
    return 1;
  }
}
