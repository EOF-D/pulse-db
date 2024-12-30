/**
 * @file src/main.cpp
 * @brief Demonstrates the RDBMS interactively.
 */

#include "pulsedb/storage/data_page.hpp"
#include "pulsedb/storage/disk_manager.hpp"
#include "pulsedb/storage/index_page.hpp"
#include "pulsedb/utils/logger.hpp"

using namespace pulse;
namespace fs = std::filesystem;

enum class RecordType : uint16_t { STRING = 1 };

class DemoREPL {
public:
  DemoREPL(const std::string &path) : logger("repl"), dm(path, !fs::exists(path)), rootPageId(0) {
    utils::logging::setLevel(utils::LogLevel::NONE);

    // Create initial root index page.
    if (dm.pageCount() == 0) {
      rootPageId = dm.allocatePage();
      dm.flushPage(storage::IndexPage(rootPageId, true));
    }

    logger.info("initialized demo repl");
  }

  void start() {
    std::string line;
    std::cout << "commands: read <key>, write <key> <value>, delete <key>, flush, exit\n\n";

    while (true) {
      std::cout << "pulse-db> ";
      if (!std::getline(std::cin, line) || line == "exit") {
        break;
      }

      run(line);
      std::cout << std::endl;
    }
  }

  void run(const std::string &line) {
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;

    if (cmd == "read") {
      uint32_t key;

      if (iss >> key) {
        read(key);
      }
    }

    else if (cmd == "write") {
      uint32_t key;
      std::string value;

      if (iss >> key && std::getline(iss, value)) {
        write(key, value.substr(1)); // Skip the leading space.
      }
    }

    else if (cmd == "delete") {
      uint32_t key;

      if (iss >> key) {
        remove(key);
      }
    }

    else if (cmd == "flush")
      flush();

    else {
      logger.error("unknown command: {}", cmd);
    }
  }

private:
  void read(uint32_t key) {
    auto rootPage = dm.fetchPage(rootPageId);
    if (!rootPage) {
      logger.error("failed to fetch root page");
      return;
    }

    auto *indexPage = static_cast<storage::IndexPage *>(rootPage.get());
    auto dataPageId = indexPage->lookup(key);
    if (!dataPageId) {
      logger.error("key not found: {}", key);
      return;
    }

    auto dataPage = dm.fetchPage(*dataPageId);
    if (!dataPage) {
      logger.error("failed to fetch data page");
      return;
    }

    auto *page = static_cast<storage::DataPage *>(dataPage.get());

    // Get slotId from key first.
    auto slotId = page->getSlotId(key);
    if (!slotId) {
      logger.error("failed to find slot for key");
      return;
    }

    auto record = page->getRecord(*slotId);
    if (!record) {
      logger.error("failed to read record");
      return;
    }

    const char *str = static_cast<const char *>(record->first);
    std::cout << "-> key " << key << " = \"" << str << "\"" << std::endl;
  }

  void write(uint32_t key, const std::string &value) {
    auto rootPage = dm.fetchPage(rootPageId);
    if (!rootPage) {
      logger.error("failed to fetch root page");
      return;
    }

    uint32_t dataPageId = dm.allocatePage();
    auto dataPage = std::make_unique<storage::DataPage>(dataPageId);

    // Insert the record into the data page.
    auto slotId = dataPage->insertRecord(
        key, value.c_str(), value.length() + 1, static_cast<uint16_t>(RecordType::STRING)
    );

    if (!slotId) {
      logger.error("failed to insert record");
      return;
    }

    auto *indexPage = static_cast<storage::IndexPage *>(rootPage.get());
    if (!indexPage->insertKey(key, dataPageId)) {
      logger.error("failed to insert index entry");
      return;
    }

    dm.flushPage(*indexPage);
    dm.flushPage(*dataPage);

    std::cout << "-> wrote key " << key << " = \"" << value << "\"" << std::endl;
  }

  void remove(uint32_t key) {
    auto rootPage = dm.fetchPage(rootPageId);
    if (!rootPage) {
      logger.error("failed to fetch root page");
      return;
    }

    auto *indexPage = static_cast<storage::IndexPage *>(rootPage.get());
    auto dataPageId = indexPage->lookup(key);
    if (!dataPageId) {
      logger.error("key not found: {}", key);
      return;
    }

    auto dataPage = dm.fetchPage(*dataPageId);
    if (!dataPage) {
      logger.error("failed to fetch data page");
      return;
    }

    auto *page = static_cast<storage::DataPage *>(dataPage.get());

    // Get and delete the record using the key.
    auto slotId = page->getSlotId(key);
    if (!slotId || !page->deleteRecord(*slotId)) {
      logger.error("failed to delete record for key: {}", key);
      return;
    }

    if (!indexPage->removeKey(key)) {
      logger.error("failed to remove key from index: {}", key);
      return;
    }

    dm.flushPage(*indexPage);
    dm.flushPage(*page);
    std::cout << "-> removed key " << key << std::endl;
  }

  void flush() {
    if (dm.sync()) {
      std::cout << "flushed all pages to disk" << std::endl;
    } else {
      std::cout << "failed to flush pages" << std::endl;
    }
  }

  uint32_t rootPageId;     /**< Root index page. */
  storage::DiskManager dm; /**< Disk manager instance. */
  utils::Logger logger;    /**< Logger instance. */
};

int main() {
  DemoREPL repl("test.db");
  repl.start();

  return 0;
}
