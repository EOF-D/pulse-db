#include "pulsedb/utils/logger.hpp"

using namespace pulse::utils;

int main() {
  logging::setLevel(LogLevel::ERROR);
  const Logger logger("main");

  logger.info("foo");
  logger.debug("bar");
  logger.warn("baz");
  logger.error("qux");
  return 0;
}
