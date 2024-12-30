/**
 * @file include/pulsedb/utils/logger.hpp
 * @brief The Logger class used for logging messages.
 */

#ifndef PULSEDB_UTILS_LOGGER_HPP
#define PULSEDB_UTILS_LOGGER_HPP

#include <chrono>
#include <iostream>

/**
 * @namespace pulse::utils
 * @brief The namespace for utility functions.
 */
namespace pulse::utils {
  /**
   * @typedef LogData
   * @brief A pair of string views to represent log data.
   */
  using LogData = std::pair<std::string_view, std::string_view>;

  /**
   * @concept OutputStream
   * @brief Concept for an output stream.
   * @tparam T Type of the stream.
   */
  template <typename T>
  concept OutputStream = requires(T &stream, typename T::char_type c) {
    { stream << c } -> std::same_as<T &>;
  };

  /**
   * @enum LogLevel
   * @brief The different levels of logging.
   */
  enum class LogLevel : uint8_t {
    NONE,  /**< No logging. */
    INFO,  /**< Informational logging. */
    DEBUG, /**< Debug logging. */
    WARN,  /**< Warn logging. */
    ERROR  /**< Error logging. */
  };

  /**
   * @namespace pulsedb::utils::logging
   * @brief The namespace for global logging configuration.
   */
  namespace logging {
    /**
     * @brief The global log level.
     */
    inline LogLevel globalLevel = LogLevel::NONE;

    /**
     * @brief Sets the global log level.
     * @param level The LogLevel to set as global.
     */
    constexpr void setLevel(LogLevel level) noexcept { globalLevel = level; }

    /**
     * @brief Get the global log level.
     * @return The global log level.
     */
    [[nodiscard]] inline LogLevel getLevel() noexcept { return globalLevel; }
  } // namespace logging
} // namespace pulse::utils

/**
 * @namespace pulse::utils
 * @brief The namespace for utility functions.
 */
namespace pulse::utils {
  /**
   * @class Logger
   * @brief Handles logging messages with different levels.
   */
  class Logger {
  public:
    /**
     * @brief Construct a new logger object.
     * @param name The name of the logger.
     * @note This constructor uses the default output stream std::cout.
     */
    explicit constexpr Logger(std::string_view name) noexcept : name(name), output(std::cout) {}

    /**
     * @brief Construct a new Logger object.
     * @tparam Stream The output stream type.
     * @param name The name of the logger.
     * @param stream The output stream.
     * @note
     *  This constructor is only available if the output stream type
     *  supports the `<<` operator.
     */
    template <OutputStream Stream>
    explicit constexpr Logger(const std::string &name, Stream &stream) noexcept
        : name(name), output(stream) {}

    /**
     * @brief Log a message with INFO level.
     * @tparam Args The argument types.
     * @param fmt The format string.
     * @param args The arguments to format.
     */
    template <typename... Args> void info(std::format_string<Args...> fmt, Args &&...args) const {
      log(LogLevel::INFO, fmt, std::forward<Args>(args)...);
    }

    /**
     * @brief Log a message with DEBUG level.
     * @tparam Args The argument types.
     * @param fmt The format string.
     * @param args The arguments to format.
     */
    template <typename... Args> void debug(std::format_string<Args...> fmt, Args &&...args) const {
      log(LogLevel::DEBUG, fmt, std::forward<Args>(args)...);
    }

    /**
     * @brief Log a message with WARN level.
     * @tparam Args The argument types.
     * @param fmt The format string.
     * @param args The arguments to format.
     */
    template <typename... Args> void warn(std::format_string<Args...> fmt, Args &&...args) const {
      log(LogLevel::WARN, fmt, std::forward<Args>(args)...);
    }

    /**
     * @brief Log a message with ERROR level.
     * @tparam Args The argument types.
     * @param fmt The format string.
     * @param args The arguments to format.
     */
    template <typename... Args> void error(std::format_string<Args...> fmt, Args &&...args) const {
      log(LogLevel::ERROR, fmt, std::forward<Args>(args)...);
    }

  private:
    /**
     * @brief Log a message with a specific log level.
     * @tparam Args The argument types.
     * @param level The log level.
     * @param fmt The format string.
     * @param args The arguments to format.
     */
    // clang-format off
    template <typename... Args>
    void log(LogLevel level, std::format_string<Args...> fmt, Args &&...args) const {
      // If the global log level is less than the specified level, return.
      if (logging::getLevel() < level)
        return;

      // Get the current timestamp & format.
      const auto time = std::chrono::current_zone()->to_local(
          std::chrono::system_clock::now());

      const std::string timestamp =
          std::format("{:%Y-%m-%d %H:%M:%S}",
                      std::chrono::floor<std::chrono::seconds>(time));

      const auto &[code, prefix] = LEVEL_DATA[static_cast<uint8_t>(level)];
      
      // Send the formatted log message to the output stream.
      output << std::format(
        "[{}]{}[{}:{}]: \x1B[0m{}\n",
        timestamp,
        code,
        name,
        prefix,
        std::format(fmt, std::forward<Args>(args)...)
      );
      // clang-format on
    }

    /**
     * @brief The data for each log level.
     */
    static constexpr std::array<LogData, 5> LEVEL_DATA = {
        {{"\x1B[0m", "NONE"},
         {"\x1B[0;32m", "INFO"},
         {"\x1B[38;5;214m", "DEBUG"},
         {"\x1B[0;33m", "WARN"},
         {"\x1B[0;31m", "ERROR"}}};

    std::string_view name; /**< The name of the logger. */
    std::ostream &output;  /**< The output stream for the logger. */
  };
} // namespace pulse::utils

#endif // PULSEDB_UTILS_LOGGER_HPP
