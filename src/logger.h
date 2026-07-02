#pragma once

#include <string>

namespace Logger {
    /*
     * Initialize the logger.
     * If filepath is empty, it will only log to stdout (if log_to_console is true).
     */
    void init(const std::string& filepath, bool log_to_console);

    /*
     * Write an info message to the log.
     * Formats it as: [TIMESTAMP] [component] msg
     */
    void info(const std::string& component, const std::string& msg);

    /*
     * Close the log file.
     */
    void shutdown();
}
