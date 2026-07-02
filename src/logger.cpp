#include "logger.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>

namespace Logger {

    static std::ofstream g_log_file;
    static std::mutex g_mutex;
    static bool g_initialized = false;
    static bool g_log_to_console = true;

    void init(const std::string& filepath, bool log_to_console) {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (g_initialized) {
            return;
        }

        g_log_to_console = log_to_console;

        if (!filepath.empty()) {
            g_log_file.open(filepath, std::ios::app);
            if (!g_log_file.is_open()) {
                std::cerr << "WARNING: Could not open log file: " << filepath << std::endl;
            }
        }
        g_initialized = true;
    }

    void info(const std::string& component, const std::string& msg) {
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* tm = std::localtime(&now_c);
        char buffer[32];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm);

        std::ostringstream formatted;
        formatted << "[" << buffer << "] [" << component << "] " << msg;

        std::string final_msg = formatted.str();

        std::lock_guard<std::mutex> lock(g_mutex);
        if (g_log_to_console) {
            std::cout << final_msg << std::endl;
        }
        if (g_log_file.is_open()) {
            g_log_file << final_msg << "\n";
            g_log_file.flush();
        }
    }

    void shutdown() {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (g_log_file.is_open()) {
            g_log_file.close();
        }
        g_initialized = false;
    }

}
