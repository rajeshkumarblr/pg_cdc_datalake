#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

struct Metrics {
    std::atomic<uint64_t> rows_ingested{0};
    std::atomic<uint64_t> files_written{0};
    std::atomic<uint64_t> bytes_processed{0};
    std::atomic<uint64_t> current_lsn{0};
    
    std::chrono::time_point<std::chrono::steady_clock> start_time = std::chrono::steady_clock::now();

    int64_t uptime_seconds() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
    }
};
