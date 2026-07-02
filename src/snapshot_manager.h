#pragma once

#include "config.h"
#include "ring_buffer.h"
#include "types.h"
#include <string>
#include <vector>
#include <atomic>

class SnapshotManager {
public:
    SnapshotManager(const Config& config, RingBuffer<CDCRow>& ring_buffer, std::atomic<bool>& shutdown_flag);

    /*
     * Perform the initial snapshot export for all tracked tables.
     * Uses the provided snapshot_name created by the replication connection.
     * Blocks until all tables are exported.
     */
    void export_snapshot(const std::string& snapshot_name, uint64_t start_lsn);

private:
    void export_table(const std::string& snapshot_name, const std::string& table_name, uint64_t start_lsn);
    TableSchema fetch_schema(const std::string& table_name);

    const Config& config_;
    RingBuffer<CDCRow>& ring_buffer_;
    std::atomic<bool>& shutdown_flag_;
};
