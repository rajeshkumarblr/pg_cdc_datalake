#pragma once

#include <cstdint>
#include <string>

/*
 * Persistent checkpoint state for crash recovery.
 * Tracks the last LSN that was successfully flushed to a Parquet file,
 * so that on restart we resume from that position in the WAL stream.
 */
struct Checkpoint {
    uint64_t    last_confirmed_lsn  = 0;
    std::string last_confirmed_time = "";
    uint64_t    files_written       = 0;
};

class CheckpointManager {
public:
    explicit CheckpointManager(const std::string& filepath)
        : filepath_(filepath) {}

    /*
     * Load checkpoint from a JSON file.
     * Returns a default (empty) checkpoint if the file doesn't exist.
     */
    Checkpoint load_checkpoint();

    /*
     * Save checkpoint to a JSON file atomically.
     * Writes to a .tmp file first, then renames to the target path.
     */
    void save_checkpoint(const Checkpoint& cp);

private:
    std::string filepath_;
};

/*
 * Convert a PG LSN uint64 to the "X/XXXXXXXX" string format.
 */
std::string lsn_to_string(uint64_t lsn);

/*
 * Parse a PG LSN string "X/XXXXXXXX" to uint64.
 */
uint64_t string_to_lsn(const std::string& s);
