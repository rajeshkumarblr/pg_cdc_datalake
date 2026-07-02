#pragma once

#include "checkpoint.h"
#include "config.h"
#include "ring_buffer.h"
#include "types.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>

/*
 * Parquet Writer — Thread 2 of the pipeline.
 *
 * Consumes CDCRow objects from the ring buffer, accumulates them in
 * Apache Arrow columnar arrays, and writes them as Parquet files when
 * the size or time flush threshold is reached.
 *
 * Key behaviors:
 *   - Dynamically builds Arrow schemas from pgoutput Relation metadata
 *   - Per-table output directories (Spark-compatible partitioning)
 *   - Atomic file writes (write to .tmp, then rename to .parquet)
 *   - Embeds CDC metadata (LSN range, timestamps) in Parquet key-value metadata
 *   - Persists checkpoint after each successful file write
 */
class ParquetWriter {
public:
    ParquetWriter(const Config& config,
                  RingBuffer<CDCRow>& ring_buffer,
                  std::atomic<bool>& shutdown_flag,
                  CheckpointManager& checkpoint_manager,
                  Checkpoint& checkpoint);

    /*
     * Main loop — runs on its own thread.
     * Pops rows from ring buffer, accumulates, and flushes to Parquet files.
     * Returns when shutdown_flag is set (after a final flush of remaining data).
     */
    void run();

private:
    /*
     * Flush the currently accumulated rows for all tables to Parquet files.
     * Called when size or time threshold is reached, or on shutdown.
     */
    void flush_all();

    const Config&           config_;
    RingBuffer<CDCRow>&     ring_buffer_;
    std::atomic<bool>&      shutdown_flag_;
    CheckpointManager&      checkpoint_manager_;
    Checkpoint&             checkpoint_;
    std::string             output_dir_;
};
