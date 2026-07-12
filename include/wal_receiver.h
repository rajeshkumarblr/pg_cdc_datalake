#pragma once

#include "config.h"
#include "metrics.h"
#include "ring_buffer.h"
#include "types.h"

#include <atomic>
#include <chrono>
#include <cstdint>

#include <libpq-fe.h>

class LogicalDecoder;

/*
 * WAL Receiver — Thread 1 of the pipeline.
 *
 * Connects to PostgreSQL in replication mode via libpq, starts streaming
 * logical replication using pgoutput, decodes the binary protocol, and
 * pushes CDCRow objects into the ring buffer for the Parquet writer.
 *
 * Handles:
 *   - Replication connection setup and START_REPLICATION command
 *   - XLogData ('w') message extraction
 *   - Keepalive ('k') response with standby status updates
 *   - Exponential backoff reconnection on connection loss
 *   - Graceful shutdown via the shutdown flag
 */
class WalReceiver {
public:
    WalReceiver(const Config& config,
                RingBuffer<CDCRow>& ring_buffer,
                Metrics& metrics,
                std::atomic<bool>& shutdown_flag,
                uint64_t start_lsn,
                class SnapshotManager* snapshot_manager = nullptr);

    /*
     * Main loop — runs on its own thread.
     * Connects, streams, pushes rows to ring buffer.
     * Returns when shutdown_flag is set or unrecoverable error.
     */
    void run();

    /*
     * Get the last LSN successfully committed by the upstream transaction.
     * Used by the Parquet writer to report confirmed flush position.
     */
    uint64_t get_last_commit_lsn() const { return last_commit_lsn_.load(); }

private:
    /*
     * Establish the replication connection and send START_REPLICATION.
     * Returns true on success (and handles the stream), false on failure.
     */
    bool connect_and_start();

    /* Establish the libpq connection in logical replication mode */
    PGconn* connect_replication();

    /* Send the START_REPLICATION command */
    bool start_replication(PGconn* conn);

    /* Process replication stream data in a loop until error or shutdown. */
    void stream_loop(PGconn* conn, LogicalDecoder& parser);

    /* Process all available CopyData messages from the socket */
    int process_copy_data(PGconn* conn, LogicalDecoder& parser, 
                          uint64_t& last_received_lsn, 
                          std::chrono::time_point<std::chrono::steady_clock>& last_status_time);

    /* Send a standby status update */
    bool send_standby_status(PGconn* conn, uint64_t received_lsn, uint64_t flushed_lsn);

    const Config&           config_;
    RingBuffer<CDCRow>&     ring_buffer_;
    Metrics&                metrics_;
    std::atomic<bool>&      shutdown_flag_;
    uint64_t                start_lsn_;
    std::atomic<uint64_t>   last_commit_lsn_;
    class SnapshotManager*  snapshot_manager_;
};
