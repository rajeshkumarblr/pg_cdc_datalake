#include "wal_receiver.h"
#include "checkpoint.h"
#include "logical_decoder.h"
#include "logger.h"
#include "snapshot_manager.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>

#include <libpq-fe.h>

/* For select() */
#include <sys/select.h>

#define TIMEOUT_SEC 10
#define FEEDBACK_INTERVAL_SEC 10

WalReceiver::WalReceiver(const Config& config,
                         RingBuffer<CDCRow>& ring_buffer,
                         std::atomic<bool>& shutdown_flag,
                         uint64_t start_lsn,
                         SnapshotManager* snapshot_manager)
    : config_(config),
      ring_buffer_(ring_buffer),
      shutdown_flag_(shutdown_flag),
      start_lsn_(start_lsn),
      last_commit_lsn_(start_lsn),
      snapshot_manager_(snapshot_manager) {}

void WalReceiver::run() {
    int backoff_sec = 1;
    constexpr int max_backoff_sec = 30;

    while (!shutdown_flag_.load()) {
        std::cout << "[receiver] Connecting to PostgreSQL..." << std::endl;

        if (connect_and_start()) {
            backoff_sec = 1;  /* Reset backoff on successful connection */
        }

        if (shutdown_flag_.load()) {
            break;
        }

        /* Connection lost — reconnect with exponential backoff */
        std::cerr << "[receiver] Connection lost. Retrying in "
                  << backoff_sec << "s..." << std::endl;
        for (int i = 0; i < backoff_sec && !shutdown_flag_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        backoff_sec = std::min(backoff_sec * 2, max_backoff_sec);
    }

    std::cout << "[receiver] Shutdown complete." << std::endl;
}

/*
 * Write a big-endian int64 into a buffer at the given offset.
 */
static void write_int64(char* buf, int offset, int64_t val) {
    for (int i = 7; i >= 0; --i) {
        buf[offset + i] = static_cast<char>(val & 0xFF);
        val >>= 8;
    }
}

/*
 * Send a standby status update to PostgreSQL via the replication connection.
 *
 * Message layout (34 bytes):
 *   byte  0:     'r' (standby status update)
 *   bytes 1-8:   received LSN
 *   bytes 9-16:  flushed LSN (last LSN safely written to Parquet)
 *   bytes 17-24: applied LSN (same as flushed for us)
 *   bytes 25-32: client system clock (microseconds since 2000-01-01)
 *   byte  33:    0 (no reply requested)
 */
bool WalReceiver::send_standby_status(PGconn* conn,
                                      uint64_t received_lsn,
                                      uint64_t flushed_lsn) {
    char buf[34];
    memset(buf, 0, sizeof(buf));

    buf[0] = 'r';
    write_int64(buf, 1, static_cast<int64_t>(received_lsn));
    write_int64(buf, 9, static_cast<int64_t>(flushed_lsn));
    write_int64(buf, 17, static_cast<int64_t>(flushed_lsn));

    /*
     * Client timestamp: microseconds since 2000-01-01 00:00:00 UTC.
     * PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01.
     * Difference is 946684800 seconds.
     */
    auto now = std::chrono::system_clock::now();
    auto since_epoch = now.time_since_epoch();
    int64_t unix_us = std::chrono::duration_cast<std::chrono::microseconds>(since_epoch).count();
    int64_t pg_us = unix_us - (946684800LL * 1000000LL);
    write_int64(buf, 25, pg_us);

    buf[33] = 0;  /* Don't request a reply */

    if (PQputCopyData(conn, buf, sizeof(buf)) <= 0) {
        std::cerr << "[receiver] Failed to send standby status: "
                  << PQerrorMessage(conn) << std::endl;
        return false;
    }
    if (PQflush(conn) < 0) {
        std::cerr << "[receiver] Failed to flush standby status: "
                  << PQerrorMessage(conn) << std::endl;
        return false;
    }

    return true;
}

bool WalReceiver::connect_and_start() {
    PGconn* conn = connect_replication();
    if (!conn) return false;

    if (!start_replication(conn)) {
        return false;
    }

    LogicalDecoder parser;
    parser.set_row_callback([this](CDCRow&& row) {
        Logger::info("WalReceiver", "Parsed and pushed row for table '" + row.table_name + "' to RingBuffer");
        ring_buffer_.push(std::move(row));
    });
    parser.set_commit_callback([this](uint64_t commit_lsn, int64_t /* commit_ts */) {
        last_commit_lsn_.store(commit_lsn);
    });

    stream_loop(conn, parser);
    
    return true;
}

PGconn* WalReceiver::connect_replication() {
    PGconn* conn = PQconnectdb(config_.replication_connection_string().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "[receiver] Connection failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return nullptr;
    }
    Logger::info("WalReceiver", "Connected in replication mode.");
    return conn;
}

bool WalReceiver::start_replication(PGconn* conn) {
    if (start_lsn_ == 0) {
        std::string create_slot_cmd = "CREATE_REPLICATION_SLOT " + config_.slot_name + " LOGICAL pgoutput EXPORT_SNAPSHOT";
        PGresult* res = PQexec(conn, create_slot_cmd.c_str());
        if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            std::string slot_name = PQgetvalue(res, 0, 0);
            std::string lsn_str = PQgetvalue(res, 0, 1);
            std::string snapshot_name = PQgetvalue(res, 0, 2);
            
            uint32_t hi, lo;
            if (sscanf(lsn_str.c_str(), "%X/%X", &hi, &lo) == 2) {
                start_lsn_ = (static_cast<uint64_t>(hi) << 32) | lo;
                last_commit_lsn_.store(start_lsn_);
                Logger::info("WalReceiver", "Created logical slot '" + slot_name + "' at LSN " + lsn_str + " with snapshot " + snapshot_name);
                
                if (snapshot_manager_) {
                    snapshot_manager_->export_snapshot(snapshot_name, start_lsn_);
                }
            }
        } else {
            std::string err = PQresultErrorMessage(res);
            if (err.find("already exists") == std::string::npos) {
                std::cerr << "[receiver] WARNING: CREATE_REPLICATION_SLOT failed: " << err << std::endl;
            } else {
                Logger::info("WalReceiver", "Slot " + config_.slot_name + " already exists. Starting from current position.");
            }
        }
        PQclear(res);
    }

    std::ostringstream cmd;
    cmd << "START_REPLICATION SLOT " << config_.slot_name
        << " LOGICAL " << lsn_to_string(start_lsn_)
        << " (proto_version '1', publication_names '"
        << config_.publication_name << "')";

    std::cout << "[receiver] " << cmd.str() << std::endl;

    PGresult* res = PQexec(conn, cmd.str().c_str());
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        std::cerr << "[receiver] START_REPLICATION failed: "
                  << PQresultErrorMessage(res) << std::endl;
        PQclear(res);
        PQfinish(conn);
        return false;
    }
    PQclear(res);

    Logger::info("WalReceiver", "Streaming started from LSN " + lsn_to_string(start_lsn_));
    return true;
}

void WalReceiver::stream_loop(PGconn* conn, LogicalDecoder& parser) {
    int socket_fd = PQsocket(conn);
    if (socket_fd < 0) {
        std::cerr << "[receiver] Cannot get socket fd" << std::endl;
        PQfinish(conn);
        return;
    }

    auto last_status_time = std::chrono::steady_clock::now();
    uint64_t last_received_lsn = start_lsn_;

    while (!shutdown_flag_.load()) {
        fd_set input_mask;
        FD_ZERO(&input_mask);
        FD_SET(socket_fd, &input_mask);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int sel_ret = select(socket_fd + 1, &input_mask, nullptr, nullptr, &timeout);

        if (sel_ret < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[receiver] select() error: " << strerror(errno) << std::endl;
            break;
        }

        if (sel_ret > 0) {
            if (PQconsumeInput(conn) == 0) {
                std::cerr << "[receiver] PQconsumeInput failed: "
                          << PQerrorMessage(conn) << std::endl;
                break;
            }
        }

        int ret = process_copy_data(conn, parser, last_received_lsn, last_status_time);
        if (ret < 0) break;

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - last_status_time).count();

        if (elapsed >= 10) {
            send_standby_status(conn, last_received_lsn, last_commit_lsn_.load());
            last_status_time = now;
        }
    }

    PQfinish(conn);
}

int WalReceiver::process_copy_data(PGconn* conn, LogicalDecoder& parser, 
                                   uint64_t& last_received_lsn, 
                                   std::chrono::time_point<std::chrono::steady_clock>& last_status_time) {
    char* buf = nullptr;
    int msg_len;

    while ((msg_len = PQgetCopyData(conn, &buf, 1 /* async */)) > 0) {
        if (msg_len < 1) {
            PQfreemem(buf);
            continue;
        }

        char msg_type = buf[0];

        switch (msg_type) {
            case 'w': // XLogData
                if (msg_len > 25) {
                    uint64_t wal_end = 0;
                    for (int i = 9; i < 17; ++i) {
                        wal_end = (wal_end << 8) | static_cast<uint8_t>(buf[i]);
                    }
                    last_received_lsn = wal_end;
                    parser.parse_message(buf + 25, msg_len - 25);
                }
                break;

            case 'k': // Primary keepalive
                if (msg_len >= 18 && static_cast<uint8_t>(buf[17]) == 1) {
                    send_standby_status(conn, last_received_lsn, last_commit_lsn_.load());
                    last_status_time = std::chrono::steady_clock::now();
                }
                break;

            default:
                // Ignore other message types if any
                break;
        }

        PQfreemem(buf);
        buf = nullptr;
    }

    if (msg_len == -2) {
        std::cerr << "[receiver] PQgetCopyData error: "
                  << PQerrorMessage(conn) << std::endl;
        return -1;
    }

    if (msg_len == -1) {
        std::cout << "[receiver] Replication stream ended." << std::endl;
        return -1;
    }

    return 0;
}


