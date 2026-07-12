#include "snapshot_manager.h"
#include "logger.h"
#include <libpq-fe.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <chrono>
#include <cstring>
#include <thread>

class PGConnGuard {
public:
    explicit PGConnGuard(PGconn* conn) : conn_(conn) {}
    ~PGConnGuard() { if (conn_) PQfinish(conn_); }
    PGconn* get() { return conn_; }
private:
    PGconn* conn_;
};

class PGResultGuard {
public:
    explicit PGResultGuard(PGresult* res) : res_(res) {}
    ~PGResultGuard() { if (res_) PQclear(res_); }
    PGresult* get() { return res_; }
private:
    PGresult* res_;
};

SnapshotManager::SnapshotManager(const Config& config, RingBuffer<CDCRow>& ring_buffer, std::atomic<bool>& shutdown_flag)
    : config_(config), ring_buffer_(ring_buffer), shutdown_flag_(shutdown_flag) {}

std::shared_ptr<const TableSchema> SnapshotManager::fetch_schema(const std::string& table_name) {
    PGConnGuard conn(PQconnectdb(config_.connection_string().c_str()));
    if (PQstatus(conn.get()) != CONNECTION_OK) {
        throw std::runtime_error("Failed to connect for schema fetch");
    }

    std::string sql = "SELECT attname, atttypid, atttypmod FROM pg_attribute a "
                      "JOIN pg_type t ON a.atttypid = t.oid "
                      "JOIN pg_class c ON a.attrelid = c.oid "
                      "WHERE c.relname = '" + table_name + "' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum;";

    PGResultGuard res(PQexec(conn.get(), sql.c_str()));
    if (PQresultStatus(res.get()) != PGRES_TUPLES_OK) {
        throw std::runtime_error("Failed to fetch schema for " + table_name);
    }

    auto schema = std::make_shared<TableSchema>();
    schema->relation_id = 0; /* Not needed for snapshot */
    schema->table_name = table_name;
    int rows = PQntuples(res.get());
    for (int i = 0; i < rows; i++) {
        ColumnSchema col;
        col.name = PQgetvalue(res.get(), i, 0);
        col.type_oid = std::stoul(PQgetvalue(res.get(), i, 1));
        col.type_modifier = std::stol(PQgetvalue(res.get(), i, 2));
        col.flags = 0;
        schema->columns.push_back(col);
    }
    return schema;
}

void SnapshotManager::export_table(const std::string& snapshot_name, const std::string& table_name, uint64_t start_lsn) {
    Logger::info("Snapshot", "Starting snapshot export for table " + table_name);

    auto schema = fetch_schema(table_name);
    if (schema->columns.empty()) {
        Logger::info("Snapshot", "Table " + table_name + " has no columns or doesn't exist. Skipping.");
        return;
    }

    PGConnGuard conn(PQconnectdb(config_.connection_string().c_str()));
    if (PQstatus(conn.get()) != CONNECTION_OK) {
        throw std::runtime_error("Failed to connect for snapshot export");
    }

    PGResultGuard begin_res(PQexec(conn.get(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"));
    std::string set_snap_sql = "SET TRANSACTION SNAPSHOT '" + snapshot_name + "';";
    PGResultGuard snap_res(PQexec(conn.get(), set_snap_sql.c_str()));

    if (PQresultStatus(snap_res.get()) != PGRES_COMMAND_OK) {
        throw std::runtime_error("Failed to set snapshot: " + std::string(PQerrorMessage(conn.get())));
    }

    std::string copy_sql = "COPY \"" + table_name + "\" TO STDOUT;";
    PGResultGuard copy_res(PQexec(conn.get(), copy_sql.c_str()));
    if (PQresultStatus(copy_res.get()) != PGRES_COPY_OUT) {
        throw std::runtime_error("Failed to start COPY for " + table_name);
    }

    char* buffer;
    std::string line_buffer;
    size_t rows_exported = 0;

    int64_t ts = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    while (!shutdown_flag_.load()) {
        int ret = PQgetCopyData(conn.get(), &buffer, 0);
        if (ret == -1) {
            break; // End of COPY
        }
        if (ret == -2) {
            throw std::runtime_error("Error during COPY: " + std::string(PQerrorMessage(conn.get())));
        }
        if (ret > 0) {
            line_buffer.append(buffer, ret);
            PQfreemem(buffer);

            // Process full lines
            size_t pos;
            while ((pos = line_buffer.find('\n')) != std::string::npos) {
                std::string line = line_buffer.substr(0, pos);
                line_buffer.erase(0, pos + 1);
                
                // Remove trailing \r if windows style
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                CDCRow cdc_row;
                cdc_row.operation = Operation::INSERT;
                cdc_row.lsn = start_lsn;
                cdc_row.commit_timestamp_us = ts;
                cdc_row.schema = schema;

                // Split tab-separated line
                size_t start = 0;
                size_t tab_pos;
                while ((tab_pos = line.find('\t', start)) != std::string::npos) {
                    std::string val = line.substr(start, tab_pos - start);
                    if (val == "\\N") {
                        cdc_row.new_values.push_back(std::nullopt);
                    } else {
                        // TODO: Postgres COPY text format escapes special chars like \t, \n.
                        // For a robust implementation, unescape them here. 
                        // For this demo, we assume simple text.
                        cdc_row.new_values.push_back(val);
                    }
                    start = tab_pos + 1;
                }
                // Last column
                std::string val = line.substr(start);
                if (val == "\\N") {
                    cdc_row.new_values.push_back(std::nullopt);
                } else {
                    cdc_row.new_values.push_back(val);
                }

                // Push to ring buffer (blocking)
                if (!ring_buffer_.push(std::move(cdc_row))) {
                    break; // shutdown signaled
                }
                rows_exported++;
            }
        }
    }

    PGResultGuard commit_res(PQexec(conn.get(), "COMMIT;"));
    Logger::info("Snapshot", "Exported " + std::to_string(rows_exported) + " rows for table " + table_name);
}

void SnapshotManager::export_snapshot(const std::string& snapshot_name, uint64_t start_lsn) {
    Logger::info("Snapshot", "Starting global snapshot export: " + snapshot_name);
    for (const auto& table : config_.tables) {
        if (shutdown_flag_.load()) break;
        try {
            export_table(snapshot_name, table, start_lsn);
        } catch (const std::exception& e) {
            Logger::info("Snapshot", "Error exporting table " + table + ": " + e.what());
        }
    }
    Logger::info("Snapshot", "Global snapshot export completed.");
}
