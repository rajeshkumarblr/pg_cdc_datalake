#pragma once

#include "types.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <unordered_map>

/*
 * Callback types for pgoutput protocol events.
 * The WAL receiver registers these to push CDCRows into the ring buffer.
 */
using RowCallback = std::function<void(CDCRow&&)>;
using CommitCallback = std::function<void(uint64_t commit_lsn, int64_t commit_timestamp_us)>;

/*
 * Decodes the pgoutput binary wire protocol from logical replication.
 *
 * pgoutput sends a series of messages, each starting with a 1-byte type tag:
 *   'R' = Relation (table schema definition)
 *   'B' = Begin (start of a transaction)
 *   'I' = Insert
 *   'U' = Update
 *   'D' = Delete
 *   'C' = Commit (end of a transaction)
 *
 * This class maintains a cache of relation schemas (table definitions) because
 * the row data messages only reference relations by an integer ID.
 */
class LogicalDecoder {
public:
    LogicalDecoder() = default;

    /*
     * Set the callback invoked for each decoded row (INSERT/UPDATE/DELETE).
     */
    void set_row_callback(RowCallback cb) { row_callback_ = std::move(cb); }

    /*
     * Set the callback invoked on each COMMIT message.
     */
    void set_commit_callback(CommitCallback cb) { commit_callback_ = std::move(cb); }

    /*
     * Parse a single pgoutput message.
     * data points to the raw bytes (after the 'w' XLogData header has been
     * stripped by the caller). len is the length of the message body.
     */
    void parse_message(const char* data, int len);

    /*
     * Get the cached schema for a relation_id. Returns nullptr if not cached.
     */
    std::shared_ptr<const TableSchema> get_schema(uint32_t relation_id) const;

private:
    void parse_relation(const char* data, int len);
    void parse_begin(const char* data, int len);
    void parse_insert(const char* data, int len);
    void parse_update(const char* data, int len);
    void parse_delete(const char* data, int len);
    void parse_commit(const char* data, int len);

    /*
     * Parse a TupleData section from an I/U/D message.
     * Returns a vector of optional<string> values, one per column.
     * offset is updated to point past the consumed bytes.
     */
    std::vector<std::optional<std::string>>
    parse_tuple_data(const char* data, int len, int& offset);

    /* Relation cache: relation_id → TableSchema */
    std::unordered_map<uint32_t, std::shared_ptr<const TableSchema>> schemas_;

    /* Current transaction context */
    uint64_t current_xact_lsn_ = 0;
    int64_t  current_xact_timestamp_us_ = 0;

    /* Callbacks */
    RowCallback    row_callback_;
    CommitCallback commit_callback_;
};
