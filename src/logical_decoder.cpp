#include "logical_decoder.h"

#include <cstring>
#include <iostream>
#include <stdexcept>

/*
 * pgoutput binary protocol helpers.
 * All multi-byte integers are in network byte order (big-endian).
 *
 * Reference:
 * https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
 */


/* Read a big-endian uint8 from buffer at offset, advance offset */
static uint8_t read_uint8(const char* data, int& offset) {
    uint8_t val = static_cast<uint8_t>(data[offset]);
    offset += 1;
    return val;
}

/* Read a big-endian uint16 from buffer at offset, advance offset */
static uint16_t read_uint16(const char* data, int& offset) {
    uint16_t val = (static_cast<uint8_t>(data[offset]) << 8) |
                    static_cast<uint8_t>(data[offset + 1]);
    offset += 2;
    return val;
}

/* Read a big-endian uint32 from buffer at offset, advance offset */
static uint32_t read_uint32(const char* data, int& offset) {
    uint32_t val = (static_cast<uint32_t>(static_cast<uint8_t>(data[offset]))     << 24) |
                   (static_cast<uint32_t>(static_cast<uint8_t>(data[offset + 1])) << 16) |
                   (static_cast<uint32_t>(static_cast<uint8_t>(data[offset + 2])) <<  8) |
                    static_cast<uint32_t>(static_cast<uint8_t>(data[offset + 3]));
    offset += 4;
    return val;
}

/* Read a big-endian int64/uint64 from buffer at offset, advance offset */
static int64_t read_int64(const char* data, int& offset) {
    int64_t val = 0;
    for (int i = 0; i < 8; ++i) {
        val = (val << 8) | static_cast<uint8_t>(data[offset + i]);
    }
    offset += 8;
    return val;
}

/* Read a null-terminated string from buffer at offset, advance offset */
static std::string read_string(const char* data, int& offset) {
    std::string s(data + offset);
    offset += static_cast<int>(s.size()) + 1;  /* +1 for the null terminator */
    return s;
}

void LogicalDecoder::parse_message(const char* data, int len) {
    if (len < 1) return;

    char msg_type = data[0];

    /* Skip the 1-byte type tag — the sub-parsers start at byte 1 */
    const char* body = data + 1;
    int body_len = len - 1;

    switch (msg_type) {
        case 'R':
            parse_relation(body, body_len);
            break;
        case 'B':
            parse_begin(body, body_len);
            break;
        case 'I':
            parse_insert(body, body_len);
            break;
        case 'U':
            parse_update(body, body_len);
            break;
        case 'D':
            parse_delete(body, body_len);
            break;
        case 'C':
            parse_commit(body, body_len);
            break;
        case 'T':
            /* Truncate — log and skip */
            std::cout << "[parser] Truncate message received (skipping)" << std::endl;
            break;
        case 'O':
            /* Origin — log and skip */
            break;
        case 'Y':
            /* Type — log and skip */
            break;
        default:
            std::cerr << "[parser] Unknown message type: '"
                      << msg_type << "' (0x" << std::hex
                      << static_cast<int>(msg_type) << std::dec << ")" << std::endl;
            break;
    }
}

/*
 * Relation message ('R'):
 *   uint32  relation_id
 *   string  namespace (schema name)
 *   string  relation name (table name)
 *   uint8   replica identity setting
 *   uint16  number of columns
 *   For each column:
 *     uint8   flags (1 = part of key)
 *     string  column name
 *     uint32  type OID
 *     int32   type modifier
 */
void LogicalDecoder::parse_relation(const char* data, int len) {
    int offset = 0;

    auto schema = std::make_shared<TableSchema>();
    schema->relation_id = read_uint32(data, offset);
    schema->schema_name = read_string(data, offset);
    schema->table_name  = read_string(data, offset);
    schema->replica_identity = static_cast<char>(read_uint8(data, offset));

    uint16_t num_cols = read_uint16(data, offset);
    schema->columns.reserve(num_cols);

    for (uint16_t i = 0; i < num_cols; ++i) {
        ColumnSchema col;
        col.flags         = read_uint8(data, offset);
        col.name          = read_string(data, offset);
        col.type_oid      = read_uint32(data, offset);
        col.type_modifier = static_cast<int32_t>(read_uint32(data, offset));
        schema->columns.push_back(std::move(col));
    }

    std::cout << "[parser] Relation: " << schema->schema_name << "."
              << schema->table_name << " (id=" << schema->relation_id
              << ", cols=" << num_cols << ")" << std::endl;

    schemas_[schema->relation_id] = std::move(schema);
}

/*
 * Begin message ('B'):
 *   int64   final LSN of the transaction
 *   int64   commit timestamp (microseconds since 2000-01-01)
 *   uint32  transaction ID (xid)
 */
void LogicalDecoder::parse_begin(const char* data, int len) {
    int offset = 0;
    current_xact_lsn_ = static_cast<uint64_t>(read_int64(data, offset));
    current_xact_timestamp_us_ = read_int64(data, offset);
    /* uint32 xid = */ read_uint32(data, offset);
}

/*
 * Insert message ('I'):
 *   uint32  relation_id
 *   uint8   'N' (new tuple follows)
 *   TupleData
 */
void LogicalDecoder::parse_insert(const char* data, int len) {
    int offset = 0;

    uint32_t relation_id = read_uint32(data, offset);
    uint8_t  tuple_type  = read_uint8(data, offset);

    if (tuple_type != 'N') {
        std::cerr << "[parser] INSERT: expected 'N', got '"
                  << static_cast<char>(tuple_type) << "'" << std::endl;
        return;
    }

    auto it = schemas_.find(relation_id);
    if (it == schemas_.end()) {
        std::cerr << "[parser] INSERT: unknown relation_id " << relation_id << std::endl;
        return;
    }

    CDCRow row;
    row.operation = Operation::INSERT;
    row.lsn = current_xact_lsn_;
    row.commit_timestamp_us = current_xact_timestamp_us_;
    row.new_values = parse_tuple_data(data, len, offset);
    row.schema = it->second;

    if (row_callback_) {
        row_callback_(std::move(row));
    }
}

/*
 * Update message ('U'):
 *   uint32  relation_id
 *   [uint8  'K' or 'O' — key or old tuple (optional)]
 *   [TupleData — old values (if present)]
 *   uint8   'N' — new tuple follows
 *   TupleData — new values
 */
void LogicalDecoder::parse_update(const char* data, int len) {
    int offset = 0;

    uint32_t relation_id = read_uint32(data, offset);

    auto it = schemas_.find(relation_id);
    if (it == schemas_.end()) {
        std::cerr << "[parser] UPDATE: unknown relation_id " << relation_id << std::endl;
        return;
    }

    CDCRow row;
    row.operation = Operation::UPDATE;
    row.lsn = current_xact_lsn_;
    row.commit_timestamp_us = current_xact_timestamp_us_;
    row.schema = it->second;

    uint8_t next_byte = read_uint8(data, offset);

    /* Check if old tuple data is present (key-only or full old tuple) */
    if (next_byte == 'K' || next_byte == 'O') {
        row.old_values = parse_tuple_data(data, len, offset);
        next_byte = read_uint8(data, offset);
    }

    if (next_byte != 'N') {
        std::cerr << "[parser] UPDATE: expected 'N', got '"
                  << static_cast<char>(next_byte) << "'" << std::endl;
        return;
    }

    row.new_values = parse_tuple_data(data, len, offset);

    if (row_callback_) {
        row_callback_(std::move(row));
    }
}

/*
 * Delete message ('D'):
 *   uint32  relation_id
 *   uint8   'K' or 'O' — key or old tuple
 *   TupleData
 */
void LogicalDecoder::parse_delete(const char* data, int len) {
    int offset = 0;

    uint32_t relation_id = read_uint32(data, offset);

    auto it = schemas_.find(relation_id);
    if (it == schemas_.end()) {
        std::cerr << "[parser] DELETE: unknown relation_id " << relation_id << std::endl;
        return;
    }

    uint8_t tuple_type = read_uint8(data, offset);
    if (tuple_type != 'K' && tuple_type != 'O') {
        std::cerr << "[parser] DELETE: expected 'K' or 'O', got '"
                  << static_cast<char>(tuple_type) << "'" << std::endl;
        return;
    }

    CDCRow row;
    row.operation = Operation::DELETE;
    row.lsn = current_xact_lsn_;
    row.commit_timestamp_us = current_xact_timestamp_us_;
    row.old_values = parse_tuple_data(data, len, offset);
    row.schema = it->second;

    if (row_callback_) {
        row_callback_(std::move(row));
    }
}

/*
 * Commit message ('C'):
 *   uint8   flags (unused, always 0)
 *   int64   commit LSN
 *   int64   end LSN
 *   int64   commit timestamp
 */
void LogicalDecoder::parse_commit(const char* data, int len) {
    int offset = 0;
    /* uint8 flags = */ read_uint8(data, offset);
    uint64_t commit_lsn = static_cast<uint64_t>(read_int64(data, offset));
    /* int64 end_lsn = */ read_int64(data, offset);
    int64_t commit_ts = read_int64(data, offset);

    if (commit_callback_) {
        commit_callback_(commit_lsn, commit_ts);
    }
}

/*
 * Parse TupleData:
 *   uint16  number of columns
 *   For each column:
 *     uint8   column data type:
 *       'n' = NULL
 *       'u' = unchanged TOAST value (not sent)
 *       't' = text-format value follows
 *     If 't':
 *       uint32  length of value (not including this header)
 *       bytes   column value in text format
 */
std::vector<std::optional<std::string>>
LogicalDecoder::parse_tuple_data(const char* data, int len, int& offset) {
    uint16_t num_cols = read_uint16(data, offset);

    std::vector<std::optional<std::string>> values;
    values.reserve(num_cols);

    for (uint16_t i = 0; i < num_cols; ++i) {
        uint8_t col_type = read_uint8(data, offset);

        switch (col_type) {
            case 'n':
                /* NULL value */
                values.push_back(std::nullopt);
                break;

            case 'u':
                /* Unchanged TOAST value — treat as NULL for our purposes */
                values.push_back(std::nullopt);
                break;

            case 't': {
                /* Text-format value */
                uint32_t val_len = read_uint32(data, offset);
                std::string val(data + offset, val_len);
                offset += static_cast<int>(val_len);
                values.push_back(std::move(val));
                break;
            }

            default:
                std::cerr << "[parser] Unknown column data type: '"
                          << static_cast<char>(col_type) << "'" << std::endl;
                values.push_back(std::nullopt);
                break;
        }
    }

    return values;
}

std::shared_ptr<const TableSchema> LogicalDecoder::get_schema(uint32_t relation_id) const {
    auto it = schemas_.find(relation_id);
    return (it != schemas_.end()) ? it->second : nullptr;
}

