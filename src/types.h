#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

/*
 * CDC operation types corresponding to pgoutput message types.
 */
enum class Operation : char {
    INSERT = 'I',
    UPDATE = 'U',
    DELETE = 'D',
};

/*
 * Describes a single column from a pgoutput Relation message.
 * The type_oid is the PostgreSQL type OID (e.g., 23 = int4, 25 = text).
 */
struct ColumnSchema {
    std::string name;
    uint32_t    type_oid;
    int32_t     type_modifier;  /* -1 means no modifier */
    uint8_t     flags;          /* 1 = part of replica identity key */
};

/*
 * Describes a table's schema, populated from pgoutput 'R' (Relation) messages.
 * The relation_id is used to look up the schema when processing row data.
 */
struct TableSchema {
    uint32_t    relation_id;
    std::string schema_name;    /* e.g. "public" */
    std::string table_name;     /* e.g. "wal_test" */
    char        replica_identity; /* 'd' = default (PK), 'n' = nothing, 'f' = full, 'i' = index */
    std::vector<ColumnSchema> columns;
};

/*
 * A single CDC row extracted from the replication stream.
 * This is the unit of data that flows through the ring buffer
 * from the WAL receiver thread to the Parquet writer thread.
 */
struct CDCRow {
    std::string table_name;
    Operation   operation;
    uint64_t    lsn;
    int64_t     commit_timestamp_us;  /* microseconds since 2000-01-01 (PG epoch) */

    /*
     * Column values as strings (PG text representation).
     * nullopt means the column value is SQL NULL.
     * For INSERT: new_values is populated, old_values is empty.
     * For UPDATE: both may be populated (old only if REPLICA IDENTITY FULL).
     * For DELETE: old_values is populated, new_values is empty.
     */
    std::vector<std::optional<std::string>> old_values;
    std::vector<std::optional<std::string>> new_values;

    /* Schema of the table this row belongs to */
    TableSchema schema;
};
