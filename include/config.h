#pragma once

#include <cstdint>
#include <string>
#include <vector>

/*
 * Application configuration loaded from cdc_data_lake.conf.
 * All fields have sensible defaults so the config file can be minimal.
 */
struct Config {
    /* PostgreSQL connection parameters */
    std::string pg_host         = "localhost";
    uint16_t    pg_port         = 5432;
    std::string pg_database     = "postgres";
    std::string pg_user         = "";
    std::string pg_password     = "";

    /* Replication slot and publication */
    std::string slot_name       = "cdc_lake_slot";
    std::string publication_name = "cdc_pub";

    /* Table filters (empty = all tables in publication) */
    std::vector<std::string> tables;

    /* Output settings */
    std::string output_dir      = "/data/cdc_lake";
    size_t      file_size_mb    = 1;
    uint32_t    flush_interval_sec = 60;

    /* Ring buffer capacity (number of CDCRow slots) */
    size_t      ring_buffer_capacity = 65536;

    /* Checkpoint file for crash recovery */
    std::string checkpoint_file = "";  /* defaults to <output_dir>/checkpoint.json */

    /* Global Logger configuration */
    std::string log_file        = "";  /* empty means stdout only */
    bool        log_to_console  = true;
    size_t      row_flush_threshold = 0; /* 0 means disabled */

    /* Convenience: build a libpq connection string for regular SQL */
    std::string connection_string() const;

    /* Convenience: build a libpq connection string for replication mode */
    std::string replication_connection_string() const;
};

/*
 * Parse a config file in key=value format.
 * Lines starting with '#' are comments. Blank lines are ignored.
 * Keys are trimmed, values are trimmed.
 */
Config parse_config(const std::string& filepath);
