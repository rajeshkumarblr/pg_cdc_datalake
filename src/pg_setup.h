#pragma once

#include "config.h"

class PgSetup {
public:
    explicit PgSetup(const Config& config) : config_(config) {}

    /*
     * One-time PostgreSQL setup performed on application startup.
     *
     * 1. Connects to PostgreSQL via libpq (regular SQL connection).
     * 2. Validates that wal_level = logical.
     * 3. Creates the publication if it doesn't exist.
     * 4. Creates the replication slot (using pgoutput) if it doesn't exist.
     * 5. Checks replication slot / walsender capacity.
     *
     * Throws std::runtime_error on fatal misconfigurations.
     */
    void setup();

private:
    const Config& config_;
};
