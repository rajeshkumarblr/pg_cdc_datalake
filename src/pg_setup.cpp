#include "pg_setup.h"

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <libpq-fe.h>

/*
 * RAII wrapper for PGconn to ensure cleanup on all exit paths.
 */
class PGConnGuard {
public:
    explicit PGConnGuard(PGconn* conn) : conn_(conn) {}
    ~PGConnGuard() { if (conn_) PQfinish(conn_); }
    PGconn* get() { return conn_; }
    PGConnGuard(const PGConnGuard&) = delete;
    PGConnGuard& operator=(const PGConnGuard&) = delete;
private:
    PGconn* conn_;
};

/*
 * RAII wrapper for PGresult.
 */
class PGResultGuard {
public:
    explicit PGResultGuard(PGresult* res) : res_(res) {}
    ~PGResultGuard() { if (res_) PQclear(res_); }
    PGresult* get() { return res_; }
    PGResultGuard(const PGResultGuard&) = delete;
    PGResultGuard& operator=(const PGResultGuard&) = delete;
private:
    PGresult* res_;
};

/*
 * Execute a single SQL query and return the result.
 * Throws on error.
 */
static PGresult* exec_sql(PGconn* conn, const std::string& sql) {
    PGresult* res = PQexec(conn, sql.c_str());
    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(conn);
        PQclear(res);
        throw std::runtime_error("SQL error: " + err + " [query: " + sql + "]");
    }
    return res;
}

void PgSetup::setup() {
    std::cout << "[pg_setup] Connecting to PostgreSQL..." << std::endl;

    PGConnGuard conn(PQconnectdb(config_.connection_string().c_str()));
    if (PQstatus(conn.get()) != CONNECTION_OK) {
        throw std::runtime_error(
            std::string("[pg_setup] Connection failed: ") + PQerrorMessage(conn.get()));
    }

    std::cout << "[pg_setup] Connected to " << config_.pg_database
              << " on " << config_.pg_host << ":" << config_.pg_port << std::endl;

    /*
     * Step 1: Validate wal_level = logical
     */
    {
        PGResultGuard res(exec_sql(conn.get(), "SHOW wal_level"));
        std::string wal_level = PQgetvalue(res.get(), 0, 0);
        if (wal_level != "logical") {
            throw std::runtime_error(
                "[pg_setup] wal_level is '" + wal_level +
                "' — must be 'logical'. Set wal_level = logical in postgresql.conf and restart.");
        }
        std::cout << "[pg_setup] wal_level = logical ✓" << std::endl;
    }

    /*
     * Step 2: Check replication slot capacity
     */
    {
        PGResultGuard res_max(exec_sql(conn.get(), "SHOW max_replication_slots"));
        int max_slots = std::stoi(PQgetvalue(res_max.get(), 0, 0));

        PGResultGuard res_cur(exec_sql(conn.get(),
            "SELECT count(*) FROM pg_replication_slots"));
        int current_slots = std::stoi(PQgetvalue(res_cur.get(), 0, 0));

        std::cout << "[pg_setup] Replication slots: " << current_slots
                  << "/" << max_slots << " used" << std::endl;

        if (current_slots >= max_slots) {
            std::cerr << "[pg_setup] WARNING: All replication slots are in use!" << std::endl;
        }
    }

    /*
     * Step 3: Create publication if it doesn't exist
     */
    {
        /* Check if publication already exists */
        std::string check_sql =
            "SELECT 1 FROM pg_publication WHERE pubname = '" +
            config_.publication_name + "'";

        PGResultGuard res(exec_sql(conn.get(), check_sql));
        if (PQntuples(res.get()) == 0) {
            /* Build CREATE PUBLICATION statement */
            std::ostringstream create_sql;
            create_sql << "CREATE PUBLICATION " << config_.publication_name;

            if (config_.tables.empty()) {
                create_sql << " FOR ALL TABLES";
            } else {
                create_sql << " FOR TABLE ";
                for (size_t i = 0; i < config_.tables.size(); ++i) {
                    if (i > 0) create_sql << ", ";
                    create_sql << config_.tables[i];
                }
            }

            PGResultGuard create_res(exec_sql(conn.get(), create_sql.str()));
            std::cout << "[pg_setup] Created publication '" << config_.publication_name
                      << "'" << std::endl;
        } else {
            std::cout << "[pg_setup] Publication '" << config_.publication_name
                      << "' already exists ✓" << std::endl;
        }
    }

    /*
     * Step 4: Replication slot creation is now deferred to WalReceiver
     * so that it can export the snapshot over the replication connection.
     */

    std::cout << "[pg_setup] Setup complete." << std::endl;
}
