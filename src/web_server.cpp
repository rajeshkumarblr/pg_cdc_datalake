#include "web_server.h"
#include "logger.h"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>
#include <iostream>

using json = nlohmann::json;

struct WebServer::Impl {
    httplib::Server svr;
};

WebServer::WebServer(const std::string& config_path, Metrics& metrics, std::atomic<bool>& shutdown_flag)
    : config_path_(config_path), metrics_(metrics), shutdown_flag_(shutdown_flag), impl_(new Impl()) {
    
    // Serve static dashboard files
    impl_->svr.set_mount_point("/", "./dashboard");

    // GET /api/metrics (JSON format for dashboard)
    impl_->svr.Get("/api/metrics", [this](const httplib::Request&, httplib::Response& res) {
        json j = {
            {"rows_ingested", metrics_.rows_ingested.load()},
            {"files_written", metrics_.files_written.load()},
            {"bytes_processed", metrics_.bytes_processed.load()},
            {"current_lsn", metrics_.current_lsn.load()},
            {"uptime_seconds", metrics_.uptime_seconds()}
        };
        res.set_content(j.dump(), "application/json");
    });

    // GET /metrics (Prometheus text format)
    impl_->svr.Get("/metrics", [this](const httplib::Request&, httplib::Response& res) {
        std::stringstream ss;
        ss << "# HELP pg_cdc_rows_ingested_total Total number of CDC rows ingested from PostgreSQL.\n"
           << "# TYPE pg_cdc_rows_ingested_total counter\n"
           << "pg_cdc_rows_ingested_total " << metrics_.rows_ingested.load() << "\n\n"
           << "# HELP pg_cdc_files_written_total Total number of Parquet files written.\n"
           << "# TYPE pg_cdc_files_written_total counter\n"
           << "pg_cdc_files_written_total " << metrics_.files_written.load() << "\n\n"
           << "# HELP pg_cdc_bytes_processed_total Total estimated bytes processed into Parquet.\n"
           << "# TYPE pg_cdc_bytes_processed_total counter\n"
           << "pg_cdc_bytes_processed_total " << metrics_.bytes_processed.load() << "\n\n"
           << "# HELP pg_cdc_current_lsn Current WAL Log Sequence Number (as base10 int).\n"
           << "# TYPE pg_cdc_current_lsn gauge\n"
           << "pg_cdc_current_lsn " << metrics_.current_lsn.load() << "\n\n"
           << "# HELP pg_cdc_uptime_seconds Daemon uptime in seconds.\n"
           << "# TYPE pg_cdc_uptime_seconds gauge\n"
           << "pg_cdc_uptime_seconds " << metrics_.uptime_seconds() << "\n";
        res.set_content(ss.str(), "text/plain");
    });

    // GET /api/config
    impl_->svr.Get("/api/config", [this](const httplib::Request&, httplib::Response& res) {
        std::ifstream file(config_path_);
        if (!file.is_open()) {
            res.status = 404;
            res.set_content("{\"error\": \"Config file not found\"}", "application/json");
            return;
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        
        json j = {{"config", buffer.str()}};
        res.set_content(j.dump(), "application/json");
    });

    // POST /api/config
    impl_->svr.Post("/api/config", [this](const httplib::Request& req, httplib::Response& res) {
        try {
            auto j = json::parse(req.body);
            if (j.contains("config") && j["config"].is_string()) {
                std::string new_config = j["config"];
                std::ofstream file(config_path_);
                if (file.is_open()) {
                    file << new_config;
                    file.close();
                    res.set_content("{\"status\": \"success\"}", "application/json");
                    return;
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Config POST Error: " << e.what() << std::endl;
        }
        res.status = 400;
        res.set_content("{\"error\": \"Invalid config payload\"}", "application/json");
    });
}

WebServer::~WebServer() {
    delete impl_;
}

void WebServer::run(int port) {
    Logger::info("WebServer", "Starting HTTP server on port " + std::to_string(port));
    impl_->svr.listen("0.0.0.0", port);
}

void WebServer::stop() {
    Logger::info("WebServer", "Stopping HTTP server...");
    impl_->svr.stop();
}
