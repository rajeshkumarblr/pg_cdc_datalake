#include "config.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

/*
 * Trim whitespace from both ends of a string.
 */
static std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

/*
 * Split a comma-separated string into a vector of trimmed tokens.
 */
static std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> tokens;
    std::istringstream stream(s);
    std::string token;
    while (std::getline(stream, token, ',')) {
        auto trimmed = trim(token);
        if (!trimmed.empty()) {
            tokens.push_back(trimmed);
        }
    }
    return tokens;
}

std::string Config::connection_string() const {
    std::ostringstream ss;
    ss << "host=" << pg_host
       << " port=" << pg_port
       << " dbname=" << pg_database;
    if (!pg_user.empty()) {
        ss << " user=" << pg_user;
    }
    return ss.str();
}

std::string Config::replication_connection_string() const {
    std::ostringstream ss;
    ss << "host=" << pg_host
       << " port=" << pg_port
       << " dbname=" << pg_database
       << " replication=database";
    if (!pg_user.empty()) {
        ss << " user=" << pg_user;
    }
    return ss.str();
}

Config parse_config(const std::string& filepath) {
    Config config;

    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + filepath);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        ++line_num;
        line = trim(line);

        /* Skip comments and blank lines */
        if (line.empty() || line[0] == '#') {
            continue;
        }

        auto eq_pos = line.find('=');
        if (eq_pos == std::string::npos) {
            std::cerr << "WARNING: config line " << line_num
                      << ": missing '=' — skipping: " << line << std::endl;
            continue;
        }

        std::string key = trim(line.substr(0, eq_pos));
        std::string value = trim(line.substr(eq_pos + 1));

        if (key == "pg_host") {
            config.pg_host = value;
        } else if (key == "pg_port") {
            config.pg_port = static_cast<uint16_t>(std::stoi(value));
        } else if (key == "pg_database") {
            config.pg_database = value;
        } else if (key == "pg_user") {
            config.pg_user = value;
        } else if (key == "slot_name") {
            config.slot_name = value;
        } else if (key == "publication_name") {
            config.publication_name = value;
        } else if (key == "tables") {
            config.tables = split_csv(value);
        } else if (key == "output_dir") {
            config.output_dir = value;
        } else if (key == "file_size_mb") {
            config.file_size_mb = static_cast<size_t>(std::stoul(value));
        } else if (key == "flush_interval_sec") {
            config.flush_interval_sec = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "ring_buffer_capacity") {
            config.ring_buffer_capacity = static_cast<size_t>(std::stoul(value));
        } else if (key == "checkpoint_file") {
            config.checkpoint_file = value;
        } else if (key == "log_file") {
            config.log_file = value;
        } else if (key == "log_to_console") {
            config.log_to_console = (value == "true" || value == "1");
        } else if (key == "row_flush_threshold") {
            config.row_flush_threshold = static_cast<size_t>(std::stoul(value));
        } else {
            std::cerr << "WARNING: config line " << line_num
                      << ": unknown key '" << key << "' — skipping" << std::endl;
        }
    }

    /* Default checkpoint file if not explicitly set */
    if (config.checkpoint_file.empty()) {
        config.checkpoint_file = config.output_dir + "/checkpoint.json";
    }

    return config;
}
