#include "checkpoint.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

std::string lsn_to_string(uint64_t lsn) {
    uint32_t hi = static_cast<uint32_t>(lsn >> 32);
    uint32_t lo = static_cast<uint32_t>(lsn & 0xFFFFFFFF);
    std::ostringstream ss;
    ss << std::uppercase << std::hex << hi << "/" << std::setw(8) << std::setfill('0') << lo;
    return ss.str();
}

uint64_t string_to_lsn(const std::string& s) {
    auto slash = s.find('/');
    if (slash == std::string::npos) {
        return 0;
    }
    uint64_t hi = std::stoull(s.substr(0, slash), nullptr, 16);
    uint64_t lo = std::stoull(s.substr(slash + 1), nullptr, 16);
    return (hi << 32) | lo;
}

/*
 * Simple JSON parser for checkpoint — avoids pulling in a JSON library
 * for just two fields. Format:
 * {
 *   "last_confirmed_lsn": "0/89056BB0",
 *   "last_confirmed_time": "2026-06-28T13:00:00Z",
 *   "files_written": 42
 * }
 */
static std::string extract_json_string(const std::string& json, const std::string& key) {
    auto key_pos = json.find("\"" + key + "\"");
    if (key_pos == std::string::npos) return "";

    auto colon = json.find(':', key_pos);
    if (colon == std::string::npos) return "";

    auto quote_start = json.find('"', colon + 1);
    if (quote_start == std::string::npos) return "";

    auto quote_end = json.find('"', quote_start + 1);
    if (quote_end == std::string::npos) return "";

    return json.substr(quote_start + 1, quote_end - quote_start - 1);
}

static uint64_t extract_json_uint(const std::string& json, const std::string& key) {
    auto key_pos = json.find("\"" + key + "\"");
    if (key_pos == std::string::npos) return 0;

    auto colon = json.find(':', key_pos);
    if (colon == std::string::npos) return 0;

    /* Skip whitespace after colon */
    auto num_start = json.find_first_not_of(" \t\r\n", colon + 1);
    if (num_start == std::string::npos) return 0;

    return std::stoull(json.substr(num_start));
}

Checkpoint CheckpointManager::load_checkpoint() {
    Checkpoint cp;

    std::ifstream file(filepath_);
    if (!file.is_open()) {
        std::cout << "[checkpoint] No checkpoint file found at " << filepath_
                  << " — starting from LSN 0/0" << std::endl;
        return cp;
    }

    /* Read the entire file into a string */
    std::ostringstream ss;
    ss << file.rdbuf();
    std::string json = ss.str();

    std::string lsn_str = extract_json_string(json, "last_confirmed_lsn");
    if (!lsn_str.empty()) {
        cp.last_confirmed_lsn = string_to_lsn(lsn_str);
    }

    cp.last_confirmed_time = extract_json_string(json, "last_confirmed_time");
    cp.files_written = extract_json_uint(json, "files_written");

    std::cout << "[checkpoint] Loaded: LSN=" << lsn_to_string(cp.last_confirmed_lsn)
              << " time=" << cp.last_confirmed_time
              << " files=" << cp.files_written << std::endl;

    return cp;
}

void CheckpointManager::save_checkpoint(const Checkpoint& cp) {
    std::string tmp_path = filepath_ + ".tmp";

    std::ofstream file(tmp_path);
    if (!file.is_open()) {
        std::cerr << "[checkpoint] ERROR: Cannot write to " << tmp_path << std::endl;
        return;
    }

    file << "{\n"
         << "  \"last_confirmed_lsn\": \"" << lsn_to_string(cp.last_confirmed_lsn) << "\",\n"
         << "  \"last_confirmed_time\": \"" << cp.last_confirmed_time << "\",\n"
         << "  \"files_written\": " << cp.files_written << "\n"
         << "}\n";

    file.close();

    /* Atomic rename: ensures the checkpoint file is never half-written */
    if (std::rename(tmp_path.c_str(), filepath_.c_str()) != 0) {
        std::cerr << "[checkpoint] ERROR: rename failed: " << strerror(errno) << std::endl;
    }
}
