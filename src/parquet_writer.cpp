#include "parquet_writer.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <nlohmann/json.hpp>

#include "logger.h"
#include <fcntl.h>
#include <unistd.h>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

/*
 * Convert an operation enum to a human-readable string for the Parquet column.
 */
static const char* operation_to_string(Operation op) {
    switch (op) {
        case Operation::INSERT: return "insert";
        case Operation::UPDATE: return "update_postimage";
        case Operation::DELETE: return "delete";
        default: return "unknown";
    }
}

/*
 * Format an ISO-8601 timestamp string for file naming.
 */
static std::string current_timestamp_string() {
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
    gmtime_r(&tt, &tm_buf);
    std::ostringstream ss;
    ss << std::put_time(&tm_buf, "%Y%m%dT%H%M%SZ");
    return ss.str();
}

/*
 * Format an LSN as a hex string for file naming.
 */
static std::string lsn_to_hex(uint64_t lsn) {
    std::ostringstream ss;
    ss << "0x" << std::uppercase << std::hex << lsn;
    return ss.str();
}

/*
 * Mapping from PostgreSQL type OID to Arrow DataType
 */
static std::shared_ptr<arrow::DataType> oid_to_arrow_type(uint32_t type_oid) {
    switch (type_oid) {
        case 21:   return arrow::int16();
        case 23:   return arrow::int32();
        case 20:   return arrow::int64();
        case 700:  return arrow::float32();
        case 701:
        case 1700: return arrow::float64();
        case 16:   return arrow::boolean();
        default:   return arrow::utf8();
    }
}

/*
 * Mapping from PostgreSQL type OID to Delta Lake string type
 */
static std::string oid_to_delta_type(uint32_t type_oid) {
    switch (type_oid) {
        case 21:   return "short";
        case 23:   return "integer";
        case 20:   return "long";
        case 700:  return "float";
        case 701:
        case 1700: return "double";
        case 16:   return "boolean";
        default:   return "string";
    }
}

/*
 * Helper to create the correct Arrow builder based on OID
 */
static std::shared_ptr<arrow::ArrayBuilder> make_builder(uint32_t type_oid) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    switch (type_oid) {
        case 21:   return std::make_shared<arrow::Int16Builder>(pool);
        case 23:   return std::make_shared<arrow::Int32Builder>(pool);
        case 20:   return std::make_shared<arrow::Int64Builder>(pool);
        case 700:  return std::make_shared<arrow::FloatBuilder>(pool);
        case 701:
        case 1700: return std::make_shared<arrow::DoubleBuilder>(pool);
        case 16:   return std::make_shared<arrow::BooleanBuilder>(pool);
        default:   return std::make_shared<arrow::StringBuilder>(pool);
    }
}

/*
 * TableBuffer accumulates rows for a single PostgreSQL table
 */
struct TableBuffer {
    std::string table_name;
    TableSchema schema;

    std::shared_ptr<arrow::StringBuilder>  op_builder;
    std::shared_ptr<arrow::Int64Builder>   lsn_builder;
    std::shared_ptr<arrow::Int64Builder>   ts_builder;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> col_builders;

    uint64_t start_lsn  = 0;
    uint64_t end_lsn    = 0;
    int64_t  start_time = 0;
    int64_t  end_time   = 0;
    size_t   row_count  = 0;
    size_t   estimated_bytes = 0;
    size_t   commit_version = 0;

    bool initialized = false;
    bool initialized_version = false;
    bool schema_changed = false;
    size_t uncompacted_commits = 0;

    void init(const TableSchema& s) {
        schema = s;
        table_name = s.table_name;
        op_builder  = std::make_shared<arrow::StringBuilder>();
        lsn_builder = std::make_shared<arrow::Int64Builder>();
        ts_builder  = std::make_shared<arrow::Int64Builder>();

        col_builders.clear();
        for (size_t i = 0; i < s.columns.size(); ++i) {
            col_builders.push_back(make_builder(s.columns[i].type_oid));
        }

        start_lsn = 0;
        end_lsn = 0;
        start_time = 0;
        end_time = 0;
        row_count = 0;
        estimated_bytes = 0;
        initialized = true;
    }

    void reset() {
        if (!initialized) return;

        op_builder  = std::make_shared<arrow::StringBuilder>();
        lsn_builder = std::make_shared<arrow::Int64Builder>();
        ts_builder  = std::make_shared<arrow::Int64Builder>();

        for (size_t i = 0; i < schema.columns.size(); ++i) {
            col_builders[i] = make_builder(schema.columns[i].type_oid);
        }

        start_lsn = 0;
        end_lsn = 0;
        start_time = 0;
        end_time = 0;
        row_count = 0;
        estimated_bytes = 0;
    }
};

/*
 * Compare two TableSchemas to detect DDL changes
 */
static bool schemas_are_equal(const TableSchema& a, const TableSchema& b) {
    if (a.columns.size() != b.columns.size()) return false;
    for (size_t i = 0; i < a.columns.size(); ++i) {
        if (a.columns[i].name != b.columns[i].name) return false;
        if (a.columns[i].type_oid != b.columns[i].type_oid) return false;
    }
    return true;
}

/*
 * Helper to get the latest commit version by scanning _delta_log/
 */
static size_t get_latest_delta_version(const std::string& delta_log_dir) {
    if (!std::filesystem::exists(delta_log_dir)) return 0;
    size_t max_version = 0;
    bool found = false;
    for (const auto& entry : std::filesystem::directory_iterator(delta_log_dir)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            if (filename.size() == 25 && filename.substr(filename.length() - 5) == ".json") {
                try {
                    size_t ver = std::stoull(filename.substr(0, 20));
                    if (!found || ver > max_version) {
                        max_version = ver;
                        found = true;
                    }
                } catch (...) {}
            }
        }
    }
    return found ? max_version + 1 : 0;
}

/*
 * Helper to append a single row to a table buffer.
 */
struct ParsedValue {
    bool is_null = true;
    uint32_t type_oid = 0;
    union {
        int16_t i16;
        int32_t i32;
        int64_t i64;
        float f32;
        double f64;
        bool b;
    } val;
    std::string str_val;
};

static void append_row_to_buffer(const CDCRow& row, TableBuffer& buf) {
    Logger::info("ParquetWriter", "Entering append_row_to_buffer for table: " + row.schema->table_name);
    if (!buf.initialized) {
        buf.init(*row.schema);
        std::cout << "[writer] Initialized buffer for table '"
                  << row.schema->table_name << "' ("
                  << row.schema->columns.size() << " columns)" << std::endl;
    } else if (!schemas_are_equal(buf.schema, *row.schema)) {
        Logger::info("ParquetWriter", "Schema change detected for table: " + row.schema->table_name);
        throw std::runtime_error("SCHEMA_CHANGE_DETECTED");
    }

    const auto& values = (row.operation == Operation::DELETE) ? row.old_values : row.new_values;

    // Phase 1: Parse all values
    std::vector<ParsedValue> parsed_values(buf.col_builders.size());
    try {
        for (size_t i = 0; i < buf.col_builders.size(); ++i) {
            if (i < values.size() && values[i].has_value()) {
                const std::string& val = values[i].value();
                uint32_t type_oid = row.schema->columns[i].type_oid;
                parsed_values[i].is_null = false;
                parsed_values[i].type_oid = type_oid;

                switch (type_oid) {
                    case 21: parsed_values[i].val.i16 = static_cast<int16_t>(std::stoi(val)); break;
                    case 23: parsed_values[i].val.i32 = static_cast<int32_t>(std::stoi(val)); break;
                    case 20: parsed_values[i].val.i64 = static_cast<int64_t>(std::stoll(val)); break;
                    case 700: parsed_values[i].val.f32 = std::stof(val); break;
                    case 701:
                    case 1700: parsed_values[i].val.f64 = std::stod(val); break;
                    case 16: parsed_values[i].val.b = (val == "t" || val == "true" || val == "1"); break;
                    default: parsed_values[i].str_val = val; break;
                }
            } else {
                parsed_values[i].is_null = true;
            }
        }
    } catch (const std::exception& e) {
        Logger::info("ParquetWriter", "Failed to parse row for table " + row.schema->table_name + ", dropping row. Error: " + std::string(e.what()));
        return; // Drop row entirely, do not append to ANY builders
    }

    // Phase 2: Append to builders
    if (buf.row_count == 0) {
        buf.start_lsn = row.lsn;
        buf.start_time = row.commit_timestamp_us;
    }
    buf.end_lsn = row.lsn;
    buf.end_time = row.commit_timestamp_us;
    buf.row_count++;

    (void)buf.op_builder->Append(operation_to_string(row.operation));
    (void)buf.lsn_builder->Append(static_cast<int64_t>(row.lsn));
    (void)buf.ts_builder->Append(row.commit_timestamp_us);
    buf.estimated_bytes += 32;

    for (size_t i = 0; i < buf.col_builders.size(); ++i) {
        if (!parsed_values[i].is_null) {
            switch (parsed_values[i].type_oid) {
                case 21: {
                    auto b = std::static_pointer_cast<arrow::Int16Builder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].val.i16);
                    buf.estimated_bytes += 2;
                    break;
                }
                case 23: {
                    auto b = std::static_pointer_cast<arrow::Int32Builder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].val.i32);
                    buf.estimated_bytes += 4;
                    break;
                }
                case 20: {
                    auto b = std::static_pointer_cast<arrow::Int64Builder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].val.i64);
                    buf.estimated_bytes += 8;
                    break;
                }
                case 700: {
                    auto b = std::static_pointer_cast<arrow::FloatBuilder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].val.f32);
                    buf.estimated_bytes += 4;
                    break;
                }
                case 701:
                case 1700: {
                    auto b = std::static_pointer_cast<arrow::DoubleBuilder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].val.f64);
                    buf.estimated_bytes += 8;
                    break;
                }
                case 16: {
                    auto b = std::static_pointer_cast<arrow::BooleanBuilder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].val.b);
                    buf.estimated_bytes += 1;
                    break;
                }
                default: {
                    auto b = std::static_pointer_cast<arrow::StringBuilder>(buf.col_builders[i]);
                    (void)b->Append(parsed_values[i].str_val);
                    buf.estimated_bytes += parsed_values[i].str_val.size();
                    break;
                }
            }
        } else {
            (void)buf.col_builders[i]->AppendNull();
        }
    }
}

/*
 * Helper to flush a table buffer to disk as a Parquet file.
 */
static void flush_table_buffer(TableBuffer& buf, const Config& config, 
                               Checkpoint& checkpoint, CheckpointManager& checkpoint_manager) {
    Logger::info("ParquetWriter", "Entering flush_table_buffer for table: " + buf.table_name);
    if (buf.row_count == 0) return;

    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.push_back(arrow::field("_change_type", arrow::utf8(), false));
    fields.push_back(arrow::field("_commit_version", arrow::int64(), false));
    fields.push_back(arrow::field("_commit_timestamp", arrow::int64(), false));

    for (const auto& col : buf.schema.columns) {
        fields.push_back(arrow::field(col.name, oid_to_arrow_type(col.type_oid), true));
    }
    auto arrow_schema = arrow::schema(fields);

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::shared_ptr<arrow::Array> op_array, lsn_array, ts_array;
    (void)buf.op_builder->Finish(&op_array);
    (void)buf.lsn_builder->Finish(&lsn_array);
    (void)buf.ts_builder->Finish(&ts_array);
    arrays.push_back(op_array);
    arrays.push_back(lsn_array);
    arrays.push_back(ts_array);

    for (auto& builder : buf.col_builders) {
        std::shared_ptr<arrow::Array> arr;
        (void)builder->Finish(&arr);
        arrays.push_back(arr);
    }

    auto table = arrow::Table::Make(arrow_schema, arrays);

    std::string table_dir = config.output_dir + "/" + buf.table_name;
    std::filesystem::create_directories(table_dir);

    std::string filename = current_timestamp_string() + "_"
        + lsn_to_hex(buf.start_lsn) + "_"
        + lsn_to_hex(buf.end_lsn) + ".parquet";

    std::string tmp_path = table_dir + "/" + filename + ".tmp";
    std::string final_path = table_dir + "/" + filename;

    auto file_result = arrow::io::FileOutputStream::Open(tmp_path);
    if (!file_result.ok()) {
        std::cerr << "[writer] Cannot open " << tmp_path << ": " << file_result.status().ToString() << std::endl;
        buf.reset();
        return;
    }
    auto outfile = file_result.ValueOrDie();

    auto kv_metadata = arrow::KeyValueMetadata::Make(
        {"cdc.start_lsn", "cdc.end_lsn", "cdc.start_time", "cdc.end_time", "cdc.row_count", "cdc.table_name"},
        {lsn_to_hex(buf.start_lsn), lsn_to_hex(buf.end_lsn), std::to_string(buf.start_time), std::to_string(buf.end_time), std::to_string(buf.row_count), buf.table_name});

    auto table_with_meta = table->ReplaceSchemaMetadata(kv_metadata);
    auto writer_props = parquet::WriterProperties::Builder().compression(parquet::Compression::SNAPPY)->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    auto write_status = parquet::arrow::WriteTable(
        *table_with_meta, arrow::default_memory_pool(), outfile, table->num_rows(), writer_props, arrow_props);

    if (!write_status.ok()) {
        std::cerr << "[writer] Parquet write failed: " << write_status.ToString() << std::endl;
        buf.reset();
        return;
    }
    (void)outfile->Close();

    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        std::cerr << "[writer] rename failed: " << strerror(errno) << std::endl;
    } else {
        // Write Delta Lake log
        std::string delta_log_dir = table_dir + "/_delta_log";
        std::filesystem::create_directories(delta_log_dir);
        
        if (!buf.initialized_version) {
            buf.commit_version = get_latest_delta_version(delta_log_dir);
            buf.initialized_version = true;
        }
        
        bool wrote_log = false;
        int retries = 0;
        
        while (!wrote_log && retries < 5) {
            std::ostringstream version_ss;
            version_ss << std::setfill('0') << std::setw(20) << buf.commit_version;
            std::string delta_log_file = delta_log_dir + "/" + version_ss.str() + ".json";
            
            int fd = open(delta_log_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (fd < 0) {
                if (errno == EEXIST) {
                    buf.commit_version++;
                    retries++;
                    continue;
                } else {
                    std::cerr << "[writer] failed to open delta log: " << strerror(errno) << std::endl;
                    break;
                }
            }
            
            FILE* f = fdopen(fd, "w");
            if (f) {
                std::string content = "";
                
                nlohmann::json commitInfo = {
                    {"commitInfo", {
                        {"timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()},
                        {"operation", "WRITE"}
                    }}
                };
                
                nlohmann::json addAction = {
                    {"add", {
                        {"path", filename},
                        {"size", std::filesystem::file_size(final_path)},
                        {"partitionValues", nlohmann::json::object()},
                        {"modificationTime", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()},
                        {"dataChange", true}
                    }}
                };
                
                if (buf.commit_version == 0 || buf.schema_changed) {
                    // First commit or schema evolution needs protocol and metadata
                    nlohmann::json protocolAction = {{"protocol", {{"minReaderVersion", 1}, {"minWriterVersion", 2}}}};
                    
                    nlohmann::json schemaFields = nlohmann::json::array();
                    schemaFields.push_back({{"name", "_change_type"}, {"type", "string"}, {"nullable", false}, {"metadata", nlohmann::json::object()}});
                    schemaFields.push_back({{"name", "_commit_version"}, {"type", "long"}, {"nullable", false}, {"metadata", nlohmann::json::object()}});
                    schemaFields.push_back({{"name", "_commit_timestamp"}, {"type", "long"}, {"nullable", false}, {"metadata", nlohmann::json::object()}});
                    for (const auto& col : buf.schema.columns) {
                        schemaFields.push_back({{"name", col.name}, {"type", oid_to_delta_type(col.type_oid)}, {"nullable", true}, {"metadata", nlohmann::json::object()}});
                    }
                    nlohmann::json schemaObj = {{"type", "struct"}, {"fields", schemaFields}};
                    
                    nlohmann::json metadataAction = {
                        {"metaData", {
                            {"id", "cdc-" + buf.table_name},
                            {"format", {{"provider", "parquet"}, {"options", nlohmann::json::object()}}},
                            {"schemaString", schemaObj.dump()},
                            {"partitionColumns", nlohmann::json::array()},
                            {"configuration", {
                                {"delta.enableChangeDataFeed", "true"}
                            }}
                        }}
                    };
                    content += protocolAction.dump() + "\n" + metadataAction.dump() + "\n";
                }
                content += commitInfo.dump() + "\n" + addAction.dump() + "\n";
                
                fwrite(content.c_str(), 1, content.size(), f);
                fclose(f);
                wrote_log = true;
                buf.commit_version++;
                buf.uncompacted_commits++;
                buf.schema_changed = false;
            } else {
                std::cerr << "[writer] fdopen failed" << std::endl;
                close(fd);
                break;
            }
        }
        
        if (!wrote_log) {
            std::cerr << "[writer] Fatal error: Could not write delta log due to OCC conflict or IO error." << std::endl;
            exit(1);
        }

        // Compaction
        if (buf.uncompacted_commits >= 10) {
            std::string cmd = "python3 delta_compact.py " + table_dir + " &";
            system(cmd.c_str());
            buf.uncompacted_commits = 0;
        }

        checkpoint.last_confirmed_lsn = buf.end_lsn;
        checkpoint.last_confirmed_time = current_timestamp_string();
        checkpoint.files_written++;
        checkpoint_manager.save_checkpoint(checkpoint);

        std::cout << "[writer] ✓ " << final_path << " (" << buf.row_count << " rows, " << buf.estimated_bytes << " bytes est.)" << std::endl;
    }
    buf.reset();
}

ParquetWriter::ParquetWriter(const Config& config,
                             RingBuffer<CDCRow>& ring_buffer,
                             std::atomic<bool>& shutdown_flag,
                             CheckpointManager& checkpoint_manager,
                             Checkpoint& checkpoint)
    : config_(config),
      ring_buffer_(ring_buffer),
      shutdown_flag_(shutdown_flag),
      checkpoint_manager_(checkpoint_manager),
      checkpoint_(checkpoint) {}

void ParquetWriter::run() {
    Logger::info("ParquetWriter", "Parquet writer started.");
    std::unordered_map<std::string, TableBuffer> table_buffers;
    const size_t file_size_bytes = config_.file_size_mb * 1024 * 1024;
    auto last_flush_time = std::chrono::steady_clock::now();

    while (!shutdown_flag_.load()) {
        auto rows = ring_buffer_.pop_batch(1000, std::chrono::milliseconds(200));

        if (!rows.empty()) {
            Logger::info("ParquetWriter", "Dequeued batch of " + std::to_string(rows.size()) + " rows from RingBuffer.");
        }

        for (auto& row : rows) {
            try {
                append_row_to_buffer(row, table_buffers[row.schema->table_name]);
            } catch (const std::exception& e) {
                if (std::string(e.what()) == "SCHEMA_CHANGE_DETECTED") {
                    // Flush the buffer to Parquet
                    Logger::info("ParquetWriter", "Flushing buffer for schema evolution on table " + row.schema->table_name);
                    flush_table_buffer(table_buffers[row.schema->table_name], config_, checkpoint_, checkpoint_manager_);
                    
                    // Re-initialize buffer with the new schema and set the flag
                    table_buffers[row.schema->table_name].init(*row.schema);
                    table_buffers[row.schema->table_name].schema_changed = true;
                    
                    // Retry appending the row
                    append_row_to_buffer(row, table_buffers[row.schema->table_name]);
                } else {
                    Logger::info("ParquetWriter", "Error appending row: " + std::string(e.what()));
                }
            }
        }

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_flush_time).count();
        bool time_flush = (elapsed >= static_cast<long>(config_.flush_interval_sec));

        for (auto& [table_name, buf] : table_buffers) {
            if (buf.row_count == 0) continue;
            bool size_flush = (buf.estimated_bytes >= file_size_bytes);
            bool row_limit_flush = (config_.row_flush_threshold > 0 && buf.row_count >= config_.row_flush_threshold);

            if (size_flush || time_flush || row_limit_flush) {
                std::string reason = row_limit_flush ? "Row Limit Reached (" + std::to_string(config_.row_flush_threshold) + ")" : (size_flush ? "Size Limit Reached" : "Time Limit Reached");
                Logger::info("ParquetWriter", "Flush condition met for table '" + table_name + "': " + reason + ". Initiating Parquet write for " + std::to_string(buf.row_count) + " rows.");
                
                flush_table_buffer(buf, config_, checkpoint_, checkpoint_manager_);
                
                Logger::info("ParquetWriter", "Successfully flushed and checkpointed table '" + table_name + "'.");
            }
        }

        if (time_flush) {
            last_flush_time = std::chrono::steady_clock::now();
        }
    }

    Logger::info("ParquetWriter", "Shutdown — flushing remaining buffers...");
    while (true) {
        auto rows = ring_buffer_.pop_batch(1000, std::chrono::milliseconds(100));
        if (rows.empty()) break;
        for (auto& row : rows) {
            try {
                append_row_to_buffer(row, table_buffers[row.schema->table_name]);
            } catch (const std::exception& e) {
                if (std::string(e.what()) == "SCHEMA_CHANGE_DETECTED") {
                    flush_table_buffer(table_buffers[row.schema->table_name], config_, checkpoint_, checkpoint_manager_);
                    table_buffers[row.schema->table_name].init(*row.schema);
                    table_buffers[row.schema->table_name].schema_changed = true;
                    append_row_to_buffer(row, table_buffers[row.schema->table_name]);
                }
            }
        }
    }

    for (auto& [table_name, buf] : table_buffers) {
        flush_table_buffer(buf, config_, checkpoint_, checkpoint_manager_);
    }

    Logger::info("ParquetWriter", "Shutdown complete. Total files: " + std::to_string(checkpoint_.files_written));
}

void ParquetWriter::flush_all() {
    Logger::info("ParquetWriter", "Entering flush_all");
    /* Flush logic has been abstracted to static helpers */
}
