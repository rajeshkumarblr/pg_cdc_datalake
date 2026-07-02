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

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

/*
 * Convert an operation enum to a human-readable string for the Parquet column.
 */
static const char* operation_to_string(Operation op) {
    switch (op) {
        case Operation::INSERT: return "INSERT";
        case Operation::UPDATE: return "UPDATE";
        case Operation::DELETE: return "DELETE";
        default: return "UNKNOWN";
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
 * TableBuffer accumulates rows for a single PostgreSQL table
 * before they are flushed into a Parquet file.
 */
struct TableBuffer {
    std::string table_name;
    TableSchema schema;

    std::shared_ptr<arrow::StringBuilder>  op_builder;
    std::shared_ptr<arrow::Int64Builder>   lsn_builder;
    std::shared_ptr<arrow::Int64Builder>   ts_builder;
    std::vector<std::shared_ptr<arrow::StringBuilder>> col_builders;

    uint64_t start_lsn  = 0;
    uint64_t end_lsn    = 0;
    int64_t  start_time = 0;
    int64_t  end_time   = 0;
    size_t   row_count  = 0;
    size_t   estimated_bytes = 0;
    size_t   commit_version = 0;

    bool initialized = false;

    void init(const TableSchema& s) {
        schema = s;
        table_name = s.table_name;
        op_builder  = std::make_shared<arrow::StringBuilder>();
        lsn_builder = std::make_shared<arrow::Int64Builder>();
        ts_builder  = std::make_shared<arrow::Int64Builder>();

        col_builders.clear();
        for (size_t i = 0; i < s.columns.size(); ++i) {
            col_builders.push_back(std::make_shared<arrow::StringBuilder>());
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

        for (auto& b : col_builders) {
            b = std::make_shared<arrow::StringBuilder>();
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
 * Helper to append a single row to a table buffer.
 */
static void append_row_to_buffer(const CDCRow& row, TableBuffer& buf) {
    Logger::info("ParquetWriter", "Entering append_row_to_buffer for table: " + row.table_name);
    if (!buf.initialized) {
        buf.init(row.schema);
        std::cout << "[writer] Initialized buffer for table '"
                  << row.table_name << "' ("
                  << row.schema.columns.size() << " columns)" << std::endl;
    }

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

    const auto& values = (row.operation == Operation::DELETE) ? row.old_values : row.new_values;

    for (size_t i = 0; i < buf.col_builders.size(); ++i) {
        if (i < values.size() && values[i].has_value()) {
            (void)buf.col_builders[i]->Append(values[i].value());
            buf.estimated_bytes += values[i].value().size();
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
    fields.push_back(arrow::field("_cdc_operation", arrow::utf8(), false));
    fields.push_back(arrow::field("_cdc_lsn", arrow::int64(), false));
    fields.push_back(arrow::field("_cdc_timestamp", arrow::int64(), false));

    for (const auto& col : buf.schema.columns) {
        fields.push_back(arrow::field(col.name, arrow::utf8(), true));
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
        
        std::ostringstream version_ss;
        version_ss << std::setfill('0') << std::setw(20) << buf.commit_version;
        std::string delta_log_file = delta_log_dir + "/" + version_ss.str() + ".json";
        
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
        
        std::ofstream delta_out(delta_log_file);
        if (delta_out.is_open()) {
            if (buf.commit_version == 0) {
                // First commit needs protocol and metadata
                nlohmann::json protocolAction = {{"protocol", {{"minReaderVersion", 1}, {"minWriterVersion", 2}}}};
                
                nlohmann::json metadataAction = {
                    {"metaData", {
                        {"id", "cdc-" + buf.table_name},
                        {"format", {{"provider", "parquet"}, {"options", nlohmann::json::object()}}},
                        {"schemaString", ""}, // Optional for simple append
                        {"partitionColumns", nlohmann::json::array()},
                        {"configuration", nlohmann::json::object()}
                    }}
                };
                delta_out << protocolAction.dump() << "\n";
                delta_out << metadataAction.dump() << "\n";
            }
            delta_out << commitInfo.dump() << "\n";
            delta_out << addAction.dump() << "\n";
            delta_out.close();
            buf.commit_version++;
        } else {
            std::cerr << "[writer] failed to write delta log: " << delta_log_file << std::endl;
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
            append_row_to_buffer(row, table_buffers[row.table_name]);
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
            append_row_to_buffer(row, table_buffers[row.table_name]);
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
