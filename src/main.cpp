/*
 * cdc_data_lake — Standalone C++ CDC-to-Parquet Pipeline
 *
 * Connects to PostgreSQL via libpq, streams WAL changes using the built-in
 * pgoutput logical decoding plugin, and writes them as compressed Parquet
 * files for Spark/Trino/Presto consumption.
 *
 * Architecture:
 *   Thread 1 (WAL Receiver): Connects to PG replication slot, decodes
 *            pgoutput binary protocol, pushes CDCRow objects into ring buffer.
 *   Thread 2 (Parquet Writer): Pops rows from ring buffer, accumulates in
 *            Apache Arrow columnar arrays, writes ~1MB Parquet files with
 *            Snappy compression and CDC metadata.
 *
 * Usage:
 *   ./cdc_data_lake --config /path/to/cdc_data_lake.conf
 */

#include "checkpoint.h"
#include "config.h"
#include "parquet_writer.h"
#include "pg_setup.h"
#include "ring_buffer.h"
#include "types.h"
#include "snapshot_manager.h"
#include "wal_receiver.h"
#include "logger.h"

#include <atomic>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

/* Global shutdown flag — set by signal handlers */
static std::atomic<bool> g_shutdown{false};

/* Self-pipe to wake up the main thread instantly on signal */
static int g_shutdown_pipe[2];

static void signal_handler(int signum) {
    const char* name = (signum == SIGINT) ? "SIGINT" : "SIGTERM";
    /* write() is async-signal-safe, unlike std::cout */
    const char* msg = "\n[main] Shutdown signal received. Flushing buffers...\n";
    write(STDOUT_FILENO, msg, strlen(msg));
    g_shutdown.store(true);
    
    /* Write to the self-pipe to wake up the main thread instantly */
    char c = 'x';
    write(g_shutdown_pipe[1], &c, 1);
}

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " --config <path/to/cdc_data_lake.conf>" << std::endl;
}

static std::string parse_args(int argc, char* argv[]) {
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
            config_path = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return "";
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            print_usage(argv[0]);
            return "";
        }
    }

    if (config_path.empty()) {
        std::cerr << "Error: --config argument is required." << std::endl;
        print_usage(argv[0]);
    }
    return config_path;
}

static void print_startup_banner(const Config& config) {
    std::cout << "╔══════════════════════════════════════════════╗" << std::endl;
    std::cout << "║         CDC Data Lake — Parquet Pipeline     ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════╝" << std::endl;
    std::cout << "  Database:    " << config.pg_database << " @ "
              << config.pg_host << ":" << config.pg_port << std::endl;
    std::cout << "  Slot:        " << config.slot_name << std::endl;
    std::cout << "  Publication: " << config.publication_name << std::endl;
    std::cout << "  Output:      " << config.output_dir << std::endl;
    std::cout << "  File size:   " << config.file_size_mb << " MB" << std::endl;
    std::cout << "  Flush every: " << config.flush_interval_sec << " sec" << std::endl;
    if (!config.tables.empty()) {
        std::cout << "  Tables:      ";
        for (size_t i = 0; i < config.tables.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << config.tables[i];
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

static void setup_signal_handlers() {
    if (pipe(g_shutdown_pipe) == -1) {
        perror("pipe");
        exit(1);
    }
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

int main(int argc, char* argv[]) {
    std::string config_path = parse_args(argc, argv);
    if (config_path.empty()) {
        return 1;
    }

    Config config;
    try {
        config = parse_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "Error loading config: " << e.what() << std::endl;
        return 1;
    }

    Logger::init(config.log_file, config.log_to_console);
    print_startup_banner(config);
    setup_signal_handlers();

    try {
        PgSetup setup(config);
        setup.setup();
    } catch (const std::exception& e) {
        std::cerr << "Error during PostgreSQL setup: " << e.what() << std::endl;
        return 1;
    }

    std::filesystem::create_directories(config.output_dir);
    
    CheckpointManager checkpoint_manager(config.checkpoint_file);
    Checkpoint checkpoint = checkpoint_manager.load_checkpoint();

    RingBuffer<CDCRow> ring_buffer(10000);

    SnapshotManager snapshot_manager(config, ring_buffer, g_shutdown);

    Logger::info("Main", "Starting pipeline threads...");
    
    WalReceiver receiver(config, ring_buffer, g_shutdown, checkpoint.last_confirmed_lsn, &snapshot_manager);
    std::thread receiver_thread(&WalReceiver::run, &receiver);

    ParquetWriter writer(config, ring_buffer, g_shutdown, checkpoint_manager, checkpoint);
    std::thread writer_thread(&ParquetWriter::run, &writer);

    Logger::info("Main", "Pipeline threads started. Press Ctrl+C to shutdown.");

    char dummy;
    if (read(g_shutdown_pipe[0], &dummy, 1) == -1) {
        // Ignored
    }

    Logger::info("Main", "Shutting down...");
    ring_buffer.shutdown();
    
    if (receiver_thread.joinable()) {
        receiver_thread.join();
    }
    if (writer_thread.joinable()) {
        writer_thread.join();
    }

    checkpoint_manager.save_checkpoint(checkpoint);
    Logger::info("Main", "Pipeline stopped. Total files written: " + std::to_string(checkpoint.files_written));
    
    Logger::shutdown();
    return 0;
}
