# PostgreSQL CDC to Delta Lake Pipeline

A high-performance C++ pipeline that captures Change Data Capture (CDC) events from PostgreSQL via logical replication and seamlessly writes them to an ACID-compliant Delta Lake (Parquet) format. This allows downstream systems like Apache Spark to perform real-time streaming analytics on live database changes.

## Architecture

The project implements a 3-tier architecture designed for extremely low latency and high throughput:

1. **WAL Receiver:** Connects to PostgreSQL using logical replication (via the `pgoutput` plugin). It transparently handles `EXPORT_SNAPSHOT` backfilling for historical data extraction and then streams live WAL events.
2. **Ring Buffer:** A lock-free, multi-producer/multi-consumer circular buffer that decouples database ingestion speeds from disk writing latencies.
3. **Parquet / Delta Writer:** Batches incoming events in memory and flushes them to disk as Snappy-compressed Apache Parquet files based on file size or time intervals. It automatically generates the Delta Lake `_delta_log` transaction JSON files so Spark can natively process the stream.

## Key Features

- **High-Speed Snapshotting:** Automatically extracts historical data using PostgreSQL's `EXPORT_SNAPSHOT` mechanism before starting the live stream.
- **Delta Lake Integration:** Writes fully compatible Delta protocol transaction logs.
- **Robust Checkpointing:** Uses a lightweight JSON checkpoint to track committed LSNs across restarts, ensuring at-least-once delivery semantics without data loss.
- **Configurable Batching:** Allows fine-tuning of Parquet flush intervals (by time, row count, or MB size limits) via a simple configuration file.
- **End-to-End Analytics Demo:** Includes Python scripts for simulating live eCommerce traffic and performing live streaming aggregations using PySpark.

## Dependencies

Ensure the following libraries are installed on your system before building:

- **PostgreSQL Client Libraries** (`libpq-dev`, `libpqxx-dev`)
- **Apache Arrow & Parquet** (`libarrow-dev`, `libparquet-dev`)
- **CMake** (v3.10+)
- **nlohmann-json** (for JSON parsing and Delta log generation)
- **OpenSSL**

## Building the Project

The pipeline is built using CMake:

```bash
mkdir -p build
cd build
cmake ..
make -j$(nproc)
```

## Configuration

All runtime configurations are managed through `cdc_data_lake.conf`. Ensure you adjust your database credentials, replication slot names, and output directories:

```ini
# PostgreSQL connection parameters
pg_host = localhost
pg_port = 5432
pg_database = ecommerce
pg_user = postgres

# Replication settings
slot_name = pg_cdc_datalake_slot
publication_name = cdc_pub

# Table filters (comma-separated). Empty = all tables in publication.
tables = products, orders, order_items

# Output settings
output_dir = ./data
file_size_mb = 1
flush_interval_sec = 60
row_flush_threshold = 1000

# Checkpoint file
checkpoint_file = ./data/.checkpoint
```

## Running the Pipeline

Before starting, ensure your PostgreSQL instance has `wal_level = logical` set. The application will automatically create the publication and replication slots if they do not exist.

```bash
./build/cdc_data_lake --config cdc_data_lake.conf
```

## End-to-End Demonstration

This repository comes with scripts to verify the complete pipeline using simulated data and Spark:

1. **Start the Data Generator:**
   Simulate a live eCommerce application generating random traffic.
   ```bash
   python3 simulate_ecommerce.py
   ```

2. **Start the CDC Pipeline:**
   Watch as the C++ engine ingests changes and flushes Parquet files.
   ```bash
   ./build/cdc_data_lake --config cdc_data_lake.conf
   ```

3. **Start Spark Analytics:**
   Stream the generated Delta Lake files using PySpark and observe live trending products.
   ```bash
   python3 spark_analytics.py
   ```
