# CDC Data Lake — Architecture & Design

This document provides a comprehensive overview of the **CDC Data Lake** architecture. It is a standalone C++ application that seamlessly streams Change Data Capture (CDC) events from PostgreSQL and outputs them as partitioned, compressed Parquet files optimized for analytics engines like Apache Spark, Trino, and Presto.

---

## 1. Architectural Overview

The application is designed using a **producer-consumer** architecture to decouple the fast I/O of reading from PostgreSQL from the CPU-intensive task of Parquet compression and serialization.

```mermaid
graph TD
    subgraph "PostgreSQL"
        WAL["Write-Ahead Log (WAL)"] --> Walsender["walsender (pgoutput)"]
        Snap["Table Data"] --> CopyOut["COPY TO STDOUT"]
    end

    subgraph "Standalone C++ Application (cdc_data_lake)"
        CopyOut -->|"Binary COPY"| SnapshotManager["Snapshot Manager"]
        Walsender -->|"libpq Replication Protocol"| WalReceiver["Thread 1: WAL Receiver"]
        SnapshotManager -->|"CDCRow objects"| RingBuffer["Lock-Free SPSC Ring Buffer"]
        WalReceiver -->|"CDCRow objects"| RingBuffer
        RingBuffer -->|"Batch Dequeue"| ParquetWriter["Thread 2: Parquet Writer"]
        ParquetWriter -->|"Delta CDF / Parquet Files"| Disk["Local Disk / Data Lake"]
        ParquetWriter -.->|"Checkpoint LSN"| CheckpointFile["checkpoint.json"]
    end

    subgraph "Analytics (Spark/Presto)"
        Disk --> Spark["Apache Spark / Delta Lake"]
    end
```

### Why this architecture?
1. **Zero Database Impact:** By using a standalone application, we eliminate the risk of crashing the PostgreSQL instance (a major risk with custom C extensions).
2. **Native Output Format:** Using PostgreSQL's built-in `pgoutput` plugin means we don't need to write any C-level decoding logic inside PostgreSQL.
3. **High Throughput:** The two-thread design ensures that Parquet compression (which uses Snappy/ZSTD and is CPU-heavy) does not block the thread receiving network packets from PostgreSQL.
4. **Data Lake Ready:** Spark and Presto prefer large (~1MB+) column-oriented files. Generating these files directly saves us from having to run intermediate compaction jobs.

---

## 2. Component Details

### A. The PostgreSQL pgoutput Plugin
The foundation of this CDC pipeline is PostgreSQL's logical decoding feature, specifically the built-in `pgoutput` plugin. 

- **Publication (`CREATE PUBLICATION`)**: Determines *which* tables' changes are streamed. It acts as a source-side filter.
- **Replication Slot**: Keeps track of how far the application has read in the WAL (Write-Ahead Log), ensuring that PostgreSQL does not delete WAL files before we've processed them.
- **Binary Protocol**: Instead of strings, `pgoutput` sends highly structured binary messages (Relation schemas, Begin/Commit transaction boundaries, and Insert/Update/Delete row operations).

### B. Configuration & Initialization (`config.cpp` & `pg_setup.cpp`)
When the application starts, it reads `cdc_data_lake.conf` and connects to PostgreSQL to automatically provision everything it needs:
1. Validates that `wal_level = logical` is set in `postgresql.conf`.
2. Creates the publication dynamically based on the configured table filters.
3. Creates the logical replication slot if it doesn't already exist.
This makes the application entirely self-bootstrapping.

### C. The Snapshot Manager (`snapshot_manager.cpp`)
Before streaming begins, if a table has no existing checkpoint, the `SnapshotManager` connects to PostgreSQL and runs `COPY (SELECT * FROM table) TO STDOUT (FORMAT BINARY)`.
- **Initial Load**: It streams all existing rows into the `RingBuffer` as `insert` operations.
- **Consistent Snapshot**: It uses a synchronized replication slot so the WAL stream picks up exactly where the snapshot left off.

### D. The WAL Receiver Thread (`wal_receiver.cpp`)
This is **Thread 1** (The Producer). 

- **LibPq Replication Mode**: Connects to PostgreSQL using `START_REPLICATION SLOT ... LOGICAL ...`.
- **Streaming Loop**: Uses the `select()` system call with `PQgetCopyData()` to read the replication stream without spinning the CPU.
- **pgoutput_parser.cpp**: This component parses the raw binary bytes from PostgreSQL. It maintains a cache of table schemas (from 'Relation' messages) so it knows column names and types. It translates 'Insert', 'Update', and 'Delete' messages into a structured C++ `CDCRow` object.
- **Keepalives**: Periodically sends heartbeat messages (Standby Status Updates) back to PostgreSQL, acknowledging the highest LSN (Log Sequence Number) we've seen to prevent timeouts.

### E. Lock-Free SPSC Ring Buffer (`ring_buffer.h`)
This is the **bridge** between the producers (SnapshotManager/WalReceiver) and Thread 2.

- **SPSC**: Single-Producer, Single-Consumer.
- **Thread-Safe**: Uses mutexes and condition variables (optimized with lock-free atomic index tracking) to allow Thread 1 to safely hand off `CDCRow` objects to Thread 2.
- **Batching**: Allows the consumer to pull up to 1000 rows at a time, drastically reducing lock contention and improving throughput.

### F. The Parquet Writer Thread (`parquet_writer.cpp`)
This is **Thread 2** (The Consumer). It is responsible for accumulating rows and compressing them into Parquet files formatted for Delta Lake CDF.

In this component, high-throughput conversion from row-oriented PostgreSQL data to column-oriented analytics format takes place:
- **`TableBuffer` Accumulation**: The writer maintains a distinct in-memory `TableBuffer` for each PostgreSQL table it tracks. As rows (`CDCRow`) are dequeued from the `RingBuffer`, they are immediately routed to their corresponding `TableBuffer`.
- **Apache Arrow Builders**: Inside the `TableBuffer`, data is appended to in-memory columnar structures using `arrow::StringBuilder` and `arrow::Int64Builder`. Arrow efficiently pivots the row-based event stream into a dense column-based memory representation.
- **Flush Conditions**: The writer periodically iterates over all active `TableBuffers` and checks two flush conditions:
  1. **Size Limit**: Has the buffer accumulated the configured file size limit (e.g., ~1MB of data)?
  2. **Time Limit**: Has the buffer hit the timeout limit (e.g., 60 seconds since the last flush)?
- **Parquet File Writing (`flush_table_buffer`)**: When a flush condition is met, the `TableBuffer` finalizes the Arrow arrays, constructing an `arrow::Table`. This table is encoded to disk with **Snappy compression**, embedding key schema metadata and CDC boundaries (like start/end LSN and timestamp boundaries) directly into the Parquet footer.
- **Spark-Friendly Partitioning**: Files are segregated into isolated directories named identically to the source table (e.g., `output_dir/my_table/...`).
- **Atomic Renames**: To guarantee data integrity for downstream analytics engines (like Apache Spark or Trino), files are initially staged with a `.tmp` extension. Only when the file is completely encoded and closed is an atomic system rename performed to change the extension to `.parquet`.

### G. Checkpointing & Crash Recovery (`checkpoint.cpp`)
To ensure **At-Least-Once Delivery** and zero data loss during crashes:

- Whenever the Parquet Writer successfully renames a `.tmp` file to `.parquet`, it records the `end_lsn` of that batch into a `checkpoint.json` file.
- When the application starts up, it reads `checkpoint.json`. Instead of asking PostgreSQL for all data from the beginning (`0/0`), it asks PostgreSQL to resume streaming exactly from the `last_confirmed_lsn`.
- This ensures that if the server crashes or loses power, the pipeline picks up right where it left off, and PostgreSQL knows it can safely delete old WAL files up to that point.

---

## 3. Data Schema and Metadata

Every generated Parquet file contains the native columns of the PostgreSQL table, plus metadata columns injected for **Delta Lake Change Data Feed (CDF)** compatibility:

1. **`_change_type`**: Indicates the operation type (`"insert"`, `"update_preimage"`, `"update_postimage"`, or `"delete"`).
2. **`_commit_version`**: The Log Sequence Number (LSN) identifying the exact position of the change in the WAL. This is mapped to Delta's commit version for deduplication and ordering.
3. **`_commit_timestamp`**: The transaction commit time in microseconds.

### Parquet Footer Metadata
In addition to column data, the Parquet file's internal key-value metadata footer contains:
- `cdc.start_lsn` / `cdc.end_lsn`: The WAL range contained in the file.
- `cdc.start_time` / `cdc.end_time`: The time range contained in the file.
- `cdc.row_count`: Total rows in the file.

This allows analytics tools to quickly skip files that aren't relevant to a specific timeframe or LSN range without having to scan the actual column data.

---

## 4. System Diagrams

### Class Diagram

The following diagram illustrates the primary classes, their responsibilities, and relationships within the standalone C++ application.

```mermaid
classDiagram
    class Config {
        +string pg_host
        +int pg_port
        +string pg_database
        +string pg_user
        +string pg_password
        +string publication_name
        +string slot_name
        +vector~string~ tables
        +string output_dir
        +string checkpoint_file
        +int batch_size
        +int flush_interval_sec
        +int ring_buffer_capacity
        +connection_string() string
    }

    class PgSetup {
        -Config config_
        +setup() void
    }

    class CheckpointManager {
        -string filepath_
        +load_checkpoint() Checkpoint
        +save_checkpoint(Checkpoint cp) void
    }

    class SnapshotManager {
        -Config config_
        -RingBuffer ring_buffer_
        +export_snapshot(string table_name) void
        -process_copy_data(char* data, int len) void
    }

    class LogicalDecoder {
        -unordered_map schemas_
        -uint64_t current_xact_lsn_
        -RowCallback row_callback_
        -CommitCallback commit_callback_
        +set_row_callback(RowCallback cb) void
        +set_commit_callback(CommitCallback cb) void
        +parse_message(char* data, int len) void
        +get_schema(uint32_t relation_id) TableSchema
    }

    class WalReceiver {
        -Config config_
        -RingBuffer ring_buffer_
        -uint64_t start_lsn_
        -atomic~uint64_t~ last_commit_lsn_
        +run() void
        +get_last_commit_lsn() uint64_t
        -connect_and_start() bool
        -stream_loop() void
        -send_standby_status(...) void
    }

    class ParquetWriter {
        -Config config_
        -RingBuffer ring_buffer_
        -CheckpointManager checkpoint_manager_
        -Checkpoint checkpoint_
        +run() void
        -flush_all() void
    }

    class RingBuffer~T~ {
        -vector~T~ buffer_
        -size_t head_
        -size_t tail_
        -mutex mtx_
        -condition_variable cv_
        +push(T item) bool
        +pop_batch(vector~T~& batch, size_t batch_size) bool
    }

    SnapshotManager --> RingBuffer : pushes CDCRow (initial load)
    WalReceiver --> LogicalDecoder : uses
    WalReceiver --> RingBuffer : pushes CDCRow (CDC)
    ParquetWriter --> RingBuffer : pops CDCRow
    ParquetWriter --> CheckpointManager : uses
    PgSetup --> Config : uses
    WalReceiver --> Config : uses
    ParquetWriter --> Config : uses
    SnapshotManager --> Config : uses
```

### Sequence Diagram: Normal Data Flow

This sequence diagram outlines the flow of data from PostgreSQL, through the pipeline, and onto disk as Delta CDF Parquet files.

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant SM as SnapshotManager
    participant WR as WalReceiver (Thread 1)
    participant LD as LogicalDecoder
    participant RB as RingBuffer
    participant PW as ParquetWriter (Thread 2)
    participant CM as CheckpointManager
    participant Disk as Data Lake (Delta CDF files)

    opt First Run (No Checkpoint)
        SM->>PG: COPY (SELECT * FROM table) TO STDOUT (BINARY)
        PG-->>SM: Binary Data
        SM->>RB: push(CDCRow with _change_type=insert)
    end

    PG->>WR: Logical Replication Stream (pgoutput binary)
    WR->>LD: parse_message(raw_bytes)
    
    alt is Relation schema
        LD-->>LD: Update TableSchema Cache
    else is INSERT/UPDATE/DELETE
        LD->>RB: push(CDCRow)
    else is COMMIT
        LD->>WR: update last_commit_lsn
    end

    loop Every Batch / Interval
        PW->>RB: pop_batch(up to 1000 rows)
        RB-->>PW: vector<CDCRow>
        
        PW->>PW: Accumulate in Arrow TableBuffer
        
        opt Size > 1MB or Interval > 60s
            PW->>Disk: Write .tmp Parquet file
            PW->>Disk: Rename .tmp -> .parquet
            PW->>CM: save_checkpoint(new LSN)
            CM->>Disk: update checkpoint.json
        end
    end

    loop Every 10 seconds
        WR->>PG: send_standby_status(last_commit_lsn)
        Note over WR,PG: Keeps WAL sender alive<br/>and recycles old WAL
    end
```

---

## 5. Testing Strategy

The repository includes a comprehensive `pytest` regression suite located in `tests/test_pipeline.py` designed to ensure that the pipeline remains resilient to database and OS edge cases.

- **`test_delta_cdf_schema`**: Spins up the daemon, injects row mutations, and strictly verifies that the resulting Parquet files conform to the Delta CDF specification (including `_change_type`, `_commit_timestamp`, and `_commit_version`).
- **`test_snapshot_handoff`**: Pre-seeds PostgreSQL with thousands of rows before daemon startup, and injects live stream changes post-startup to verify that the `SnapshotManager` seamlessly hands off to the `WalReceiver` without dropping or duplicating a single row.
- **`test_graceful_shutdown`**: Sends OS-level signals (`SIGTERM`) to verify that the daemon correctly catches the interrupt, bypasses spin-locks via the self-pipe trick, forcefully flushes pending Arrow buffers to Parquet, and persists the `.checkpoint` state safely before exiting.
