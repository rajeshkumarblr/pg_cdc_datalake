# AI Context Handoff

**Hello to the next AI Assistant!** 👋
If you are reading this, the user has recently moved this repository to a new workspace or devcontainer. This document provides the critical context of what we've built, our architectural philosophies, and the current state of the codebase.

## 🎯 Project Overview
This project (`pg_cdc_datalake`) is a **standalone, high-performance C++ daemon**. It connects to a PostgreSQL database via logical replication (`pgoutput`), decodes the WAL stream, and directly writes the changes to local storage as **Apache Parquet files** with **Delta Lake** metadata (JSON transaction logs).

## 🏛️ Architectural Philosophies (CRITICAL)
Please adhere to these guidelines when making future changes:
1. **Performance & Minimalism First:** We avoid heavy frameworks. For example, we implemented the embedded web server using the single-header `cpp-httplib` instead of bringing in massive dependencies like `prometheus-cpp`.
2. **Native C++ over External Scripts:** The core pipeline is 100% C++. We only use Python for occasional background maintenance tasks (like Delta log compaction via `delta_compact.py`).
3. **Standalone Daemon:** This is NOT a PostgreSQL extension (no PGXS required). It is a standalone client that connects over standard network ports. This allows it to run on separate worker nodes.
4. **Test-Driven:** Any new features or modifications MUST pass the existing test suite (`pytest tests/`) and include new tests if applicable.

## 🚀 Current State & Features
- **Logical Decoding:** Implemented in `src/logical_decoder.cpp`. Successfully decodes inserts, updates, deletes, and handles schema evolution dynamically (e.g. `ALTER TABLE`).
- **Memory Optimization:** We heavily optimized the `CDCRow` struct by using `std::shared_ptr<const TableSchema>` to prevent massive string copy overheads under high load.
- **Embedded Dashboard:** The daemon serves a gorgeous, glassmorphic HTML/CSS/JS dashboard (`dashboard/` directory) on port `8080`.
- **Prometheus Metrics:** We expose real-time metrics via `GET /metrics` in the standard Prometheus plain-text format.
- **Dynamic Configuration:** The `cdc_data_lake.conf` file can be read and overwritten on-the-fly via the `/api/config` REST endpoints.
- **Dockerization:** We have a multi-stage `Dockerfile` (Ubuntu 22.04 base) that builds the C++ binary and packages it into a minimal runtime image.

## 🗺️ Next Steps (Roadmap)
Phase 1 (Parquet/Delta core) and Phase 2 (Observability/Web Dashboard) are mostly complete. 
Depending on user priorities, the next focus might be:
1. Adding support for **Apache Iceberg** metadata generation (Phase 4).
2. Advanced compaction/vacuuming strategies for the Delta Lake storage.
3. Hardening error handling and automated recovery for production deployment.

*You're all caught up! You can delete or archive this file once you've absorbed the context.*
