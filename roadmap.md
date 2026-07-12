# Project Roadmap: Operationalization & Observability

This roadmap outlines the plan to transform the pg_cdc_datalake pipeline into a highly mature, easily deployable, and observable service while strictly adhering to a high-performance, pure C++ architecture.

## 1. Embedded Web Dashboard & Configuration 🎛️
Instead of spinning up a heavy Node.js or Python backend, we will keep the service 100% C++ by embedding a lightweight C++ HTTP server (e.g., `cpp-httplib`) directly into the daemon.
* **The UI:** Build a modern Vanilla HTML/CSS/JS frontend (with rich aesthetics, dark mode, and glassmorphism) served directly from the C++ binary.
* **Configuration API:** The UI will make API calls to the C++ backend to read the current `cdc_data_lake.conf`, present it in a nice form, and save changes back to disk.

## 2. Real-Time Metrics Page 📊
Since the HTTP server will run inside the same memory space as our pipeline, we can expose real-time metrics with zero overhead.
* **Atomic Counters:** Add `std::atomic` counters to the `WalReceiver` and `ParquetWriter` (e.g., `rows_ingested`, `files_written`, `bytes_processed`, `current_lsn`).
* **Metrics API:** The embedded HTTP server will expose an `/api/metrics` JSON endpoint.
* **Live Dashboard:** The frontend dashboard will poll this endpoint and display live, dynamic charts (e.g., throughput over time) and progress bars.

## 3. The Docker Image 🐳
Create a multi-stage `Dockerfile` to make deployment trivial.
* **Stage 1 (Builder):** Pulls a base image, installs `cmake`, `libpq`, and `apache-arrow/parquet` libraries, and compiles the C++ application.
* **Stage 2 (Runtime):** A very lightweight image that only contains the compiled binary and the bare minimum shared libraries. 
This allows users to run the pipeline anywhere with a single `docker run` command, eliminating the headache of installing C++ dependencies.
