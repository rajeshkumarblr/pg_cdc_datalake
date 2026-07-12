cmake_minimum_required(VERSION 3.16)
project(cdc_data_lake CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Find PostgreSQL (for libpq) ──────────────────────────────────────────────
# Use pg_config to locate the include and library directories.
execute_process(
    COMMAND pg_config --includedir
    OUTPUT_VARIABLE PG_INCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND pg_config --libdir
    OUTPUT_VARIABLE PG_LIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS "PostgreSQL include: ${PG_INCLUDEDIR}")
message(STATUS "PostgreSQL lib:     ${PG_LIBDIR}")

# ── Find Apache Arrow and Parquet ─────────────────────────────────────────────
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

message(STATUS "Arrow version: ${Arrow_VERSION}")
message(STATUS "Parquet found: yes")

# ── Source files ──────────────────────────────────────────────────────────────
set(SOURCES
    src/main.cpp
    src/config.cpp
    src/checkpoint.cpp
    src/pg_setup.cpp
    src/logical_decoder.cpp
    src/wal_receiver.cpp
    src/parquet_writer.cpp
    src/logger.cpp
    src/snapshot_manager.cpp
)

# ── Build the executable ─────────────────────────────────────────────────────
add_executable(cdc_data_lake ${SOURCES})

target_include_directories(cdc_data_lake PRIVATE
    ${PG_INCLUDEDIR}
    src/
    include/
)

target_link_directories(cdc_data_lake PRIVATE
    ${PG_LIBDIR}
)

target_link_libraries(cdc_data_lake PRIVATE
    pq                  # libpq (PostgreSQL client library)
    Arrow::arrow_shared
    Parquet::parquet_shared
    pthread             # For std::thread
)

# ── Install ──────────────────────────────────────────────────────────────────
install(TARGETS cdc_data_lake DESTINATION bin)
