# Stage 1: Build the C++ executable
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libpq-dev \
    libarrow-dev \
    libparquet-dev \
    nlohmann-json3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build the project
RUN cmake -B build . && make -j4 -C build

# Stage 2: Runtime image
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies
# Note: we use libarrow-dev and libparquet-dev here to ensure exact SO dependencies are met
RUN apt-get update && apt-get install -y \
    libpq5 \
    libarrow-dev \
    libparquet-dev \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir deltalake

WORKDIR /app

# Copy the compiled binary from builder
COPY --from=builder /app/build/cdc_data_lake /app/cdc_data_lake

# Copy necessary runtime assets
COPY dashboard /app/dashboard
COPY cdc_data_lake.conf /app/cdc_data_lake.conf
COPY delta_compact.py /app/delta_compact.py

# Expose Web Dashboard port
EXPOSE 8080

CMD ["/app/cdc_data_lake", "--config", "/app/cdc_data_lake.conf"]
