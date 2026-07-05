#!/bin/bash
set -e

echo "1. Cleaning environment..."
pkill -f cdc_data_lake || true
pkill -f simulate_ecommerce.py || true
rm -rf data/*
rm -f data/.checkpoint
psql -d ecommerce -p 9712 -c "SELECT pg_drop_replication_slot('pg_cdc_datalake_slot');" || true

echo "2. Setting up 100 products..."
python3 setup_100_products.py

echo "3. Starting CDC Data Lake in background..."
./build/cdc_data_lake --config cdc_data_lake.conf > data/cdc.log 2>&1 &
CDC_PID=$!

echo "Waiting 5 seconds for snapshot to finish and replication slot to be created..."
sleep 5

echo "4. Streaming 50 new products..."
python3 stream_50_products.py

echo "5. Waiting 5 seconds for WAL flush to reach CDC..."
sleep 5

echo "6. Stopping CDC gracefully to trigger parquet flush..."
kill -TERM $CDC_PID
wait $CDC_PID || true

echo "7. Running analytics..."
python3 analytics.py
