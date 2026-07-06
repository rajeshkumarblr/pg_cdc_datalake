import pytest
import glob
import os
import time
import pyarrow.parquet as pq

DATA_DIR = "data"

def wait_for_parquet(table_name, max_sec=10):
    for _ in range(max_sec):
        files = glob.glob(os.path.join(DATA_DIR, table_name, "*.parquet"))
        if files:
            return files[0]
        time.sleep(1)
    raise TimeoutError(f"No Parquet file found for {table_name}")

def test_delta_cdf_schema(clean_env, run_daemon):
    """Verify that the generated Parquet files contain the required CDF metadata."""
    run_daemon.start()
    
    clean_env.execute("INSERT INTO products (id, name, category, price) VALUES (1, 'TestProduct', 'A', 10.0)")
    clean_env.execute("UPDATE products SET price = 20.0 WHERE id = 1")
    clean_env.execute("DELETE FROM products WHERE id = 1")
    
    # Wait for Postgres to stream the WAL events over the replication slot
    time.sleep(1)
    
    # Stop daemon to flush
    run_daemon.stop()
    
    file_path = wait_for_parquet("products")
    table = pq.read_table(file_path)
    df = table.to_pandas()
    
    assert "_change_type" in df.columns
    assert "_commit_version" in df.columns
    assert "_commit_timestamp" in df.columns
    
    assert len(df) == 3 # Insert, Update, Delete
    
    ops = df["_change_type"].tolist()
    assert ops == ["insert", "update_postimage", "delete"]
    
    prices = df["price"].tolist()
    assert prices == [10.0, 20.0, 20.0]  # delete contains the last known values

def test_snapshot_handoff(clean_env, run_daemon):
    """Verify that SnapshotManager cleanly captures pre-existing data without duplication."""
    # Pre-seed 100 rows BEFORE daemon starts
    for i in range(100):
        clean_env.execute("INSERT INTO products (id, name, category, price) VALUES (%s, %s, %s, %s)",
                          (i, f"Prod{i}", "B", i * 1.5))
    
    run_daemon.start()
    
    # Insert 10 more rows AFTER daemon started
    for i in range(100, 110):
        clean_env.execute("INSERT INTO products (id, name, category, price) VALUES (%s, %s, %s, %s)",
                          (i, f"Prod{i}", "B", i * 1.5))
    
    run_daemon.stop()
    
    file_path = wait_for_parquet("products")
    df = pq.read_table(file_path).to_pandas()
    
    # Total rows should be exactly 110
    assert len(df) == 110

def test_graceful_shutdown(clean_env, run_daemon):
    """Verify the daemon exits instantly (code 0) and saves the checkpoint on SIGTERM."""
    run_daemon.start()
    
    start_time = time.time()
    code = run_daemon.stop(timeout=2) # Should exit instantly, not timeout
    end_time = time.time()
    
    assert code == 0, f"Daemon exited with non-zero status {code}"
    assert (end_time - start_time) < 1.0, "Daemon took too long to shutdown"
    
    # Verify checkpoint exists
    assert os.path.exists(os.path.join(DATA_DIR, ".checkpoint"))
