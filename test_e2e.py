import psycopg2
import pyarrow.parquet as pq
import time
import os
import glob
import subprocess
import shutil
import pandas as pd

# Configuration
DB_HOST = "localhost"
DB_PORT = 9712
DB_NAME = "practice"
DB_USER = "documentdb"
OUTPUT_DIR = "/data/cdc_lake/wal_test"
CONFIG_FILE = "cdc_data_lake.conf"
APP_BIN = "build/cdc_data_lake"

def setup_db():
    print("[1/5] Setting up database...")
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS wal_test;")
    cur.execute("""
        CREATE TABLE wal_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            score INT
        );
    """)
    
    cur.execute("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'cdc_lake_slot' AND active = true;")
    cur.execute("SELECT pg_drop_replication_slot('cdc_lake_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'cdc_lake_slot');")
    cur.execute("DROP PUBLICATION IF EXISTS cdc_pub;")
    
    cur.close()
    conn.close()

def clean_output():
    print("[2/5] Cleaning old parquet files...")
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if os.path.exists("/data/cdc_lake/checkpoint.json"):
        os.remove("/data/cdc_lake/checkpoint.json")

def generate_data():
    print("[3/5] Generating data in PostgreSQL...")
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)
    conn.autocommit = True
    cur = conn.cursor()
    
    for i in range(1, 11):
        cur.execute("INSERT INTO wal_test (name, score) VALUES (%s, %s)", (f"Player_{i}", i * 10))
    
    cur.execute("UPDATE wal_test SET score = 999 WHERE id = 5;")
    cur.execute("DELETE FROM wal_test WHERE id = 8;")
    
    cur.close()
    conn.close()

def wait_for_parquet():
    print("[4/5] Waiting for Parquet file to be generated (max 70s)...")
    for i in range(70):
        files = glob.glob(os.path.join(OUTPUT_DIR, "*.parquet"))
        if files:
            print(f"Found parquet file: {files[0]}")
            return files[0]
        time.sleep(1)
    
    raise TimeoutError("No Parquet file was generated in time.")

def verify_parquet(file_path):
    print("[5/5] Verifying Parquet file contents...")
    table = pq.read_table(file_path)
    df = table.to_pandas()
    
    print("\n--- Parquet Metadata ---")
    print(table.schema.metadata)
    
    print("\n--- Parquet Data ---")
    print(df)
    
    assert len(df) == 12, f"Expected 12 CDC operations, found {len(df)}"
    assert "_cdc_operation" in df.columns, "Missing _cdc_operation column"
    assert "_cdc_lsn" in df.columns, "Missing _cdc_lsn column"
    assert "_cdc_timestamp" in df.columns, "Missing _cdc_timestamp column"
    
    print("\n✅ Verification SUCCESS!")

if __name__ == "__main__":
    setup_db()
    clean_output()
    
    print("[*] Starting cdc_data_lake application...")
    proc = subprocess.Popen([f"./{APP_BIN}", "--config", CONFIG_FILE], 
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    try:
        # Give it a second to connect and start replication
        time.sleep(2)
        generate_data()
        
        file_path = wait_for_parquet()
        verify_parquet(file_path)
        
    finally:
        print("[*] Terminating cdc_data_lake application...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        
        print("\n--- App Logs ---")
        out, _ = proc.communicate()
        print(out)
