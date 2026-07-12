import pytest
import psycopg2
import subprocess
import time
import os
import shutil

DB_HOST = "localhost"
DB_PORT = 9712
DB_NAME = "ecommerce"
DB_USER = "documentdb"
CONFIG_FILE = "cdc_data_lake.conf"
APP_BIN = "build/cdc_data_lake"
DATA_DIR = "data"

@pytest.fixture(scope="session")
def db_connection():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)
    conn.autocommit = True
    yield conn
    conn.close()

@pytest.fixture(scope="function", autouse=True)
def clean_env(db_connection):
    cur = db_connection.cursor()
    
    # Alter products replica identity to FULL so we get old values on DELETE
    cur.execute("ALTER TABLE products REPLICA IDENTITY FULL;")

    # Drop existing replication slot if any
    try:
        cur.execute("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'pg_cdc_datalake_slot' AND active = true;")
        cur.execute("SELECT pg_drop_replication_slot('pg_cdc_datalake_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pg_cdc_datalake_slot');")
    except Exception as e:
        print("Ignoring slot cleanup error:", e)
    
    # Clean output data
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # TRUNCATE tables and reset schema changes
    try:
        cur.execute("ALTER TABLE products DROP COLUMN IF EXISTS description;")
    except Exception as e:
        pass
    cur.execute("TRUNCATE TABLE orders, order_items, products CASCADE;")
    
    yield cur
    
    # Cleanup
    cur.execute("TRUNCATE TABLE orders, order_items, products CASCADE;")
    cur.close()

@pytest.fixture
def run_daemon():
    class DaemonManager:
        def __init__(self):
            self.process = None

        def start(self, config=CONFIG_FILE):
            self.process = subprocess.Popen([f"./{APP_BIN}", "--config", config],
                                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            time.sleep(2) # wait for startup and snapshot export

        def stop(self, timeout=5):
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    return self.process.wait()
            return self.process.returncode

        def get_logs(self):
            if self.process:
                out, _ = self.process.communicate()
                return out
            return ""

    daemon = DaemonManager()
    yield daemon
    daemon.stop()
