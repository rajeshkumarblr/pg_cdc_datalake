import sys
from deltalake import DeltaTable

def compact_delta_log(table_dir):
    try:
        dt = DeltaTable(table_dir)
        dt.create_checkpoint()
        print(f"[delta_compact] Successfully created checkpoint for {table_dir}")
    except Exception as e:
        print(f"[delta_compact] Failed to create checkpoint for {table_dir}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 delta_compact.py <table_dir>")
        sys.exit(1)
    
    compact_delta_log(sys.argv[1])
