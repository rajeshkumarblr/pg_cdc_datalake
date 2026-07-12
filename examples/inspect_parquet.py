import sys
import pyarrow.parquet as pq

def inspect_parquet(file_path):
    print(f"Inspecting: {file_path}")
    print("-" * 50)
    
    try:
        table = pq.read_table(file_path)
        
        print("Schema:")
        for field in table.schema:
            print(f"  - {field.name}: {field.type}")
            
        print("\nMetadata:")
        metadata = table.schema.metadata
        if metadata:
            for k, v in metadata.items():
                print(f"  - {k.decode('utf-8')}: {v.decode('utf-8')}")
        else:
            print("  (No metadata found)")
            
        print(f"\nRow count: {table.num_rows}")
        print("\nFirst 5 rows:")
        df = table.to_pandas()
        print(df.head(5))
        
        print("-" * 50)
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_parquet.py <path_to_parquet_file>")
        sys.exit(1)
        
    for arg in sys.argv[1:]:
        inspect_parquet(arg)
