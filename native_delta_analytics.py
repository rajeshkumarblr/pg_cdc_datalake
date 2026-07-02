import time
import pandas as pd
from deltalake import DeltaTable

def main():
    print("Starting Native Delta Lake Analytics...")
    data_path = "/home/documentdb/pg_cdc_datalake/data/order_items"
    
    while True:
        try:
            # Load the Delta Table using the native deltalake (delta-rs) library
            dt = DeltaTable(data_path)
            
            # Convert to Pandas for analytics
            df = dt.to_pandas()
            
            if not df.empty:
                # Convert columns from string to numeric types
                df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
                df['price_at_purchase'] = pd.to_numeric(df['price_at_purchase'], errors='coerce')

                # Calculate total revenue and units sold per product
                df['total_price'] = df['quantity'] * df['price_at_purchase']
                summary = df.groupby('product_id').agg(
                    total_units_sold=('quantity', 'sum'),
                    total_revenue=('total_price', 'sum')
                ).reset_index()
                
                # Sort by revenue descending and show top 10
                summary = summary.sort_values('total_revenue', ascending=False).head(10)
                
                print(f"\n--- Top Trending Products (Version: {dt.version()}) ---")
                print(summary.to_string(index=False))
            else:
                print("\n[!] No data found yet.")
                
        except Exception as e:
            print(f"[!] Error reading Delta Lake (Waiting for initial data): {e}")
            
        time.sleep(10)

if __name__ == "__main__":
    main()
