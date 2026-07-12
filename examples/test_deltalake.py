from deltalake import DeltaTable
import os

try:
    dt = DeltaTable("data/order_items")
    print(dt.to_pandas())
except Exception as e:
    print(f"Error reading Delta Lake: {e}")
