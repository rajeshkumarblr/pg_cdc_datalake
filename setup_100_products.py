import psycopg2

DB_PARAMS = {
    'dbname': 'ecommerce',
    'user': 'documentdb',
    'host': 'localhost',
    'port': 9712
}

def setup_100_products():
    print("Setting up initial 100 products...")
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE orders, order_items, products CASCADE;")
        
        # Insert 100 products
        for i in range(1, 101):
            cur.execute("""
                INSERT INTO products (id, name, category, price) 
                VALUES (%s, %s, %s, %s)
            """, (i, f"Product {i}", "Category A", i * 10.0))
            
    print("Inserted 100 products successfully.")

if __name__ == "__main__":
    setup_100_products()
