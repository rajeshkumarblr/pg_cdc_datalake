import psycopg2

DB_PARAMS = {
    'dbname': 'ecommerce',
    'user': 'documentdb',
    'host': 'localhost',
    'port': 9712
}

def stream_50_products():
    print("Streaming 50 new products...")
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Insert 50 products
        for i in range(101, 151):
            cur.execute("""
                INSERT INTO products (id, name, category, price) 
                VALUES (%s, %s, %s, %s)
            """, (i, f"Product {i}", "Category B", i * 10.0))
            
    print("Streamed 50 products successfully.")

if __name__ == "__main__":
    stream_50_products()
