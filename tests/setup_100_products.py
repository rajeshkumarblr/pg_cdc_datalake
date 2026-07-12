import psycopg2

def get_db_params():
    params = {'dbname': 'ecommerce', 'host': 'localhost', 'port': 5432, 'user': 'cdc', 'password': 'cdc_password'}
    try:
        with open("cdc_data_lake.conf", "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k == "pg_host":
                        params['host'] = v
                    elif k == "pg_port":
                        params['port'] = int(v)
                    elif k == "pg_database":
                        params['dbname'] = v
                    elif k == "pg_user":
                        params['user'] = v
                    elif k == "pg_password":
                        params['password'] = v
    except Exception:
        pass
    return params

DB_PARAMS = get_db_params()


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
