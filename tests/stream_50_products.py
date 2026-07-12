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
