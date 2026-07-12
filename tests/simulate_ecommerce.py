import psycopg2
import time
import random
import threading

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


def setup_schema():
    print("Setting up e-commerce schema...")
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Create tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10, 2)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                customer_id INT,
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                id SERIAL PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                price_at_purchase DECIMAL(10, 2)
            );
        """)
        
        # Add them to publication if not already added
        # Ensure we drop it if it exists so we have a clean publication
        try:
            cur.execute("DROP PUBLICATION IF EXISTS cdc_pub;")
        except Exception:
            pass

        try:
            cur.execute("CREATE PUBLICATION cdc_pub FOR TABLE products, orders, order_items;")
        except Exception as e:
            print(f"Publication creation error: {e}")

        # Seed products if empty
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            print("Seeding initial products...")
            products = [
                ('Laptop Pro', 'Electronics', 1299.99),
                ('Wireless Mouse', 'Electronics', 49.99),
                ('Coffee Maker', 'Home', 89.99),
                ('Desk Chair', 'Office', 199.99),
                ('Mechanical Keyboard', 'Electronics', 149.99),
                ('Water Bottle', 'Home', 19.99),
                ('Notebook', 'Office', 5.99),
                ('Headphones', 'Electronics', 249.99),
                ('Monitor', 'Electronics', 399.99),
                ('Desk Lamp', 'Office', 35.00)
            ]
            for p in products:
                cur.execute("INSERT INTO products (name, category, price) VALUES (%s, %s, %s)", p)
    conn.close()
    print("Schema setup complete.")


def simulate_orders():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    print("Starting order simulator thread...")
    while True:
        try:
            # Create a new order
            customer_id = random.randint(1, 1000)
            cur.execute("INSERT INTO orders (customer_id, status) VALUES (%s, %s) RETURNING id", (customer_id, 'PENDING'))
            order_id = cur.fetchone()[0]
            
            # Fetch some random products
            cur.execute("SELECT id, price FROM products ORDER BY RANDOM() LIMIT %s", (random.randint(1, 4),))
            products = cur.fetchall()
            
            for prod in products:
                qty = random.randint(1, 3)
                cur.execute("INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase) VALUES (%s, %s, %s, %s)", 
                            (order_id, prod[0], qty, prod[1]))
            
            conn.commit()
            time.sleep(random.uniform(0.01, 0.1)) # Rapid inserts
        except Exception as e:
            print(f"Error in order thread: {e}")
            conn.rollback()
            time.sleep(1)


def simulate_updates():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    print("Starting update simulator thread...")
    while True:
        try:
            # Occasionally update order statuses
            if random.random() < 0.5:
                cur.execute("SELECT id FROM orders WHERE status = 'PENDING' ORDER BY RANDOM() LIMIT 5")
                orders = cur.fetchall()
                for o in orders:
                    cur.execute("UPDATE orders SET status = 'SHIPPED' WHERE id = %s", (o[0],))
                
            # Occasionally update a product price
            if random.random() < 0.1:
                cur.execute("SELECT id, price FROM products ORDER BY RANDOM() LIMIT 1")
                prod = cur.fetchone()
                if prod:
                    new_price = float(prod[1]) * random.uniform(0.9, 1.1)
                    cur.execute("UPDATE products SET price = %s WHERE id = %s", (new_price, prod[0]))
            
            conn.commit()
            time.sleep(random.uniform(0.5, 2.0))
        except Exception as e:
            print(f"Error in update thread: {e}")
            conn.rollback()
            time.sleep(1)

if __name__ == '__main__':
    setup_schema()
    
    print("\n[!] Schema created. Starting traffic simulation automatically...")
    
    t1 = threading.Thread(target=simulate_orders)
    t2 = threading.Thread(target=simulate_updates)
    
    t1.daemon = True
    t2.daemon = True
    
    t1.start()
    t2.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping simulator...")
