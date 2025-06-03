import psycopg2
import json
import csv
import logging
from datetime import datetime

# Log faylni sozlash
logging.basicConfig(filename='product_db.log', level=logging.INFO, format='%(asctime)s - %(message)s')

class ProductDB:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.conn = psycopg2.connect(
            dbname="postgres", user="postgres", password="123", host="192.168.100.11", port=5432
        )
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                quantity INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def add_product(self, name, price, quantity):
        self.cursor.execute("""
            INSERT INTO products (name, price, quantity) VALUES (%s, %s, %s)
        """, (name, price, quantity))
        self.conn.commit()
        logging.info(f"Product added: {name}, {price}, {quantity}")

    def get_all_products(self):
        self.cursor.execute("SELECT * FROM products")
        return self.cursor.fetchall()

    def get_product_by_id(self, product_id):
        self.cursor.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
        return self.cursor.fetchone()

    def update_product(self, product_id, name=None, price=None, quantity=None):
        update_fields = []
        params = []
        if name is not None:
            update_fields.append("name = %s")
            params.append(name)
        if price is not None:
            update_fields.append("price = %s")
            params.append(price)
        if quantity is not None:
            update_fields.append("quantity = %s")
            params.append(quantity)

        if update_fields:
            params.append(product_id)
            query = f"UPDATE products SET {', '.join(update_fields)} WHERE id = %s"
            self.cursor.execute(query, tuple(params))
            self.conn.commit()
            logging.info(f"Product updated (id={product_id}): {update_fields}")

    def delete_product(self, product_id):
        self.cursor.execute("DELETE FROM products WHERE id = %s", (product_id,))
        self.conn.commit()
        logging.info(f"Product deleted: id={product_id}")

    def search_by_name(self, keyword):
        self.cursor.execute("SELECT * FROM products WHERE name ILIKE %s", (f'%{keyword}%',))
        return self.cursor.fetchall()

    def filter_by_price(self, min_price, max_price):
        self.cursor.execute("""
            SELECT * FROM products WHERE price BETWEEN %s AND %s
        """, (min_price, max_price))
        return self.cursor.fetchall()

    def export_to_json(self, filename="products.json"):
        products = self.get_all_products()
        keys = ["id", "name", "price", "quantity", "created_at"]
        data = [dict(zip(keys, prod)) for prod in products]
        with open(filename, "w") as f:
            json.dump(data, f, default=str, indent=4)
        logging.info("Exported to JSON")

    def export_to_csv(self, filename="products.csv"):
        products = self.get_all_products()
        with open(filename, mode="w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "price", "quantity", "created_at"])
            writer.writerows(products)
        logging.info("Exported to CSV")

    def close(self):
        self.cursor.close()
        self.conn.close()
        logging.info("Database connection closed")
