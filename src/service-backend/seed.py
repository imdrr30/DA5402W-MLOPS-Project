import os
import sys
import time
import psycopg2

SEED_DIR = os.environ.get("SEED_DIR", "/seed")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "ecomm-postgres"),
    "user": os.environ.get("DB_USER", "ecomm"),
    "password": os.environ.get("DB_PASSWORD", "ecomm"),
    "dbname": os.environ.get("DB_NAME", "ecommdb"),
}


def wait_for_db(retries: int = 30, delay: int = 2) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Database is ready.")
            return conn
        except psycopg2.OperationalError:
            print(f"DB not ready, retrying ({attempt}/{retries})...")
            time.sleep(delay)
    print("Could not connect to database. Exiting.")
    sys.exit(1)


def already_seeded(conn: psycopg2.extensions.connection) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM categories")
        return cur.fetchone()[0] > 0


def seed(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        print("Seeding categories...")
        with open(os.path.join(SEED_DIR, "amazon_categories.csv"), "r", encoding="utf-8") as f:
            cur.copy_expert(
                "COPY categories (id, category_name) FROM STDIN WITH (FORMAT CSV, HEADER)", f
            )

        print("Seeding products (1.4M rows, this may take a minute)...")
        with open(os.path.join(SEED_DIR, "amazon_products.csv"), "r", encoding="utf-8") as f:
            cur.copy_expert(
                """COPY products (asin, title, img_url, product_url, stars, reviews,
                   price, list_price, category_id, is_best_seller, bought_in_last_month)
                   FROM STDIN WITH (FORMAT CSV, HEADER, NULL '')""",
                f,
            )

        print("Seeding users...")
        with open(os.path.join(SEED_DIR, "users.csv"), "r", encoding="utf-8") as f:
            cur.copy_expert(
                """COPY users (id, first_name, last_name, email, phone_number,
                   region, country, hashed_password, is_deleted)
                   FROM STDIN WITH (FORMAT CSV, HEADER)""",
                f,
            )

        print("Seeding events...")
        with open(os.path.join(SEED_DIR, "events.csv"), "r", encoding="utf-8") as f:
            cur.copy_expert(
                """COPY events (id, visitor_id, user_id, timestamp, event_type,
                   product_id, order_id_for_refund, is_recommended)
                   FROM STDIN WITH (FORMAT CSV, HEADER)""",
                f,
            )

    conn.commit()
    print("Seed complete.")


if __name__ == "__main__":
    conn = wait_for_db()
    try:
        if already_seeded(conn):
            print("Database already seeded, skipping.")
        else:
            seed(conn)
    finally:
        conn.close()
