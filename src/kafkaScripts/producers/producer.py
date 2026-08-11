"""
producer.py
-----------
Synthetic customer + event generator for the MLOps e-commerce project
described in ARCH.md.

What this script does differently from producer_1.py:
  * producer_1.py samples events independently and at random, so a
    given user/product pair has no coherent story (a "transaction"
    can appear with no preceding "view" or "add_to_cart").
  * This script instead simulates real customer SESSIONS. Each
    customer has a hidden persona (purchase_intent, indecisiveness,
    engagement_level) and every session follows the logical order:

        recommendation_shown (maybe)
          -> view (possibly several times -> repeat-view signal)
          -> add_to_cart
          -> delete_from_cart / add_to_cart churn loop (cart-churn signal)
          -> quantity + transaction   (purchase)
             OR left abandoned in the cart

    Abandoned carts are queued and followed up 1-3 days later with a
    notification-events sequence (discount_offer_sent / reminder_sent
    -> offer_accepted / offer_declined), matching ARCH.md Phase 5.

  * Because every event carries a session_id, the downstream Spark
    feature job can compute exactly the signals ARCH.md section 3.3
    asks for (view_count, add_to_cart_count, abandonment_count,
    cart_to_purchase_ratio, recommendation_engagement_rate) *and* the
    "repeat-view / cart-churn" signal requested for the purchase-intent
    scoring model (a customer who views + adds/removes a lot before
    buying, or never buys, looks very different from one who buys on
    the first pass).

Outputs (into --out-dir):
  users.csv               -- customer records (same shape as producer_1.py)
  products.csv             -- product catalog (same shape as producer_1.py)
  events.csv               -- session-consistent event stream
  customer_personas.csv    -- the HIDDEN ground-truth persona used to
                               drive the simulation (purchase_intent,
                               indecisiveness, engagement_level). Useful
                               to validate/evaluate the purchase-intent
                               model later -- do NOT feed it into
                               training, it would leak the label.

Usage:
  python producer.py --n-users 2000 --n-products 300 --n-days 21
  python producer.py --n-users 2000 --stream --kafka-broker localhost:9092
"""

import argparse
import os
import random
import string
import time
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Reference data -- kept consistent with producer_1.py so users.csv /
# products.csv can be loaded by the same downstream pipeline.
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan", "Rohan",
    "Ananya", "Diya", "Saanvi", "Aadhya", "Kavya", "Priya", "Anika", "Riya", "Meera", "Neha",
    "Rahul", "Amit", "Sanjay", "Deepak", "Pooja", "Sunita", "Lakshmi", "Divya", "Karan", "Arnav",
    "John", "Jane", "Emma", "Liam", "Olivia", "Noah", "Sophia", "Lucas", "Mia", "James",
    "Emily", "Michael", "Sarah", "David", "Laura", "Mei", "Wei", "Yuki", "Hiro", "Jin",
    "Ling", "Haruto", "Sakura", "Min-jun", "Ji-woo", "Ahmed", "Fatima", "Ali", "Layla", "Omar",
    "Yasmin", "Hassan", "Zainab", "Carlos", "Sofia", "Diego", "Valentina", "Mateo", "Camila", "Santiago",
    "Isabella", "Chidi", "Ngozi", "Kwame", "Amara", "Tendai", "Zola", "Ivan", "Nadia", "Lukas",
    "Elena", "Marco", "Anna", "Pierre", "Claire",
]
LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Patel", "Reddy", "Iyer", "Nair", "Menon", "Rao", "Chopra",
    "Malhotra", "Kapoor", "Joshi", "Desai", "Mehta", "Agarwal", "Bhatt", "Kulkarni", "Pillai", "Chatterjee",
    "Smith", "Brown", "Wilson", "Taylor", "Anderson", "Thomas", "Moore", "Martin", "Chen", "Wang",
    "Tanaka", "Suzuki", "Kim", "Park", "Nakamura", "Li", "Khan", "Al-Farsi", "Hassan", "Ibrahim",
    "Said", "El-Masry", "Silva", "Garcia", "Lopez", "Martinez", "Rodriguez", "Fernandez", "Okafor", "Mensah",
    "Ndlovu", "Adeyemi", "Muller", "Rossi", "Novak", "Petrov", "Kowalski", "Dubois",
]
REGIONS_COUNTRIES = [
    ("North America", "USA"), ("North America", "Canada"), ("North America", "Mexico"),
    ("Europe", "UK"), ("Europe", "Germany"), ("Europe", "France"), ("Europe", "Spain"), ("Europe", "Italy"),
    ("Asia", "China"), ("Asia", "India"), ("Asia", "Japan"), ("Asia", "South Korea"), ("Asia", "Singapore"), ("Asia", "UAE"),
    ("South America", "Brazil"), ("South America", "Argentina"),
    ("Africa", "Nigeria"), ("Africa", "South Africa"),
    ("Oceania", "Australia"), ("Oceania", "New Zealand"),
]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]

PRODUCT_TYPES = {
    "computers": ["Mouse", "Keyboard", "Monitor", "Laptop Stand", "Webcam"],
    "kitchen": ["Toaster", "Juicer", "Blender", "Coffee Maker", "Air Fryer"],
    "furniture": ["Office Chair", "Standing Desk", "Bookshelf", "Sofa", "Bed Frame"],
    "electronics": ["Bluetooth Speaker", "Smartwatch", "Headphones", "Power Bank", "Tablet"],
    "fitness": ["Yoga Mat", "Dumbbell Set", "Resistance Bands", "Treadmill"],
    "books": ["Novel", "Cookbook", "Biography", "Sci-Fi Anthology"],
    "clothing": ["Running Shoes", "Denim Jacket", "Wool Sweater", "T-Shirt Pack"],
    "toys": ["Building Blocks", "RC Car", "Puzzle Set", "Action Figure"],
    "beauty": ["Face Serum", "Hair Dryer", "Electric Razor", "Makeup Kit"],
    "others": ["Umbrella", "Backpack", "Water Bottle", "Desk Lamp"],
}
CATEGORIES = list(PRODUCT_TYPES.keys())
FOCUS_CATEGORIES = ["computers", "electronics"]  # customers browse these more often

RECOMMENDATION_VIEWS = ["home", "product_detail", "final_cart", "notification"]
ALNUM = np.array(list(string.ascii_uppercase + string.digits))

EVENT_TOPIC = {
    "view": "user-events",
    "add_to_cart": "user-events",
    "delete_from_cart": "user-events",
    "quantity": "user-events",
    "transaction": "user-events",
    "refund_request": "user-events",
    "recommendation_shown": "recommendation-actions",
    "recommendation_clicked": "recommendation-actions",
    "recommendation_accepted": "recommendation-actions",
    "recommendation_rejected": "recommendation-actions",
    "discount_offer_sent": "notification-events",
    "reminder_sent": "notification-events",
    "offer_accepted": "notification-events",
    "offer_declined": "notification-events",
}


# ---------------------------------------------------------------------------
# Static reference tables (customers / products) -- same shape as producer_1.py
# ---------------------------------------------------------------------------
def generate_users(n_users: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(1, n_users + 1)
    first = rng.choice(FIRST_NAMES, size=n_users)
    last = rng.choice(LAST_NAMES, size=n_users)
    rc_idx = rng.integers(0, len(REGIONS_COUNTRIES), size=n_users)
    regions = np.array([REGIONS_COUNTRIES[i][0] for i in rc_idx])
    countries = np.array([REGIONS_COUNTRIES[i][1] for i in rc_idx])
    domains = rng.choice(EMAIL_DOMAINS, size=n_users)
    emails = np.array([f"{f.lower()}.{l.lower()}{i}@{d}" for f, l, i, d in zip(first, last, ids, domains)])
    phone_digits = rng.integers(0, 10, size=(n_users, 11))
    phones = np.array(["+" + "".join(row.astype(str)) for row in phone_digits])
    pw_chars = np.array(list(string.ascii_letters + string.digits))
    pw_idx = rng.integers(0, len(pw_chars), size=(n_users, 22))
    hashed_passwords = np.array(["$2b$12$" + "".join(pw_chars[row]) for row in pw_idx])
    is_deleted = rng.random(n_users) < 0.05

    return pd.DataFrame({
        "id": ids, "first_name": first, "last_name": last, "email": emails,
        "phone_number": phones, "region": regions, "country": countries,
        "hashed_password": hashed_passwords, "is_deleted": is_deleted,
    })


def generate_products(n_products: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(1, n_products + 1)
    categories = rng.choice(CATEGORIES, size=n_products)
    product_types = np.array([rng.choice(PRODUCT_TYPES[c]) for c in categories])
    names = np.array([f"{pt} Model-{i}" for pt, i in zip(product_types, ids)])
    short_desc = np.array([f"High quality {pt.lower()} model-{i} built for durability." for pt, i in zip(product_types, ids)])
    long_desc = np.array([f"Experience premium performance with the {n}." for n in names])
    features = np.array([f"Premium Quality/Durable Build/Version v{i}" for i in ids])
    prices = np.round(rng.uniform(9.99, 2999.99, size=n_products), 2)
    start, end = datetime(2026, 1, 1), datetime(2026, 6, 30)
    total_seconds = int((end - start).total_seconds())
    created_offsets = rng.integers(0, total_seconds, size=n_products)
    created_on = pd.to_datetime(start) + pd.to_timedelta(created_offsets, unit="s")
    update_offsets = rng.integers(0, 60 * 86400, size=n_products)
    updated_on = created_on + pd.to_timedelta(update_offsets, unit="s")
    sig_ids = rng.integers(10000000, 99999999, size=n_products)
    image_urls = np.array([f"https://images.unsplash.com/photo-15{s}?auto=format&fit=crop&w=500&q=80&sig={i}" for s, i in zip(sig_ids, ids)])

    return pd.DataFrame({
        "id": ids, "name": names, "short_description": short_desc, "long_description": long_desc,
        "features": features, "category": categories, "image_url": image_urls, "price": prices,
        "created_on": created_on.strftime("%Y-%m-%d %H:%M:%S"), "updated_on": updated_on.strftime("%Y-%m-%d %H:%M:%S"),
    })


def assign_personas(n_users: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    'rand.cus' -- randomly assigns each customer a hidden behavioral
    persona that drives the simulation:

      purchase_intent  -- probability of converting a cart into a sale
                           (right-skewed: most customers are low-intent
                           browsers, a minority convert readily)
      indecisiveness    -- how often the customer re-views a product and
                           adds/removes it from the cart before deciding
      engagement_level  -- how many shopping sessions/day the customer has
    """
    purchase_intent = rng.beta(2, 5, size=n_users)          # skewed toward low intent
    indecisiveness = rng.beta(2, 2, size=n_users)            # roughly symmetric 0-1
    engagement_level = rng.choice(
        ["low", "medium", "high"], size=n_users, p=[0.50, 0.35, 0.15]
    )
    return pd.DataFrame({
        "user_id": np.arange(1, n_users + 1),
        "purchase_intent": np.round(purchase_intent, 4),
        "indecisiveness": np.round(indecisiveness, 4),
        "engagement_level": engagement_level,
    })


# ---------------------------------------------------------------------------
# Event stream simulation
# ---------------------------------------------------------------------------
class EventIdCounter:
    def __init__(self, start=1):
        self.value = start

    def next(self):
        v = self.value
        self.value += 1
        return v


def make_event(counter, session_id, user_id, ts, event_type, product_id="",
               order_id="", is_recommended=False, recommendation_view=""):
    return {
        "id": counter.next(),
        "topic": EVENT_TOPIC[event_type],
        "session_id": session_id,
        "visitor_id": "",
        "user_id": user_id,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "event_type": event_type,
        "product_id": product_id,
        "order_id_for_refund": order_id,
        "is_recommended": is_recommended,
        "recommendation_view": recommendation_view,
    }


def sessions_per_day(engagement_level: str, rng: np.random.Generator) -> int:
    low, high = {"low": (0, 2), "medium": (1, 3), "high": (2, 5)}[engagement_level]
    return int(rng.integers(low, high + 1))


def random_order_id(rng: np.random.Generator) -> str:
    idx = rng.integers(0, len(ALNUM), size=8)
    return "ORD-" + "".join(ALNUM[idx])


def simulate_customer_day(counter, user_id, persona, day_date, focus_ids, all_ids, rng):
    """
    Simulates every shopping session a single customer has on a single
    day, following the logical order:
      recommendation_shown -> view(s) -> add_to_cart ->
      [delete_from_cart / add_to_cart churn] -> transaction | abandon

    Returns (events, abandoned_carts) where abandoned_carts is a list of
    {user_id, product_id} dicts eligible for a follow-up notification.
    """
    events = []
    abandoned_carts = []
    n_sessions = sessions_per_day(persona["engagement_level"], rng)

    for s in range(n_sessions):
        session_id = f"u{user_id}-{day_date.isoformat()}-s{s}"

        pool = focus_ids if rng.random() < 0.8 and len(focus_ids) > 0 else all_ids
        product_id = int(rng.choice(pool))

        t = datetime(day_date.year, day_date.month, day_date.day,
                     int(rng.integers(0, 24)), int(rng.integers(0, 60)), int(rng.integers(0, 60)))

        is_rec = rng.random() < 0.30
        rec_view = ""
        if is_rec:
            rec_view = str(rng.choice(RECOMMENDATION_VIEWS))
            events.append(make_event(counter, session_id, user_id, t, "recommendation_shown",
                                      product_id=product_id, is_recommended=True, recommendation_view=rec_view))
            t += timedelta(seconds=int(rng.integers(1, 10)))
            if rng.random() < 0.6:
                events.append(make_event(counter, session_id, user_id, t, "recommendation_clicked",
                                          product_id=product_id, is_recommended=True, recommendation_view=rec_view))
                t += timedelta(seconds=int(rng.integers(1, 5)))
            else:
                events.append(make_event(counter, session_id, user_id, t, "recommendation_rejected",
                                          product_id=product_id, is_recommended=True, recommendation_view=rec_view))
                continue  # customer ignored the recommendation, session ends

        # -- repeat-view signal: more indecisive customers look longer --
        n_views = 1 + int(rng.poisson(persona["indecisiveness"] * 2))
        n_views = min(n_views, 5)
        for _ in range(n_views):
            events.append(make_event(counter, session_id, user_id, t, "view",
                                      product_id=product_id, is_recommended=is_rec, recommendation_view=rec_view))
            t += timedelta(seconds=int(rng.integers(5, 120)))

        add_to_cart_prob = 0.30 + 0.40 * persona["purchase_intent"]
        in_cart = rng.random() < add_to_cart_prob
        if not in_cart:
            continue

        events.append(make_event(counter, session_id, user_id, t, "add_to_cart", product_id=product_id))
        t += timedelta(seconds=int(rng.integers(5, 60)))

        # -- cart-churn signal: indecisive customers add/remove repeatedly --
        churn_rounds = min(int(rng.poisson(persona["indecisiveness"] * 1.5)), 3)
        for _ in range(churn_rounds):
            events.append(make_event(counter, session_id, user_id, t, "delete_from_cart", product_id=product_id))
            t += timedelta(seconds=int(rng.integers(10, 300)))
            if rng.random() < 0.7:
                events.append(make_event(counter, session_id, user_id, t, "add_to_cart", product_id=product_id))
                t += timedelta(seconds=int(rng.integers(10, 120)))
            else:
                in_cart = False
                break

        if not in_cart:
            continue  # customer removed it for good this session

        # more back-and-forth slightly erodes conversion probability
        purchase_prob = max(persona["purchase_intent"] * (1.0 - 0.12 * churn_rounds), 0.02)
        if rng.random() < purchase_prob:
            events.append(make_event(counter, session_id, user_id, t, "quantity", product_id=product_id))
            t += timedelta(seconds=int(rng.integers(2, 10)))
            events.append(make_event(counter, session_id, user_id, t, "transaction", product_id=product_id))
            t += timedelta(seconds=int(rng.integers(2, 10)))
            if rng.random() < 0.03:
                events.append(make_event(counter, session_id, user_id, t, "refund_request",
                                          product_id=product_id, order_id=random_order_id(rng)))
        else:
            # left in the cart -> abandoned -> candidate for a discount/reminder
            abandoned_carts.append({"user_id": user_id, "product_id": product_id})

    return events, abandoned_carts


def process_notifications(counter, pending_today, day_date, personas_by_id, rng):
    """
    Follows up on carts abandoned 1-3 days earlier (ARCH.md Phase 5):
    discount_offer_sent/reminder_sent -> offer_accepted (-> late purchase)
    or offer_declined. Published to the notification-events topic.
    """
    events = []
    for item in pending_today:
        user_id, product_id = item["user_id"], item["product_id"]
        persona = personas_by_id[user_id]
        t = datetime(day_date.year, day_date.month, day_date.day,
                     int(rng.integers(8, 20)), int(rng.integers(0, 60)), int(rng.integers(0, 60)))
        session_id = f"u{user_id}-{day_date.isoformat()}-notif"

        offer_type = "discount_offer_sent" if rng.random() < 0.6 else "reminder_sent"
        events.append(make_event(counter, session_id, user_id, t, offer_type, product_id=product_id))
        t += timedelta(minutes=int(rng.integers(5, 600)))

        accept_prob = 0.15 + 0.35 * persona["purchase_intent"]
        if rng.random() < accept_prob:
            events.append(make_event(counter, session_id, user_id, t, "offer_accepted", product_id=product_id))
            t += timedelta(seconds=int(rng.integers(30, 300)))
            events.append(make_event(counter, session_id, user_id, t, "quantity", product_id=product_id))
            t += timedelta(seconds=int(rng.integers(2, 10)))
            events.append(make_event(counter, session_id, user_id, t, "transaction", product_id=product_id))
        else:
            events.append(make_event(counter, session_id, user_id, t, "offer_declined", product_id=product_id))
    return events


def simulate_events(n_users, n_products, n_days, active_fraction, products_df, personas_df, rng):
    counter = EventIdCounter(start=1)
    focus_ids = products_df.loc[products_df["category"].isin(FOCUS_CATEGORIES), "id"].values
    all_ids = products_df["id"].values
    personas_by_id = {row["user_id"]: row for _, row in personas_df.iterrows()}

    # notify_queue[day_index] = list of {user_id, product_id} to follow up on
    # (abandoned carts can be scheduled up to 3 days after n_days, so pad the range)
    notify_queue = {d: [] for d in range(1, n_days + 4)}

    all_events = []
    start_date = date.today() - timedelta(days=n_days)

    for d in range(1, n_days + 1):
        day_date = start_date + timedelta(days=d)
        n_active = max(1, int(n_users * active_fraction))
        active_uids = rng.choice(np.arange(1, n_users + 1), size=n_active, replace=False)

        day_events = []
        for uid in active_uids:
            persona = personas_by_id[int(uid)]
            evs, abandoned = simulate_customer_day(counter, int(uid), persona, day_date, focus_ids, all_ids, rng)
            day_events.extend(evs)
            for a in abandoned:
                delay = int(rng.integers(1, 4))  # follow up 1-3 days later
                notify_queue.setdefault(d + delay, []).append(a)

        notif_events = process_notifications(counter, notify_queue.get(d, []), day_date, personas_by_id, rng)
        day_events.extend(notif_events)

        day_events.sort(key=lambda e: e["timestamp"])
        all_events.extend(day_events)
        print(f"  Day {d}/{n_days} ({day_date}): {len(day_events):,} events "
              f"(running total {len(all_events):,})")

    return pd.DataFrame(all_events)


# ---------------------------------------------------------------------------
# Kafka helper (optional, mirrors producer_1.py)
# ---------------------------------------------------------------------------
def get_kafka_producer(broker):
    try:
        from kafka import KafkaProducer
        import json as _json
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
        )
        print(f"Connected to Kafka broker at: {broker}")
        return producer
    except ImportError:
        print("[WARNING] 'kafka-python' not installed -- skipping Kafka publish, CSV output only.")
        return None
    except Exception as e:
        print(f"[WARNING] Could not connect to Kafka broker ({e}) -- CSV output only.")
        return None


def publish_to_kafka(producer, events_df):
    for _, row in events_df.iterrows():
        record = row.to_dict()
        producer.send(record["topic"], record)
    producer.flush()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Simulate thousands of customers and their view -> cart -> purchase "
                    "journeys for the MLOps e-commerce pipeline (ARCH.md)."
    )
    parser.add_argument("--n-users", type=int, default=1000, help="number of customers to create")
    parser.add_argument("--n-products", type=int, default=200, help="number of products to create")
    parser.add_argument("--n-days", type=int, default=15, help="number of days to simulate")
    parser.add_argument("--active-fraction", type=float, default=0.8,
                         help="fraction of customers active on any given day (0-1)")
    parser.add_argument("--out-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stream", action="store_true", help="also publish events to Kafka")
    parser.add_argument("--kafka-broker", type=str, default="localhost:9092")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)

    users_path = os.path.join(args.out_dir, "users.csv")
    products_path = os.path.join(args.out_dir, "products.csv")
    events_path = os.path.join(args.out_dir, "events.csv")
    personas_path = os.path.join(args.out_dir, "customer_personas.csv")

    t0 = time.time()

    print(f"Generating users.csv ({args.n_users:,} rows)...")
    users_df = generate_users(args.n_users, rng)
    users_df.to_csv(users_path, index=False)

    print(f"Generating products.csv ({args.n_products:,} rows)...")
    products_df = generate_products(args.n_products, rng)
    products_df.to_csv(products_path, index=False)

    print(f"Assigning {args.n_users:,} customer personas (rand.cus)...")
    personas_df = assign_personas(args.n_users, rng)
    personas_df.to_csv(personas_path, index=False)
    print("  (customer_personas.csv is SIMULATION GROUND TRUTH ONLY -- "
          "use it to evaluate the purchase-intent model, never as a training feature)")

    print(f"Simulating {args.n_days} days of behavior for up to {args.n_users:,} customers...")
    events_df = simulate_events(
        n_users=args.n_users,
        n_products=args.n_products,
        n_days=args.n_days,
        active_fraction=args.active_fraction,
        products_df=products_df,
        personas_df=personas_df,
        rng=rng,
    )
    events_df.to_csv(events_path, index=False)

    if args.stream:
        producer = get_kafka_producer(args.kafka_broker)
        if producer is not None:
            print(f"Publishing {len(events_df):,} events to Kafka...")
            publish_to_kafka(producer, events_df)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s.")
    print(f"  users.csv               : {len(users_df):,} rows")
    print(f"  products.csv            : {len(products_df):,} rows")
    print(f"  events.csv              : {len(events_df):,} rows")
    print(f"  customer_personas.csv   : {len(personas_df):,} rows")


if __name__ == "__main__":
    main()
