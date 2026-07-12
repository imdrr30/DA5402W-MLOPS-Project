import argparse
import os
import string
import time
from datetime import datetime

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Static reference data
# --------------------------------------------------------------------------

EVENT_TYPES = ["view", "add_to_cart", "delete_from_cart", "quantity", "transaction", "refund_request"]
EVENT_TYPE_WEIGHTS = [0.55, 0.18, 0.08, 0.07, 0.09, 0.03]

RECOMMENDATION_VIEWS = ["home", "product_detail", "final_cart", "notification"]

REGIONS_COUNTRIES = [
    ("North America", "USA"), ("North America", "Canada"), ("North America", "Mexico"),
    ("Europe", "UK"), ("Europe", "Germany"), ("Europe", "France"), ("Europe", "Spain"), ("Europe", "Italy"),
    ("Asia", "China"), ("Asia", "India"), ("Asia", "Japan"), ("Asia", "South Korea"), ("Asia", "Singapore"), ("Asia", "UAE"),
    ("South America", "Brazil"), ("South America", "Argentina"),
    ("Africa", "Nigeria"), ("Africa", "South Africa"),
    ("Oceania", "Australia"), ("Oceania", "New Zealand"),
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan", "Rohan", 
    "Ananya", "Diya", "Saanvi", "Aadhya", "Kavya", "Priya", "Anika", "Riya", "Meera", "Neha", 
    "Rahul", "Amit", "Sanjay", "Deepak", "Pooja", "Sunita", "Lakshmi", "Divya", "Karan", "Arnav",
    "John", "Jane", "Emma", "Liam", "Olivia", "Noah", "Sophia", "Lucas", "Mia", "James", 
    "Emily", "Michael", "Sarah", "David", "Laura", "Mei", "Wei", "Yuki", "Hiro", "Jin", 
    "Ling", "Haruto", "Sakura", "Min-jun", "Ji-woo", "Ahmed", "Fatima", "Ali", "Layla", "Omar", 
    "Yasmin", "Hassan", "Zainab", "Carlos", "Sofia", "Diego", "Valentina", "Mateo", "Camila", "Santiago", 
    "Isabella", "Chidi", "Ngozi", "Kwame", "Amara", "Tendai", "Zola", "Ivan", "Nadia", "Lukas", 
    "Elena", "Marco", "Anna", "Pierre", "Claire"
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Patel", "Reddy", "Iyer", "Nair", "Menon", "Rao", "Chopra", 
    "Malhotra", "Kapoor", "Joshi", "Desai", "Mehta", "Agarwal", "Bhatt", "Kulkarni", "Pillai", "Chatterjee",
    "Smith", "Brown", "Wilson", "Taylor", "Anderson", "Thomas", "Moore", "Martin", "Chen", "Wang", 
    "Tanaka", "Suzuki", "Kim", "Park", "Nakamura", "Li", "Khan", "Al-Farsi", "Hassan", "Ibrahim", 
    "Said", "El-Masry", "Silva", "Garcia", "Lopez", "Martinez", "Rodriguez", "Fernandez", "Okafor", "Mensah", 
    "Ndlovu", "Adeyemi", "Muller", "Rossi", "Novak", "Petrov", "Kowalski", "Dubois"
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "protonmail.com", "rediffmail.com", "yandex.com", "aol.com", "example.com",
]

PRODUCT_TYPES = {
    "computers": ["Mouse", "Keyboard", "Monitor", "Laptop Stand", "Webcam", "USB Hub", "Graphics Card"],
    "kitchen": ["Toaster", "Juicer", "Blender", "Coffee Maker", "Air Fryer", "Rice Cooker", "Kettle"],
    "furniture": ["Office Chair", "Standing Desk", "Bookshelf", "Sofa", "Bed Frame", "Dining Table", "Wardrobe"],
    "electronics": ["Bluetooth Speaker", "Smartwatch", "Headphones", "Power Bank", "Tablet", "Smart TV", "Router"],
    "fitness": ["Yoga Mat", "Dumbbell Set", "Resistance Bands", "Treadmill", "Foam Roller", "Jump Rope", "Kettlebell"],
    "books": ["Novel", "Cookbook", "Biography", "Sci-Fi Anthology", "Self-Help Guide", "Comic Collection"],
    "clothing": ["Running Shoes", "Denim Jacket", "Wool Sweater", "Rain Coat", "T-Shirt Pack", "Formal Shirt"],
    "toys": ["Building Blocks", "RC Car", "Puzzle Set", "Action Figure", "Board Game", "Plush Toy"],
    "beauty": ["Face Serum", "Hair Dryer", "Electric Razor", "Makeup Kit", "Perfume", "Face Mask Set"],
    "others": ["Umbrella", "Backpack", "Water Bottle", "Desk Lamp", "Travel Pillow"],
    "sports": ["Cricket Bat", "Football", "Badminton Racket", "Basketball", "Tennis Racket", "Cycling Helmet"],
    "groceries": ["Basmati Rice Pack", "Organic Honey", "Green Tea Box", "Spice Mix Set", "Cold-Pressed Oil"],
    "pet_supplies": ["Dog Leash", "Cat Scratching Post", "Pet Bed", "Aquarium Filter", "Chew Toy"],
    "automotive": ["Car Vacuum", "Dash Cam", "Tire Inflator", "Seat Cover Set", "Phone Mount"],
    "garden": ["Garden Hose", "Pruning Shears", "Plant Pot Set", "Lawn Mower", "Watering Can"],
    "office_supplies": ["Notebook Set", "Stapler", "Whiteboard", "Desk Organizer", "Printer Paper"],
    "health": ["Digital Thermometer", "Blood Pressure Monitor", "First Aid Kit", "Vitamin Pack"],
    "baby": ["Stroller", "Baby Monitor", "Diaper Bag", "High Chair", "Baby Carrier"],
    "jewelry": ["Silver Necklace", "Gold-Plated Earrings", "Charm Bracelet", "Wrist Watch", "Ring Set"],
    "musical_instruments": ["Acoustic Guitar", "Digital Piano Keyboard", "Drum Pad", "Violin", "Ukulele"],
}
CATEGORIES = list(PRODUCT_TYPES.keys())

START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 6, 30)
TOTAL_SECONDS_RANGE = int((END_DATE - START_DATE).total_seconds())

USER_ID_NULL_RATE = 0.20
PRODUCT_ID_NULL_RATE = 0.05
IS_RECOMMENDED_RATE = 0.15
USER_DELETED_RATE = 0.05

ALNUM = np.array(list(string.ascii_uppercase + string.digits))

# --------------------------------------------------------------------------
# Sequential User Journey Configuration
# --------------------------------------------------------------------------
SEQUENCES = [
    ["view", "view"],                              # Just browsing
    ["view", "add_to_cart", "view"],               # Cart consideration
    ["view", "add_to_cart", "transaction"],        # Direct conversion
    ["view", "add_to_cart", "quantity", "transaction"], # Bulk buy conversion
    ["view", "add_to_cart", "delete_from_cart"],   # Abandonment/Removal
    ["refund_request"]                             # Standard post-sale issue
]
SEQ_WEIGHTS = [0.35, 0.20, 0.20, 0.10, 0.12, 0.03]


def generate_users(n_users: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(1, n_users + 1)
    first = rng.choice(FIRST_NAMES, size=n_users)
    last = rng.choice(LAST_NAMES, size=n_users)
    region_country_idx = rng.integers(0, len(REGIONS_COUNTRIES), size=n_users)
    regions = np.array([REGIONS_COUNTRIES[i][0] for i in region_country_idx])
    countries = np.array([REGIONS_COUNTRIES[i][1] for i in region_country_idx])
    domains = rng.choice(EMAIL_DOMAINS, size=n_users)

    emails = np.array([f"{f.lower()}.{l.lower()}{i}@{d}" for f, l, i, d in zip(first, last, ids, domains)])
    phone_digits = rng.integers(0, 10, size=(n_users, 11))
    phones = np.array(["+" + "".join(row.astype(str)) for row in phone_digits])

    pw_chars = np.array(list(string.ascii_letters + string.digits))
    pw_idx = rng.integers(0, len(pw_chars), size=(n_users, 22))
    hashed_passwords = np.array(["$2b$12$" + "".join(pw_chars[row]) for row in pw_idx])
    is_deleted = rng.random(n_users) < USER_DELETED_RATE

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
    short_desc = np.array([f"High quality {pt.lower()} model-{i} built for daily use." for pt, i in zip(product_types, ids)])
    long_desc = np.array([f"Experience premium performance with the {n}." for n in names])
    features = np.array([f"Premium Quality/Durable Build/Version v{i}" for i in ids])
    prices = np.round(rng.uniform(9.99, 2999.99, size=n_products), 2)

    created_offsets = rng.integers(0, TOTAL_SECONDS_RANGE, size=n_products)
    created_on = pd.to_datetime(START_DATE) + pd.to_timedelta(created_offsets, unit="s")
    update_offsets = rng.integers(0, 60 * 86400, size=n_products)
    updated_on = created_on + pd.to_timedelta(update_offsets, unit="s")

    sig_ids = rng.integers(10000000, 99999999, size=n_products)
    image_urls = np.array([f"https://images.unsplash.com/photo-15{s}?auto=format&fit=crop&w=500&q=80&sig={i}" for s, i in zip(sig_ids, ids)])

    return pd.DataFrame({
        "id": ids, "name": names, "short_description": short_desc, "long_description": long_desc,
        "features": features, "category": categories, "image_url": image_urls, "price": prices,
        "created_on": created_on.strftime("%Y-%m-%d %H:%M:%S"), "updated_on": updated_on.strftime("%Y-%m-%d %H:%M:%S"),
    })


def generate_events_chunk(chunk_size: int, start_id: int, n_users: int, n_products: int, rng: np.random.Generator) -> pd.DataFrame:
    # Estimate sessions needed to hit chunk_size safely (Avg length ~2.7)
    num_sessions = int(chunk_size / 2.3) + 2000
    seq_idx = rng.choice(len(SEQUENCES), size=num_sessions, p=SEQ_WEIGHTS)
    
    # Fast flattening of chosen session structures
    flat_events = [evt for i in seq_idx for evt in SEQUENCES[i]]
    lengths = np.array([len(SEQUENCES[i]) for i in seq_idx])
    total_events = np.sum(lengths)
    
    # Build localized step indexing for monotonic chronological increments
    cumsum = np.cumsum(lengths)
    step_indices = np.ones(total_events, dtype=int)
    step_indices[0] = 0
    if len(lengths) > 1:
        step_indices[cumsum[:-1]] -= lengths[:-1]
    step_indices = np.cumsum(step_indices)
    
    # Establish Session Identity Profile
    anon_mask = rng.random(num_sessions) < USER_ID_NULL_RATE
    user_ids = rng.integers(1, n_users + 1, size=num_sessions).astype(object)
    user_ids[anon_mask] = ""
    
    session_ids = np.arange(start_id, start_id + num_sessions)
    session_visitor_ids = np.where(anon_mask, np.array([f"visitor_{sid}" for sid in session_ids]), "")
    
    product_null_mask = rng.random(num_sessions) < PRODUCT_ID_NULL_RATE
    product_ids = rng.integers(1, n_products + 1, size=num_sessions).astype(object)
    product_ids[product_null_mask] = ""
    
    # Monotonic Timestamp offsets inside the session timeline
    base_offsets = rng.integers(0, TOTAL_SECONDS_RANGE - (4 * 300), size=num_sessions)
    event_base_offsets = np.repeat(base_offsets, lengths)
    
    # Actions follow sequentially, 90s apart + up to 60s random variability
    random_deltas = step_indices * 90 + rng.integers(5, 60, size=total_events)
    random_deltas[step_indices == 0] = 0  # Align first event step exactly with base session time
    event_final_offsets = event_base_offsets + random_deltas
    
    is_recommended = rng.random(num_sessions) < IS_RECOMMENDED_RATE
    recommendation_view = np.where(is_recommended, rng.choice(RECOMMENDATION_VIEWS, size=num_sessions), "")
    
    # Slice arrays down to match exact chunk boundaries
    event_event_types = np.array(flat_events)[:chunk_size]
    event_user_ids = np.repeat(user_ids, lengths)[:chunk_size]
    event_visitor_ids = np.repeat(session_visitor_ids, lengths)[:chunk_size]
    event_product_ids = np.repeat(product_ids, lengths)[:chunk_size]
    event_final_offsets = event_final_offsets[:chunk_size]
    event_is_recommended = np.repeat(is_recommended, lengths)[:chunk_size]
    event_recommendation_view = np.repeat(recommendation_view, lengths)[:chunk_size]
    
    # Business rule safeguard: Only show recommendations on 'view' events
    event_recommendation_view = np.where((event_event_types == "view"), event_recommendation_view, "")
    
    ids = np.arange(start_id, start_id + chunk_size)
    timestamps = pd.to_datetime(START_DATE) + pd.to_timedelta(event_final_offsets, unit="s")
    
    # Handle refund specific order IDs
    refund_mask = event_event_types == "refund_request"
    order_ids = np.full(chunk_size, "", dtype=object)
    n_refunds = int(refund_mask.sum())
    if n_refunds > 0:
        letters_idx = rng.integers(0, len(ALNUM), size=(n_refunds, 8))
        refund_orders = np.array(["ORD-" + "".join(row) for row in ALNUM[letters_idx]])
        order_ids[refund_mask] = refund_orders
        
    return pd.DataFrame({
        "id": ids,
        "visitor_id": event_visitor_ids,
        "user_id": event_user_ids,
        "timestamp": timestamps.strftime("%Y-%m-%dT%H:%M:%S"),
        "event_type": event_event_types,
        "product_id": event_product_ids,
        "order_id_for_refund": order_ids,
        "is_recommended": event_is_recommended,
        "recommendation_view": event_recommendation_view,
    })


def generate_events_to_csv(path: str, n_events: int, n_users: int, n_products: int, chunk_size: int, rng: np.random.Generator) -> None:
    n_written = 0
    first_chunk = True
    t_start = time.time()

    while n_written < n_events:
        this_chunk = min(chunk_size, n_events - n_written)
        df = generate_events_chunk(chunk_size=this_chunk, start_id=n_written + 1, n_users=n_users, n_products=n_products, rng=rng)
        df.to_csv(path, mode="w" if first_chunk else "a", header=first_chunk, index=False)
        n_written += this_chunk
        first_chunk = False
        elapsed = time.time() - t_start
        rate = n_written / elapsed if elapsed > 0 else 0
        print(f"  events.csv: {n_written:,}/{n_events:,} rows ({n_written / n_events:.1%}) | {rate:,.0f} rows/sec | {elapsed:.1f}s elapsed")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce CSV data.")
    parser.add_argument("--n-users", type=int, default=1000)
    parser.add_argument("--n-products", type=int, default=200)
    parser.add_argument("--n-events", type=int, default=5000)
    parser.add_argument("--chunk-size", type=int, default=500_000)
    parser.add_argument("--out-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    users_path = os.path.join(args.out_dir, "output/users.csv")
    products_path = os.path.join(args.out_dir, "output/products.csv")
    events_path = os.path.join(args.out_dir, "output/events.csv")

    print(f"Generating users.csv ({args.n_users:,} rows)...")
    t0 = time.time()
    generate_users(args.n_users, rng).to_csv(users_path, index=False)
    print(f"  done in {time.time() - t0:.1f}s")

    print(f"Generating products.csv ({args.n_products:,} rows)...")
    t0 = time.time()
    generate_products(args.n_products, rng).to_csv(products_path, index=False)
    print(f"  done in {time.time() - t0:.1f}s")

    print(f"Generating events.csv ({args.n_events:,} rows)...")
    t0 = time.time()
    generate_events_to_csv(events_path, args.n_events, args.n_users, args.n_products, args.chunk_size, rng)
    print(f"  done in {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
