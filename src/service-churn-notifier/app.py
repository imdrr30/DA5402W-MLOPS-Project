"""
Churn Notifier Service
- Consumes user-events from Kafka
- Tracks per-user cart state in memory
- Detects cart abandonment (no activity for ABANDONMENT_MINUTES)
- Looks up customer value score from pre-computed features or MLflow champion model
- Simulates sending a personalised discount email (prints to console)
"""
import json
import logging
import os
import random
import string
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "user-events"
ABANDONMENT_MINUTES = int(os.environ.get("ABANDONMENT_MINUTES", "15"))
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "60"))
FEATURES_PATH = os.environ.get("FEATURES_PATH", "/features/user_value_features.csv")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "ecomm-postgres"),
    "user": os.environ.get("DB_USER", "ecomm"),
    "password": os.environ.get("DB_PASSWORD", "ecomm"),
    "dbname": os.environ.get("DB_NAME", "ecommdb"),
}

# Discount % by value label: High=small nudge, Low=big incentive
DISCOUNT_BY_LABEL = {2: 3, 1: 8, 0: 15}
DEFAULT_DISCOUNT = 5

# ---------------------------------------------------------------------------
# In-memory cart state
# {user_id: {items: set(), last_activity: datetime, notified: bool}}
# ---------------------------------------------------------------------------
cart_state: dict = defaultdict(lambda: {
    "items": set(),
    "last_activity": None,
    "notified": False,
})
state_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
EVENTS_PROCESSED = Counter('churn_events_processed_total', 'Total Kafka events processed by churn notifier')
CART_ABANDONMENTS = Counter('churn_cart_abandonments_total', 'Total cart abandonments detected')
NOTIFICATIONS_SENT = Counter('churn_notifications_sent_total', 'Total discount notifications sent')
ACTIVE_CARTS = Gauge('churn_active_carts', 'Number of active carts currently being tracked')

# ---------------------------------------------------------------------------
# Pre-computed user scores (loaded once at startup, refreshed periodically)
# ---------------------------------------------------------------------------
user_scores: dict = {}  # {user_id_str: label_int}


def load_user_scores() -> None:
    global user_scores
    if not os.path.exists(FEATURES_PATH):
        log.warning("Features file not found at %s — score lookup will use MLflow or default.", FEATURES_PATH)
        return
    try:
        df = pd.read_csv(FEATURES_PATH, usecols=["user_id", "value_label"])
        user_scores = dict(zip(df["user_id"].astype(str), df["value_label"].astype(int)))
        log.info("Loaded value scores for %d users from %s", len(user_scores), FEATURES_PATH)
    except Exception as e:
        log.warning("Could not load features CSV: %s", e)


def score_from_mlflow(user_id: str) -> int | None:
    """Try to get a score prediction from the MLflow @champion CustomerValueScorer."""
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        model_uri = "models:/CustomerValueScorer@champion"
        model = mlflow.sklearn.load_model(model_uri)
        # Minimal feature vector for a user with unknown history — worst-case defaults
        feature_cols = [
            "transaction_count", "add_count", "remove_count", "view_count",
            "refund_count", "unique_sessions", "unique_products", "conversion_rate",
            "removal_rate", "cart_abandon_rate", "recommendation_ctr",
            "days_since_last_tx", "events_per_day", "active_days",
        ]
        with state_lock:
            state = cart_state.get(user_id, {})
            n_items = len(state.get("items", set()))
        row = pd.DataFrame([{col: 0 for col in feature_cols}])
        row["add_count"] = n_items
        row["unique_sessions"] = 1
        label = int(model.predict(row)[0])
        return label
    except Exception as e:
        log.debug("MLflow score lookup failed for user %s: %s", user_id, e)
        return None


def get_user_score(user_id: str) -> int:
    label = user_scores.get(str(user_id))
    if label is None:
        label = score_from_mlflow(user_id)
    if label is None:
        log.debug("No score found for user %s — using default discount.", user_id)
        label = None
    return label


# ---------------------------------------------------------------------------
# Email simulation
# ---------------------------------------------------------------------------
def get_user_email(user_id: str) -> str | None:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT email FROM users WHERE id = %s AND is_deleted = FALSE", (user_id,))
            row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        log.warning("DB email lookup failed for user %s: %s", user_id, e)
        return None


def generate_coupon(discount: int) -> str:
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"SAVE{discount}-{suffix}"


def trigger_discount_notification(user_id: str) -> None:
    label = get_user_score(user_id)
    discount = DISCOUNT_BY_LABEL.get(label, DEFAULT_DISCOUNT)
    coupon = generate_coupon(discount)
    email = get_user_email(user_id) or f"user_{user_id}@unknown.com"

    log.info(
        "📧 MAIL TRIGGERED → %s | Coupon: %s (%d%% off) | Customer tier: %s",
        email,
        coupon,
        discount,
        {2: "High", 1: "Medium", 0: "Low"}.get(label, "Unknown"),
    )
    print(
        f"\n{'='*60}\n"
        f"  DISCOUNT EMAIL TRIGGERED\n"
        f"  To      : {email}\n"
        f"  Coupon  : {coupon}\n"
        f"  Discount: {discount}%\n"
        f"  Tier    : {({2: 'High', 1: 'Medium', 0: 'Low'}.get(label, 'Unknown'))}\n"
        f"  Reason  : Cart abandoned for >{ABANDONMENT_MINUTES} minutes\n"
        f"{'='*60}\n",
        flush=True,
    )
    NOTIFICATIONS_SENT.inc()


# ---------------------------------------------------------------------------
# Cart state update from Kafka event
# ---------------------------------------------------------------------------
def handle_event(event: dict) -> None:
    user_id = str(event.get("user_id", "")).strip()
    if not user_id or user_id in ("", "None", "nan"):
        return  # skip anonymous visitors

    event_type = event.get("event_type", "")
    product_id = str(event.get("product_id", "")).strip()
    now = datetime.utcnow()

    with state_lock:
        state = cart_state[user_id]
        state["last_activity"] = now

        if event_type == "add_to_cart" and product_id:
            state["items"].add(product_id)
            state["notified"] = False  # reset so next abandonment triggers again
        elif event_type == "delete_from_cart" and product_id:
            state["items"].discard(product_id)
        elif event_type == "transaction":
            state["items"].clear()
            state["notified"] = False

    # Update Prometheus metrics
    EVENTS_PROCESSED.inc()
    ACTIVE_CARTS.set(sum(1 for s in cart_state.values() if s["items"]))


# ---------------------------------------------------------------------------
# Background abandonment checker
# ---------------------------------------------------------------------------
def abandonment_checker() -> None:
    while True:
        time.sleep(CHECK_INTERVAL_SECONDS)
        cutoff = datetime.utcnow() - timedelta(minutes=ABANDONMENT_MINUTES)
        to_notify = []

        with state_lock:
            for user_id, state in cart_state.items():
                if (
                    state["items"]
                    and state["last_activity"]
                    and state["last_activity"] < cutoff
                    and not state["notified"]
                ):
                    to_notify.append(user_id)
                    state["notified"] = True  # prevent duplicate sends

        for user_id in to_notify:
            CART_ABANDONMENTS.inc()
            try:
                trigger_discount_notification(user_id)
            except Exception as e:
                log.error("Notification failed for user %s: %s", user_id, e)


# ---------------------------------------------------------------------------
# Kafka consumer loop
# ---------------------------------------------------------------------------
def start_consumer() -> None:
    consumer = None
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                group_id="churn-notifier",
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            log.info("Connected to Kafka broker at %s, consuming topic '%s'", KAFKA_BROKERS, TOPIC)
            break
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying (%d/30)...", attempt + 1)
            time.sleep(5)

    if consumer is None:
        log.error("Could not connect to Kafka after 30 attempts. Exiting.")
        return

    for message in consumer:
        try:
            handle_event(message.value)
        except Exception as e:
            log.error("Error handling event: %s", e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)

    load_user_scores()

    # Reload scores every 10 minutes in background (picks up new Airflow runs)
    def score_reloader():
        while True:
            time.sleep(600)
            load_user_scores()

    threading.Thread(target=score_reloader, daemon=True).start()
    threading.Thread(target=abandonment_checker, daemon=True).start()

    log.info(
        "Churn Notifier started | abandonment window: %d min | check interval: %ds",
        ABANDONMENT_MINUTES,
        CHECK_INTERVAL_SECONDS,
    )
    start_consumer()
