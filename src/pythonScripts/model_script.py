import os
import pandas as pd
import glob
from mlxtend.frequent_patterns import apriori, association_rules
import numpy as np
import json
import traceback
import sys

# ==========================================
# --- CONFIGURATION & FILE PATHS ---
# ==========================================

# Base project directory
BASE_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in locals() else "."

# Base directory pointing to 'DB'
BASE_DB_DIR = os.path.join(BASE_PROJECT_DIR, "DB") if not os.path.exists("DB") else "DB"

# Directory where lookup data CSVs are stored
LOOKUP_DATA_DIR = os.path.join(BASE_PROJECT_DIR, "lookupData") if not os.path.exists("lookupData") else "lookupData"

# Directory where snapshots are saved by feature_script.py
SNAPSHOT_DIR = os.path.join(BASE_DB_DIR, "snapshot")

# Path to mapping file generated during ingestion/feature extraction
PRODUCT_MAP_PATH = os.path.join(BASE_DB_DIR, "product_id_map.json")

# --- Apriori Hyperparameters ---
MIN_SUPPORT = 0.01 
MIN_CONFIDENCE = 0.5 

# ==========================================
# --- HELPER FUNCTIONS ---
# ==========================================

def get_latest_snapshot(snapshot_dir):
    """Finds the most recently created parquet file in the snapshot directory."""
    if not os.path.exists(snapshot_dir):
        raise FileNotFoundError(f"Snapshot directory not found at {snapshot_dir}")

    files = glob.glob(os.path.join(snapshot_dir, "*.snapshot_*.parquet"))
    if not files:
        files = glob.glob(os.path.join(snapshot_dir, "*.parquet"))
        if not files:
             raise FileNotFoundError(f"No .parquet snapshot files found in {snapshot_dir}")

    latest_file = max(files, key=os.path.getctime)
    return latest_file

def load_product_names(map_path, lookup_dir=LOOKUP_DATA_DIR):
    """
    Loads ID -> Name mapping.
    First searches 'lookupData' folder for product CSV files (e.g. products.csv).
    Falls back to 'DB/product_id_map.json' if lookup CSV is missing.
    """
    product_map = {}

    # 1. Try loading from lookupData CSV files
    if os.path.exists(lookup_dir):
        csv_files = glob.glob(os.path.join(lookup_dir, "*.csv"))
        for csv_file in csv_files:
            try:
                prod_df = pd.read_csv(csv_file)
                if 'id' in prod_df.columns and 'name' in prod_df.columns:
                    for _, row in prod_df.iterrows():
                        product_map[str(row['id'])] = str(row['name'])
                    print(f"  Loaded {len(product_map)} product names from lookup CSV: {csv_file}")
                    return product_map
            except Exception as e:
                pass

    # 2. Fallback to product_id_map.json
    if os.path.exists(map_path):
        try:
            with open(map_path, 'r', encoding='utf-8') as f:
                json_map = json.load(f)
                print(f"  Loaded {len(json_map)} product mappings from JSON: {map_path}")
                return json_map
        except Exception as e:
            print(f"Warning: Could not load product map at {map_path}: {e}")

    return product_map if product_map else None

def replace_ids_with_names(frozenset_items, product_map):
    """Converts a frozenset of IDs to a comma-separated string of names."""
    if not product_map:
        return ", ".join(list(frozenset_items))
    
    names = []
    for pid in frozenset_items:
        str_pid = str(pid)
        names.append(product_map.get(str_pid, f"Unknown Item ({str_pid})"))
    
    return ", ".join(names)

# ==========================================
# --- MAIN TRAINING FUNCTION ---
# ==========================================

def train_apriori():
    print("\n==========================================")
    print("      Starting Apriori Model Training     ")
    print("         (Local Pandas Edition)           ")
    print("==========================================\n")

    try:
        # 1. Locate and Load Snapshot
        print("Status: Loading data snapshot...")
        try:
            snapshot_path = get_latest_snapshot(SNAPSHOT_DIR)
            print(f"  Found latest snapshot: {snapshot_path}")
        except FileNotFoundError as e:
            print(f"  Data Loading Error: {e}")
            return

        # Load using pandas
        df = pd.read_parquet(snapshot_path)

        if df.empty or 'items' not in df.columns:
            print("  Error: Snapshot is empty or missing 'items' column.")
            return

        baskets = df['items'].tolist()
        print(f"  Loaded {len(baskets)} customer baskets for analysis.")

        if len(baskets) < 2:
            print("  Error: Not enough baskets to perform association analysis.")
            return

        # 2. Preprocessing: Standard Pandas One-Hot Encoding
        # PANDAS 2.0+ COMPATIBLE FIX:
        # Replaced .sum(level=0) with .groupby(level=0).sum()
        print("\nStatus: Performing One-Hot Encoding (local)...")
        
        ohe_df = pd.get_dummies(df['items'].apply(pd.Series).stack()).groupby(level=0).sum()
        
        # Ensure data is strictly integer (0/1) for mlxtend
        ohe_df = (ohe_df > 0).astype(np.int8)

        print(f"  Encoding complete. Shape (Baskets x Unique Products): {ohe_df.shape}")

        if ohe_df.empty or ohe_df.shape[1] == 0:
            print("  Error: One-hot encoding resulted in empty DataFrame. Check input data.")
            return

        # 3. Run Apriori Algorithm
        print(f"\nStatus: Running Apriori algorithm (min_support={MIN_SUPPORT})...")
        
        frequent_itemsets = apriori(
            ohe_df, 
            min_support=MIN_SUPPORT, 
            use_colnames=True, 
            low_memory=False
        )

        if frequent_itemsets.empty:
            print("  No frequent itemsets found.")
            print("  Recommendation: Try lowering 'MIN_SUPPORT' in the configuration section.")
            return

        print(f"  Found {len(frequent_itemsets)} frequent itemsets.")

        # 4. Generate Association Rules
        print(f"\nStatus: Generating association rules (min_confidence={MIN_CONFIDENCE})...")
        rules = association_rules(
            frequent_itemsets, 
            metric="confidence", 
            min_threshold=MIN_CONFIDENCE
        )

        if rules.empty:
            print("  No association rules found satisfying the confidence threshold.")
            return

        print(f"  Generated {len(rules)} rules.")

        # 5. Output Results (with Product Name Lookups from lookupData)
        print("\n==========================================")
        print("          Top 10 Association Rules        ")
        print("          (Sorted by Lift)                ")
        print("==========================================\n")

        product_map = load_product_names(PRODUCT_MAP_PATH, LOOKUP_DATA_DIR)
        if not product_map:
            print("Note: lookupData CSV and product_id_map.json not found. Displaying raw IDs.")

        top_rules = rules.sort_values(by="lift", ascending=False).head(10)

        display_df = top_rules[['support', 'confidence', 'lift']].copy()
        display_df['antecedents'] = top_rules['antecedents'].apply(lambda x: replace_ids_with_names(x, product_map))
        display_df['consequents'] = top_rules['consequents'].apply(lambda x: replace_ids_with_names(x, product_map))

        pd.set_option('display.max_colwidth', 50)
        
        for index, row in display_df.iterrows():
            print(f"RULE: {{ {row['antecedents']} }} => {{ {row['consequents']} }}")
            print(f"  - Support:    {row['support']:.4f}")
            print(f"  - Confidence: {row['confidence']:.4f}")
            print(f"  - Lift:       {row['lift']:.4f}")
            print("-" * 20)

    except Exception:
        print("\n!!! An unexpected error occurred during training !!!")
        traceback.print_exc()

if __name__ == "__main__":
    if not os.path.exists(SNAPSHOT_DIR):
        print(f"CRITICAL ERROR: Snapshot directory not found at {SNAPSHOT_DIR}")
        print("Please run data ingestion scripts to create snapshot data.")
    else:
        train_apriori()