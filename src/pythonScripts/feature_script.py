import os
import pandas as pd
import glob
from datetime import datetime
import traceback

# --- CONFIGURATION ---
# Update this to your absolute path
BASE_INPUT_DIR = r"C:\Samuel\Web_MTech\Term_3\MLOPS\Project\DA5402W-MLOPS-Project\DB\consumed_events"
SNAPSHOT_DIR = os.path.join("DB", "snapshot") # Creates DB/snapshot relative to script

# CRITICAL: Change this to your actual transaction event type, e.g., 'transaction'
TARGET_EVENT_TYPE = "add_to_cart" 
# ---------------------

def create_snapshot():
    print("Starting feature extraction (non-Ray mode)...")

    try:
        # 1. Find all Parquet files recursively
        # This looks through id_type=*/event_date=*/*.parquet
        search_path = os.path.join(BASE_INPUT_DIR, "**", "*.parquet")
        print(f"Searching for files in: {search_path}")
        
        # recursive=True allows ** to match subdirectories
        parquet_files = glob.glob(search_path, recursive=True)

        if not parquet_files:
            print(f"No parquet files found found in {BASE_INPUT_DIR}")
            return

        print(f"Found {len(parquet_files)} parquet files. Reading and combining...")

        # 2. Read and Combine Files using Pandas
        # We read files one by one and concatenate them. 
        # This is slower than Ray but much more stable on Windows.
        dfs = []
        for file in parquet_files:
            try:
                # Using pyarrow engine to read
                dfs.append(pd.read_parquet(file, engine='pyarrow'))
            except Exception as e:
                print(f"Error reading {file}: {e}")
        
        if not dfs:
            print("Could not read any parquet files successfully.")
            return

        df = pd.concat(dfs, ignore_index=True)
        print(f"Total records loaded: {len(df)}")

        # 3. Data Preprocessing
        if 'event_time' in df.columns:
            df['event_time'] = pd.to_datetime(df['event_time'])

        if 'product_id' in df.columns:
            df['product_id'] = df['product_id'].astype(str)
        else:
            print("Error: Column 'product_id' not found in data.")
            return
            
        if 'event_type' not in df.columns:
             print("Error: Column 'event_type' not found in data.")
             return

        # 4. Filter Data
        print(f"Filtering for event type: {TARGET_EVENT_TYPE}...")
        txn_df = df[df['event_type'] == TARGET_EVENT_TYPE].copy()

        # Filter Strategy: Only include logged-in users
        print("Filtering for events with valid user_id...")
        if 'user_id' in txn_df.columns:
            txn_df = txn_df.dropna(subset=['user_id'])
            # Ensure user_id is string and not empty
            txn_df['user_id'] = txn_df['user_id'].astype(str)
            txn_df = txn_df[txn_df['user_id'].str.strip() != '']
        else:
            print("Error: Column 'user_id' not found.")
            return

        if txn_df.empty:
            print("No transaction events linked to user_ids found.")
            return

        # 5. Create Customer Baskets
        print(f"Grouping products into baskets for {txn_df['user_id'].nunique()} unique users...")
        basket_df = txn_df.groupby('user_id')['product_id'].apply(list).reset_index()
        
        basket_df.rename(columns={'product_id': 'items'}, inplace=True)

        # Filter out single-item baskets
        basket_df = basket_df[basket_df['items'].map(len) > 1]

        if basket_df.empty:
             print("No valid multi-item baskets found.")
             return

        # 6. Save Snapshot as Parquet
        if not os.path.exists(SNAPSHOT_DIR):
            os.makedirs(SNAPSHOT_DIR)
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"snapshot_{timestamp_str}.parquet"
        output_path = os.path.join(SNAPSHOT_DIR, output_filename)

        print(f"Saving snapshot ({len(basket_df)} valid baskets) to {output_path}...")
        
        # Enforce pyarrow engine for writing
        basket_df.to_parquet(output_path, index=False, engine='pyarrow')
        
        print("Snapshot created successfully.")

    except Exception as e:
        print(f"An error occurred during snapshot creation: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    create_snapshot()