# ==============================================================================
# Apriori Model Execution & Pickle Serialization Script
# ==============================================================================
# Requirements: pip install pandas mlxtend
# Description:
# 1. Reads 'extracted_features.csv' from local machine.
# 2. Preprocesses cart_product_id_list into a transaction basket.
# 3. Applies Apriori to extract frequent itemsets and association rules.
# 4. Saves the association rules to 'apriori_rules.csv'.
# 5. Serializes and saves the trained Apriori model / rules to 'apriori_model.pkl'.
# ==============================================================================

import pandas as pd
import json
import os
import pickle
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_apriori_and_save(
    features_csv: str = "extracted_features.csv", 
    min_support: float = 0.001, 
    min_confidence: float = 0.1,
    rules_output_csv: str = "apriori_rules.csv",
    model_pickle_file: str = "apriori_model.pkl"
):
    """
    Applies Apriori algorithm to extracted_features.csv, exports rules to CSV,
    and saves the model object into a pickle (.pkl) file.
    """
    if not os.path.exists(features_csv):
        print(f"Error: '{features_csv}' not found in current directory ({os.getcwd()}).")
        print("Please place 'extracted_features.csv' in the same folder as this script.")
        return

    print(f"Loading extracted dataset: {features_csv}...")
    df = pd.read_csv(features_csv)

    # 1. Parse JSON cart_product_id_list strings into Python lists
    transactions = []
    for item in df['cart_product_id_list']:
        if isinstance(item, str):
            try:
                p_list = json.loads(item)
            except:
                p_list = [p.strip(" []'\"") for p in item.split(",") if p.strip(" []'\"")]
        elif isinstance(item, list):
            p_list = item
        else:
            p_list = []
            
        if p_list:
            transactions.append([str(p) for p in p_list if p])

    print(f"Total valid baskets/transactions found: {len(transactions)}")
    if not transactions:
        print("No non-empty cart transactions found in dataset.")
        return

    # 2. One-Hot Encoding for Apriori
    print("One-hot encoding transactions...")
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    basket_df = pd.DataFrame(te_ary, columns=te.columns_)

    # 3. Apply Apriori Algorithm for Frequent Itemsets
    print(f"Running Apriori algorithm (min_support = {min_support})...")
    frequent_itemsets = apriori(basket_df, min_support=min_support, use_colnames=True)
    
    if frequent_itemsets.empty:
        print("No frequent itemsets found with min_support =", min_support)
        print("Tip: Lower the min_support value (e.g. min_support=0.005).")
        return

    print(f"Found {len(frequent_itemsets)} frequent itemsets.")

    # 4. Generate Association Rules
    print(f"Generating Association Rules (min_confidence = {min_confidence})...")
    rules = association_rules(
    frequent_itemsets, 
    metric="confidence", )
    #min_threshold=min_confidence)

    if not rules.empty:
        # Convert frozensets to standard lists for clean CSV export
        rules_export = rules.copy()
        rules_export['antecedents'] = rules_export['antecedents'].apply(lambda x: list(x))
        rules_export['consequents'] = rules_export['consequents'].apply(lambda x: list(x))
        
        # Sort by Lift descending
        rules_export = rules_export.sort_values(by='lift', ascending=False)

        # 5. Export Association Rules to CSV
        rules_export.to_csv(rules_output_csv, index=False)
        print(f"Association Rules saved to CSV: '{rules_output_csv}'")

        # 6. Save Model artifact object to Pickle (.pkl) File
        model_payload = {
            'encoder': te,
            'frequent_itemsets': frequent_itemsets,
            'rules': rules,
            'parameters': {
                'min_support': min_support,
                'min_confidence': min_confidence,
                'total_transactions': len(transactions)
            }
        }
        
        with open(model_pickle_file, 'wb') as f:
            pickle.dump(model_payload, f)
            
        print(f"Model serialized & saved as pickle file: '{model_pickle_file}'")
        
        print("=== Top 10 Association Rules by Lift ===")
        print(rules_export[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(100))
    else:
        print("No association rules met the minimum confidence threshold.")

if __name__ == "__main__":
    # Execute Apriori and save results locally
    input_path = os.path.join(BASE_DIR, "../../..", "DB", "extracted_features.csv")
    model_path = os.path.join(BASE_DIR, "../../..", "artefacts", "apriori_model.pkl")
    rules_path = os.path.join(BASE_DIR, "../../..", "artefacts", "apriori_rules.csv")
    run_apriori_and_save(
        features_csv=input_path,
        min_support=0.000000000000000001,         # 1% minimum support
        min_confidence=0,      # 10% minimum confidence
        rules_output_csv=rules_path,
        model_pickle_file=model_path
    )
