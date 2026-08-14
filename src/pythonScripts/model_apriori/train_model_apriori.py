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
# 6. Evaluates recommendation quality on the held-out test split (leave-out
#    Precision/Recall/F1/HitRate/MAP/NDCG @K + Coverage) and stores the
#    results in the pickle payload as 'eval_metrics'.
#
# NOTE: this script does NOT talk to MLflow. Logging the trained pickle's
# params/metrics/artifacts to MLflow is handled by the separate
# log_apriori_to_mlflow.py script, run as its own Airflow task after this one.
# ==============================================================================

import math
import random
import pandas as pd
import json
import os
import pickle
import argparse
from pathlib import Path
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sklearn.model_selection import train_test_split

try:
    import mlflow
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit("mlflow is not installed. Install it with: pip install mlflow") from exc
    

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def resolve_repo_root(start_file: Path) -> Path:
    current = start_file.resolve()
    for parent in [current, *current.parents]:
        if (parent / "artifacts").exists():
            return parent
    return current.parents[-1]


def generate_recommendations(rules_df: pd.DataFrame, basket_items, top_k: int = 5) -> list:
    """Rank consequents of rules whose antecedents are satisfied by basket_items.

    Mirrors the ranking logic used by AprioriRecommender.predict in
    apriori_mlflow_predict.py (sorted by lift desc, then confidence desc),
    so offline evaluation here reflects what gets served in production.
    """
    basket_set = set(basket_items)
    if rules_df.empty or not basket_set:
        return []

    matching = rules_df[rules_df["antecedents"].apply(lambda ants: set(ants).issubset(basket_set))]
    if matching.empty:
        return []

    matching = matching.sort_values(["lift", "confidence"], ascending=False)

    seen = set()
    ordered_items = []
    for _, row in matching.iterrows():
        for item in row["consequents"]:
            if item in basket_set or item in seen:
                continue
            seen.add(item)
            ordered_items.append(item)
            if len(ordered_items) >= top_k:
                return ordered_items
    return ordered_items


def evaluate_recommendations(
    rules_df: pd.DataFrame,
    test_transactions: list,
    top_k: int = 5,
    holdout_frac: float = 0.3,
    random_state: int = 42,
) -> dict:
    """Leave-out evaluation on held-out test baskets.

    For each test basket with >=2 items, hides a fraction of its items
    (the "held_out" set) and uses the remainder as the query. Recommends
    top_k items from the trained rules and scores overlap with held_out.

    Returns Precision@K, Recall@K, F1@K, Hit Rate@K, MAP@K, NDCG@K
    (averaged across eligible baskets) plus Coverage (share of the test
    catalog that ever got recommended).
    """
    rng = random.Random(random_state)
    catalog_items = set()
    for basket in test_transactions:
        catalog_items.update(basket)

    precisions, recalls, f1s, hits, aps, ndcgs = [], [], [], [], [], []
    recommended_union = set()
    eligible_baskets = 0

    for basket in test_transactions:
        items = list(dict.fromkeys(basket))  # de-dup, preserve order
        if len(items) < 2:
            continue

        shuffled = items[:]
        rng.shuffle(shuffled)
        n_holdout = max(1, round(len(shuffled) * holdout_frac))
        n_holdout = min(n_holdout, len(shuffled) - 1)  # keep >=1 input item
        held_out = set(shuffled[:n_holdout])
        input_items = shuffled[n_holdout:]

        if not input_items or not held_out:
            continue

        eligible_baskets += 1
        recommended = generate_recommendations(rules_df, input_items, top_k=top_k)
        recommended_union.update(recommended)

        hit_flags = [1 if item in held_out else 0 for item in recommended]
        num_hits = sum(hit_flags)

        precision = num_hits / top_k
        recall = num_hits / len(held_out)
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        hit = 1.0 if num_hits > 0 else 0.0

        # Average Precision (for MAP@K)
        hits_so_far = 0
        precision_sum = 0.0
        for i, flag in enumerate(hit_flags, start=1):
            if flag:
                hits_so_far += 1
                precision_sum += hits_so_far / i
        ap = precision_sum / min(len(held_out), top_k) if held_out else 0.0

        # NDCG@K
        dcg = sum(flag / math.log2(i + 2) for i, flag in enumerate(hit_flags))
        idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(held_out), top_k)))
        ndcg = dcg / idcg if idcg > 0 else 0.0

        precisions.append(precision)
        recalls.append(recall)
        f1s.append(f1)
        hits.append(hit)
        aps.append(ap)
        ndcgs.append(ndcg)

    if eligible_baskets == 0:
        return {
            "eval_basket_count": 0,
            "precision_at_k": None,
            "recall_at_k": None,
            "f1_at_k": None,
            "hit_rate_at_k": None,
            "map_at_k": None,
            "ndcg_at_k": None,
            "coverage": None,
        }

    coverage = len(recommended_union) / len(catalog_items) if catalog_items else 0.0

    return {
        "eval_basket_count": eligible_baskets,
        "precision_at_k": round(sum(precisions) / eligible_baskets, 4),
        "recall_at_k": round(sum(recalls) / eligible_baskets, 4),
        "f1_at_k": round(sum(f1s) / eligible_baskets, 4),
        "hit_rate_at_k": round(sum(hits) / eligible_baskets, 4),
        "map_at_k": round(sum(aps) / eligible_baskets, 4),
        "ndcg_at_k": round(sum(ndcgs) / eligible_baskets, 4),
        "coverage": round(coverage, 4),
    }


def run_apriori_and_save(
    features_csv: str = "extracted_features.csv", 
    min_support: float = 0.001, 
    min_confidence: float = 0.1,
    rules_output_csv: str = "apriori_rules.csv",
    model_pickle_file: str = "apriori_model.pkl",
    test_size: float = 0.3,
    random_state: int = 42,
    eval_top_k: int = 5,
    eval_holdout_frac: float = 0.3,
):
    """
    Applies Apriori algorithm to extracted_features.csv, exports rules to CSV,
    evaluates recommendation quality on the held-out test split, and saves the
    model object (rules + eval_metrics) into a pickle (.pkl) file.
    """
    repo_root = resolve_repo_root(Path(__file__))
    if features_csv is None:
        features_csv = repo_root / "DB" / "extracted_features.csv"
    if model_pickle_file is None:
        model_pickle_file = repo_root / "artifacts" / "apriori_model.pkl"
    if rules_output_csv is None:
        rules_output_csv = repo_root / "artifacts" / "apriori_rules.csv"
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

    total_transactions = len(transactions)
    print(f"Total valid baskets found: {total_transactions}")
    if total_transactions < 2:
        print("Not enough transactions for train/test split.")
        return

    # 2. Perform 70:30 Train/Test Split
    # We set shuffle=True to ensure randomness (recommended unless temporal order matters)
    train_transactions, test_transactions = train_test_split(
        transactions, test_size=test_size, random_state=random_state
    )

    print(f"Training transactions (70%): {len(train_transactions)}")
    print(f"Testing transactions (30%): {len(test_transactions)}")

    # 2. One-Hot Encoding for Apriori
    print("One-hot encoding transactions...")
    te = TransactionEncoder()
    te_ary_train = te.fit(train_transactions).transform(train_transactions)
    basket_df_train = pd.DataFrame(te_ary_train, columns=te.columns_)

    # 3. Apply Apriori Algorithm for Frequent Itemsets
    print(f"Running Apriori algorithm (min_support = {min_support})...")
    frequent_itemsets = apriori(basket_df_train, min_support=min_support, use_colnames=True)
    
    if frequent_itemsets.empty:
        print("No frequent itemsets found with min_support =", min_support)
        print("Tip: Lower the min_support value (e.g. min_support=0.005).")
        return

    print(f"Found {len(frequent_itemsets)} frequent itemsets.")

    # 4. Generate Association Rules
    print(f"Generating Association Rules (min_confidence = {min_confidence})...")
    rules = association_rules(
    frequent_itemsets, 
    metric="confidence", 
    min_threshold=min_confidence)

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

        # 6. Evaluate recommendation quality on the held-out test transactions.
        # rules (not rules_export) is used here because antecedents/consequents
        # are still frozensets, matching what generate_recommendations expects
        # and what AprioriRecommender.predict operates on at serving time.
        print(f"Evaluating recommendations on {len(test_transactions)} test baskets "
              f"(top_k={eval_top_k}, holdout_frac={eval_holdout_frac})...")
        eval_metrics = evaluate_recommendations(
            rules,
            test_transactions,
            top_k=eval_top_k,
            holdout_frac=eval_holdout_frac,
            random_state=random_state,
        )
        print(f"Evaluation metrics: {eval_metrics}")

        # 7. Save Model artifact object to Pickle (.pkl) File
        model_payload = {
            'encoder': te,
            'frequent_itemsets': frequent_itemsets,
            'rules': rules,
            'eval_metrics': eval_metrics,
            'parameters': {
                'min_support': min_support,
                'min_confidence': min_confidence,
                'total_transactions_original': total_transactions,
                'train_size': len(train_transactions),
                'test_size': len(test_transactions),
                'random_state': random_state,
                'eval_top_k': eval_top_k,
                'eval_holdout_frac': eval_holdout_frac,
            }
        }
        
        with open(model_pickle_file, 'wb') as f:
            pickle.dump(model_payload, f)
            
        print(f"Model serialized & saved as pickle file: '{model_pickle_file}'")
        
        print("=== Top 10 Association Rules by Lift ===")
        print(rules_export[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))
    else:
        print("No association rules met the minimum confidence threshold.")

# def log_apriori_artifacts(
#     model_path: Path | None = None,
#     rules_path: Path | None = None,
#     tracking_uri: str | None = None,
# ) -> None:
#     repo_root = resolve_repo_root(Path(__file__))

#     if model_path is None:
#         model_path = repo_root / "artifacts" / "apriori_model.pkl"
#     if rules_path is None:
#         rules_path = repo_root / "artifacts" / "apriori_rules.csv"

#     if not model_path.exists():
#         raise FileNotFoundError(f"Apriori model not found at: {model_path}")

#     if tracking_uri is None:
#         tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")

#     mlflow.set_tracking_uri(tracking_uri)

#     with model_path.open("rb") as fh:
#         model_payload = pickle.load(fh)

#     params = model_payload.get("parameters", {})
#     rules = model_payload.get("rules")
#     frequent_itemsets = model_payload.get("frequent_itemsets")

#     if rules is None:
#         raise ValueError("The Apriori model payload does not contain association rules")

#     rules_export = rules.copy()
#     rules_export["antecedents"] = rules_export["antecedents"].apply(lambda x: list(x))
#     rules_export["consequents"] = rules_export["consequents"].apply(lambda x: list(x))
#     rules_export = rules_export.sort_values(by="lift", ascending=False)
#     rules_export.to_csv(rules_path, index=False)

#     with mlflow.start_run(run_name="apriori_training") as run:
#         mlflow.log_param("min_support", params.get("min_support"))
#         mlflow.log_param("min_confidence", params.get("min_confidence"))
#         mlflow.log_param("total_transactions", params.get("total_transactions"))
#         mlflow.log_metric("frequent_itemsets_count", len(frequent_itemsets) if frequent_itemsets is not None else 0)
#         mlflow.log_metric("association_rules_count", len(rules))

#         mlflow.log_artifact(str(model_path), artifact_path="artifacts")
#         mlflow.log_artifact(str(rules_path), artifact_path="artifacts")

#         print(f"MLflow run started: {run.info.run_id}")
#         print(f"Logged model artifact: {model_path}")
#         print(f"Logged rules artifact: {rules_path}")
#         print(f"Tracking URI: {tracking_uri}")
        

if __name__ == "__main__":
    # Execute Apriori and save results locally
    input_path = os.path.join(BASE_DIR, "../../..", "DB", "extracted_features.csv")

    parser = argparse.ArgumentParser(description="Upload Apriori artifacts to MLflow")
    parser.add_argument("--model-path", type=str, default=None, help="Path to apriori_model.pkl")
    parser.add_argument("--rules-path", type=str, default=None, help="Path to apriori_rules.csv")
    parser.add_argument("--input-path", type=str, default=None, help="Path to extracted_features.csv")
    parser.add_argument("--tracking-uri", type=str, default=None, help="MLflow tracking server URI")
    args = parser.parse_args()
    
    # log_apriori_artifacts(
    #     model_path=Path(args.model_path) if args.model_path else None,
    #     rules_path=Path(args.rules_path) if args.rules_path else None,
    #     tracking_uri=args.tracking_uri,
    # )
    
    run_apriori_and_save(
        features_csv=Path(args.input_path) if args.input_path else None,
        min_support=0.000000000000000001,         
        min_confidence=0.1,      # 10% minimum confidence
        rules_output_csv=Path(args.rules_path) if args.rules_path else None,
        model_pickle_file=Path(args.model_path) if args.model_path else None
    )