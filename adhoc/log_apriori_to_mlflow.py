import argparse
import os
import pickle
from pathlib import Path

try:
    import mlflow
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit("mlflow is not installed. Install it with: pip install mlflow") from exc


def resolve_repo_root(start_file: Path) -> Path:
    current = start_file.resolve()
    for parent in [current, *current.parents]:
        if (parent / "artifacts").exists():
            return parent
    return current.parents[-1]


def log_apriori_artifacts(
    model_path: Path | None = None,
    rules_path: Path | None = None,
    tracking_uri: str | None = None,
) -> None:
    repo_root = resolve_repo_root(Path(__file__))

    if model_path is None:
        model_path = repo_root / "artifacts" / "apriori_model.pkl"
    if rules_path is None:
        rules_path = repo_root / "artifacts" / "apriori_rules.csv"

    if not model_path.exists():
        raise FileNotFoundError(f"Apriori model not found at: {model_path}")

    if tracking_uri is None:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")

    mlflow.set_tracking_uri(tracking_uri)

    with model_path.open("rb") as fh:
        model_payload = pickle.load(fh)

    params = model_payload.get("parameters", {})
    rules = model_payload.get("rules")
    frequent_itemsets = model_payload.get("frequent_itemsets")

    if rules is None:
        raise ValueError("The Apriori model payload does not contain association rules")

    rules_export = rules.copy()
    rules_export["antecedents"] = rules_export["antecedents"].apply(lambda x: list(x))
    rules_export["consequents"] = rules_export["consequents"].apply(lambda x: list(x))
    rules_export = rules_export.sort_values(by="lift", ascending=False)
    rules_export.to_csv(rules_path, index=False)

    with mlflow.start_run(run_name="apriori_training") as run:
        mlflow.log_param("min_support", params.get("min_support"))
        mlflow.log_param("min_confidence", params.get("min_confidence"))
        mlflow.log_param("total_transactions", params.get("total_transactions"))
        mlflow.log_metric("frequent_itemsets_count", len(frequent_itemsets) if frequent_itemsets is not None else 0)
        mlflow.log_metric("association_rules_count", len(rules))

        mlflow.log_artifact(str(model_path), artifact_path="artifacts")
        mlflow.log_artifact(str(rules_path), artifact_path="artifacts")

        print(f"MLflow run started: {run.info.run_id}")
        print(f"Logged model artifact: {model_path}")
        print(f"Logged rules artifact: {rules_path}")
        print(f"Tracking URI: {tracking_uri}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Apriori artifacts to MLflow")
    parser.add_argument("--model-path", type=str, default=None, help="Path to apriori_model.pkl")
    parser.add_argument("--rules-path", type=str, default=None, help="Path to apriori_rules.csv")
    parser.add_argument("--tracking-uri", type=str, default=None, help="MLflow tracking server URI")
    args = parser.parse_args()

    log_apriori_artifacts(
        model_path=Path(args.model_path) if args.model_path else None,
        rules_path=Path(args.rules_path) if args.rules_path else None,
        tracking_uri=args.tracking_uri,
    )
