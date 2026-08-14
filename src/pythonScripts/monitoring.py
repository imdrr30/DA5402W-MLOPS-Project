# pythonScripts/monitoring.py
import argparse
import json
import os
from datetime import datetime

import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently import ColumnMapping
from mlflow.tracking import MlflowClient

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# TODO: adjust to your actual model's feature set once you decide what
# Evidently should track drift on (raw event columns vs. session features).
CATEGORICAL_FEATURES = ["event_type", "product_id"]
NUMERICAL_FEATURES = []


def load_reference(reference_path: str) -> pd.DataFrame:
    if not os.path.exists(reference_path):
        raise FileNotFoundError(
            f"Reference data not found at {reference_path}. "
            "Bootstrap it once with a known-good snapshot of consumed_events."
        )
    return pd.read_parquet(reference_path)


def load_current(consumed_events_path: str, run_date: str) -> pd.DataFrame:
    df = pd.read_parquet(consumed_events_path)
    df["event_time"] = pd.to_datetime(df["event_time"])
    target_date = pd.to_datetime(run_date).date()
    return df[df["event_time"].dt.date == target_date]


def run_drift_report(reference_df: pd.DataFrame, current_df: pd.DataFrame, output_html_path: str) -> dict:
    column_mapping = ColumnMapping(
        categorical_features=CATEGORICAL_FEATURES,
        numerical_features=NUMERICAL_FEATURES,
    )
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference_df, current_data=current_df, column_mapping=column_mapping)

    os.makedirs(os.path.dirname(output_html_path), exist_ok=True)
    report.save_html(output_html_path)

    result = report.as_dict()
    drift = result["metrics"][0]["result"]
    return {
        "dataset_drift": drift.get("dataset_drift"),
        "n_drifted_columns": drift.get("number_of_drifted_columns"),
        "share_drifted_columns": drift.get("share_of_drifted_columns"),
    }


def get_latest_metric(mlflow_tracking_uri: str, experiment_name: str, metric_key: str) -> float:
    """Fetch the most recent value of `metric_key` from the latest MLflow run
    that has it.

    metric_key is expected to be one of the recommendation-quality metrics
    logged by train_model_apriori.py's evaluate_recommendations(), e.g.
    "recall_at_k" (default trigger metric), "precision_at_k", "f1_at_k",
    "hit_rate_at_k", "map_at_k", "ndcg_at_k", or "coverage".
    """
    client = MlflowClient(tracking_uri=mlflow_tracking_uri)
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(f"MLflow experiment '{experiment_name}' not found")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=10,
    )
    for run in runs:
        if metric_key in run.data.metrics:
            return run.data.metrics[metric_key]

    raise ValueError(f"No run in experiment '{experiment_name}' has metric '{metric_key}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--reference-path", default=os.path.join(BASE_DIR, "../..", "DB", "reference", "reference_data.parquet"))
    parser.add_argument("--consumed-events-path", default=os.path.join(BASE_DIR, "../..", "DB", "consumed_events"))
    parser.add_argument("--report-output-dir", default=os.path.join(BASE_DIR, "../..", "outputs", "monitoring"))
    parser.add_argument("--status-output-dir", default=os.path.join(BASE_DIR, "../..", "DB", "monitoring"))
    parser.add_argument("--mlflow-tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    parser.add_argument("--mlflow-experiment-name", default=os.environ.get("MLFLOW_EXPERIMENT_NAME", "Default"))
    # Recommendation-quality metric to monitor and its retrain threshold.
    # Defaults to recall_at_k: the share of items customers actually add to
    # their basket next that the model successfully surfaced in its top-K
    # recommendations. Tune the threshold once you have a few weeks of
    # historical values logged — 0.15 is a starting point, not a rule.
    parser.add_argument("--metric-key", default=os.environ.get("MONITORING_METRIC_KEY", "recall_at_k"))
    parser.add_argument("--metric-threshold", type=float, default=float(os.environ.get("MONITORING_METRIC_THRESHOLD", 0.15)))
    args = parser.parse_args()

    reference_df = load_reference(args.reference_path)
    current_df = load_current(args.consumed_events_path, args.run_date)

    if current_df.empty:
        print(f"[WARN] No current data for {args.run_date} — skipping drift check")
        drift_summary = {"dataset_drift": None, "n_drifted_columns": None, "share_drifted_columns": None}
    else:
        html_path = os.path.join(args.report_output_dir, f"drift_report_{args.run_date}.html")
        drift_summary = run_drift_report(reference_df, current_df, html_path)
        print(f"[INFO] Drift report written to {html_path}")

    metric_value = get_latest_metric(args.mlflow_tracking_uri, args.mlflow_experiment_name, args.metric_key)
    trigger_retrain = metric_value < args.metric_threshold

    status = {
        "run_date": args.run_date,
        "metric_key": args.metric_key,
        "metric_value": metric_value,
        "threshold": args.metric_threshold,
        "trigger_retrain": trigger_retrain,
        "checked_at": datetime.utcnow().isoformat(),
        **drift_summary,
    }

    os.makedirs(args.status_output_dir, exist_ok=True)
    with open(os.path.join(args.status_output_dir, f"status_{args.run_date}.json"), "w") as f:
        json.dump(status, f, indent=2)
    with open(os.path.join(args.status_output_dir, "latest_status.json"), "w") as f:
        json.dump(status, f, indent=2)

    print(f"[INFO] {args.metric_key}={metric_value:.4f} threshold={args.metric_threshold} trigger_retrain={trigger_retrain}")
    print(f"[INFO] Drift summary: {drift_summary}")


if __name__ == "__main__":
    main()