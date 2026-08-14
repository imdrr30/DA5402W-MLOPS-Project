"""
Shared MLflow model registry utility.
Compares a new run's metric against the current champion and promotes only if better.
"""
import mlflow
from mlflow.tracking import MlflowClient


def promote_if_best(
    model_name: str,
    new_run_id: str,
    metric_name: str,
    higher_is_better: bool = True,
) -> bool:
    """
    Register the model from new_run_id and set it as @champion if its
    metric_name beats the current champion. Returns True if promoted.
    """
    client = MlflowClient()

    # Register new version (always — every run is tracked)
    model_uri = f"runs:/{new_run_id}/sklearn_model"
    new_version = mlflow.register_model(model_uri, model_name).version

    new_run = client.get_run(new_run_id)
    new_metric = new_run.data.metrics.get(metric_name)
    if new_metric is None:
        print(f"WARNING: metric '{metric_name}' not found in run {new_run_id}. Skipping promotion.")
        return False

    # Check if a champion already exists
    try:
        champion_version = client.get_model_version_by_alias(model_name, "champion")
        champ_run = client.get_run(champion_version.run_id)
        champ_metric = champ_run.data.metrics.get(metric_name, None)
    except Exception:
        champion_version = None
        champ_metric = None

    is_better = (
        champ_metric is None
        or (higher_is_better and new_metric > champ_metric)
        or (not higher_is_better and new_metric < champ_metric)
    )

    if is_better:
        client.set_registered_model_alias(model_name, "champion", new_version)
        print(
            f"[Registry] '{model_name}' v{new_version} promoted to @champion "
            f"({metric_name}: {champ_metric} → {new_metric:.4f})"
        )
        return True
    else:
        print(
            f"[Registry] '{model_name}' v{new_version} NOT promoted "
            f"(new {metric_name}={new_metric:.4f} vs champion={champ_metric:.4f})"
        )
        return False
