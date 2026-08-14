import os
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


class BasketRequest(BaseModel):
    items: list[int] | None = None


class BasketResponse(BaseModel):
    predicted_items: list[list[str]]


app = FastAPI(title="Apriori Recommender API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def normalize_mlflow_serve_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.fragment or parsed.path.startswith("#/"):
        parsed = parsed._replace(path="/invocations", fragment="")
    elif not parsed.path or parsed.path == "/":
        parsed = parsed._replace(path="/invocations")
    elif not parsed.path.rstrip("/").endswith("/invocations"):
        parsed = parsed._replace(path=parsed.path.rstrip("/") + "/invocations")
    return urlunparse(parsed)


MLFLOW_SERVE_URL = normalize_mlflow_serve_url(
    os.getenv("MLFLOW_SERVE_URL", "http://127.0.0.1:5001/invocations")
)


@app.post("/predict", response_model=BasketResponse)
def predict(request: BasketRequest) -> BasketResponse:
    payload = {"dataframe_split": {"columns": ["items"], "data": [[request.items or []]]}}

    try:
        response = requests.post(MLFLOW_SERVE_URL, json=payload, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"MLflow serving request failed: {exc}") from exc

    try:
        result = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="MLflow serving returned an invalid JSON response") from exc

    if isinstance(result, dict):
        if "predicted_items" in result:
            return BasketResponse(predicted_items=result["predicted_items"])
        if "predictions" in result:
            predictions = result["predictions"]
            if isinstance(predictions, list) and predictions:
                first = predictions[0]
                if isinstance(first, dict) and "predicted_items" in first:
                    return BasketResponse(predicted_items=[item["predicted_items"] for item in predictions])
                if all(isinstance(item, list) for item in predictions):
                    return BasketResponse(predicted_items=predictions)
            if isinstance(predictions, list):
                return BasketResponse(predicted_items=predictions)

    if isinstance(result, list):
        return BasketResponse(predicted_items=result)

    raise HTTPException(status_code=502, detail="Unexpected response format from MLflow serving")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
