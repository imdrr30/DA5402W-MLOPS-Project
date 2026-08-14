import os
import logging
from contextlib import asynccontextmanager
from typing import Any

import mlflow.pyfunc
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

log = logging.getLogger("apriori-api")
logging.basicConfig(level=logging.INFO)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = "AprioriRecommender"
MODEL_ALIAS = "champion"

_model = None


def load_model() -> None:
    global _model
    import mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        _model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@{MODEL_ALIAS}")
        log.info("Loaded %s@%s from MLflow.", MODEL_NAME, MODEL_ALIAS)
    except Exception as e:
        log.warning("Could not load model from MLflow: %s. Will retry on first request.", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
    yield


class BasketRequest(BaseModel):
    items: list[int] | None = None


class BasketResponse(BaseModel):
    predicted_items: list[str]


app = FastAPI(title="Apriori Recommender API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": _model is not None}


@app.post("/predict", response_model=BasketResponse)
def predict(request: BasketRequest) -> BasketResponse:
    global _model
    if _model is None:
        load_model()
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded yet. Run the Apriori training DAG first.")

    items = [str(i) for i in (request.items or [])]
    input_df = pd.DataFrame([{"items": items}])
    try:
        result = _model.predict(input_df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

    # Flatten result — model returns list of recommended item ID strings
    if isinstance(result, list):
        flat = [str(x) for sublist in result for x in (sublist if isinstance(sublist, list) else [sublist])]
    elif hasattr(result, "tolist"):
        flat = [str(x) for x in result.tolist()]
    else:
        flat = [str(result)]

    return BasketResponse(predicted_items=flat)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5002)

