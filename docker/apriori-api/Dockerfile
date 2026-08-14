FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.111.0 \
    uvicorn==0.30.1 \
    mlflow==2.14.0 \
    pandas==2.2.2 \
    mlxtend==0.23.1

COPY src/pythonScripts/model_apriori/apriori_http_service.py .
COPY src/pythonScripts/model_apriori/apriori_mlflow_predict.py .

EXPOSE 5002

ENV PYTHONUNBUFFERED=1

CMD ["python", "apriori_http_service.py"]
