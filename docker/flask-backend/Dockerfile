FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/service-backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/service-backend/app.py .
COPY src/service-backend/seed.py .

EXPOSE 5001

ENV PYTHONUNBUFFERED=1

CMD ["sh", "-c", "python seed.py && python app.py"]
