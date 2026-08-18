FROM apache/airflow:2.7.1-python3.10

USER airflow


# Pin numpy to 1.x before installing pandas/pyarrow/mlxtend; remove snowflake provider to avoid pyOpenSSL conflict
RUN pip install --no-cache-dir \
    "numpy<2" \
    pandas \
    scikit-learn \
    mlxtend \
    mlflow \
    pyarrow \
    pyspark==3.5.1 \
    "dvc[ssh]" \
    "kafka-python==2.2.15" \
    "pyOpenSSL>=24.0.0" \
    "cryptography>=42.0.0" \
    "evidently==0.4.25" \
    && \
    pip uninstall -y apache-airflow-providers-snowflake snowflake-connector-python 2>/dev/null || true