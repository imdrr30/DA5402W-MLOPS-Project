FROM apache/airflow:2.7.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip docker.io && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install --no-cache-dir \
        numpy \
        pandas 

RUN groupdel docker 2>/dev/null; groupadd -g 1001 docker && usermod -aG docker airflow

USER airflow

# Pin numpy to 1.x before installing pandas/pyarrow/mlxtend
RUN pip install --no-cache-dir \
    "numpy<2" \
    pandas \
    scikit-learn \
    mlxtend \
    mlflow \
    pyarrow \
    "kafka-python==2.2.15" \
    "pyOpenSSL>=24.0.0" \
    "cryptography>=42.0.0" \
    "evidently==0.4.25"

