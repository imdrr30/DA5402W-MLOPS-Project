FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    python3 -m pip install --no-cache-dir \
        numpy \
        pandas \
        kafka-python

USER spark