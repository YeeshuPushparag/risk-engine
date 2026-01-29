FROM apache/spark:3.5.7

USER root
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=/usr/local/bin/python3

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel certifi && \
    python3 -m pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp

COPY . .

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
