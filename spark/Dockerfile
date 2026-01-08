FROM apache/spark:3.5.7

USER root
WORKDIR /app

# ---- Python + pip ----
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3 1 && \
    python3 -m pip install --upgrade pip setuptools wheel certifi

# Needed for Kafka ivy jars
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp

COPY requirements.txt /app/requirements.txt

RUN pip3 install --no-cache-dir \
    --index-url https://pypi.org/simple \
    -r /app/requirements.txt

COPY . /app

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

USER root
