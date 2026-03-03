FROM apache/spark:3.5.7

USER root
WORKDIR /app

# Install Python 3.11 explicitly (Ubuntu 20.04 supports this)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-venv \
        python3.11-distutils \
        python3-pip \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Force Spark to use Python 3.11
ENV PYSPARK_PYTHON=/usr/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11

COPY requirements.txt .

# Install dependencies using Python 3.11
RUN /usr/bin/python3.11 -m pip install --no-cache-dir --break-system-packages --upgrade pip && \
    /usr/bin/python3.11 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

# Ivy cache directory for Spark packages
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp

COPY . .

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"