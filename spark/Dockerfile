FROM apache/spark:3.5.7

USER root
WORKDIR /app

# Install only required system packages (NO Python override)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Use Spark's built-in Python
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

COPY requirements.txt .

# Upgrade pip and install dependencies
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt

# Ivy cache directory for Spark packages
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp

COPY . .

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"