FROM apache/spark:3.5.7

USER root
WORKDIR /app

# Install system python only
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Force Spark to use system python
ENV PYSPARK_PYTHON=/usr/bin/python3

# Install deps INTO THE SAME PYTHON
COPY requirements.txt .
RUN /usr/bin/python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel certifi && \
    /usr/bin/python3 -m pip install --no-cache-dir -r requirements.txt

# Spark ivy cache
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp

# App code
COPY . .

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
