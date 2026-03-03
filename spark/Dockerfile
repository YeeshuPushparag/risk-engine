FROM apache/spark:3.5.7

USER root
WORKDIR /app

# Install Python 3.12 explicitly (NO floating python3)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.12 \
        python3.12-venv \
        python3.12-distutils \
        python3-pip \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Force Spark to use Python 3.12
ENV PYSPARK_PYTHON=/usr/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12

COPY requirements.txt .

# Install Python dependencies using Python 3.12
RUN /usr/bin/python3.12 -m pip install --no-cache-dir --break-system-packages certifi && \
    /usr/bin/python3.12 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

# Ivy cache directory for Spark packages
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp

COPY . .

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"