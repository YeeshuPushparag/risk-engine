FROM apache/spark:3.5.7

USER root
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3-pip && \
    rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

COPY requirements.txt .

RUN /usr/bin/python3 -m pip install --upgrade pip && \
    /usr/bin/python3 -m pip install --no-cache-dir -r requirements.txt

COPY . .

RUN /usr/bin/python3 -m compileall .

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"