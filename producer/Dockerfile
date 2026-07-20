FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /app

RUN python -m compileall /app

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python"]
