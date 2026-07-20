FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir --upgrade pip==25.0.1 setuptools wheel && \
    python -m pip install --no-cache-dir --no-compile -r /tmp/requirements.txt

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
