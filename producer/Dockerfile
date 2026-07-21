FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN set -eux; \
    python -m pip install --upgrade pip; \
    python -m pip --version; \
    python -m pip debug; \
    python -m pip install \
      -vvv \
      --no-cache-dir \
      --report /tmp/pip-report.json \
      -r /tmp/requirements.txt

COPY . /app

RUN python -m compileall /app

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python"]