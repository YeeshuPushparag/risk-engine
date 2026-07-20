FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Ensure /usr/local/bin exists (fixes pip console script symlink issue)
RUN mkdir -p /usr/local/bin

COPY requirements.txt /tmp/requirements.txt

# Pin pip to a stable version to prevent future regressions
RUN pip install pip==25.3 && \
    pip install -r /tmp/requirements.txt

COPY . /app

RUN python -m compileall /app

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python"]