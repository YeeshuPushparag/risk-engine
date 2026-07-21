FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN set -eux; \
    python -m pip install --upgrade pip; \
    python -m pip --version; \
    python -m pip debug; \
    \
    echo "==================== REQUIREMENTS ===================="; \
    cat /tmp/requirements.txt; \
    \
    echo "==================== INSTALL ===================="; \
    python -m pip install \
        -vvv \
        --no-cache-dir \
        --report /tmp/pip-report.json \
        -r /tmp/requirements.txt || true; \
    \
    echo "==================== INSTALLED DIST-INFO ===================="; \
    find /usr/local/lib/python3.12/site-packages -maxdepth 1 -name "*.dist-info" | sort; \
    \
    echo "==================== SEARCHING FOR jp.py ===================="; \
    find /usr/local/lib/python3.12/site-packages -name RECORD -exec grep -H "jp.py" {} \; || true; \
    \
    echo "==================== SEARCHING FOR jp ===================="; \
    find /usr/local/lib/python3.12/site-packages -name RECORD -exec grep -H "jp" {} \; || true; \
    \
    echo "==================== BIN DIRECTORY ===================="; \
    ls -lah /usr/local/bin || true; \
    \
    echo "==================== REPORT EXISTS ===================="; \
    ls -lah /tmp || true; \
    \
    echo "==================== FAIL BUILD ===================="; \
    exit 1

COPY . /app

RUN python -m compileall /app

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python"]