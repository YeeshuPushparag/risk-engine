FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# System deps required for psycopg==3.3.2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

# Collect static at build time
RUN python manage.py collectstatic --noinput

EXPOSE 8000

CMD ["uvicorn", "backend.asgi:application", "--host", "0.0.0.0", "--port", "8000"]
