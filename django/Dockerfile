FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN /usr/local/bin/python -m pip install --upgrade pip \
 && /usr/local/bin/python -m pip install --no-cache-dir -r requirements.txt

COPY . .

RUN /usr/local/bin/python manage.py collectstatic --noinput

EXPOSE 8000

CMD ["/usr/local/bin/python", "-m", "uvicorn", "backend.asgi:application", "--host", "0.0.0.0", "--port", "8000"]
