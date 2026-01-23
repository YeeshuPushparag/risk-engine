FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN echo "=== REQUIREMENTS FILE ===" && cat requirements.txt

RUN python -m pip install --upgrade pip \
 && python -m pip install --no-cache-dir -r requirements.txt -v

RUN python -c "import django; print('Django OK:', django.get_version())"
RUN python -c "import uvicorn; print('Uvicorn OK:', uvicorn.__version__)"

COPY . .

RUN python manage.py collectstatic --noinput

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "backend.asgi:application", "--host", "0.0.0.0", "--port", "8000"]
