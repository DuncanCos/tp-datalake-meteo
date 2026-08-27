FROM python:3.12-slim

RUN pip install --no-cache-dir kafka-python==2.2.15 requests

WORKDIR /app
# le code est monté en volume (./producer:/app) pour itérer sans rebuild
CMD ["python", "-u", "openmeteo_producer.py"]
