FROM python:3.12-slim

RUN pip install --no-cache-dir fastapi "uvicorn[standard]" hdfs pandas pyarrow

WORKDIR /app
# le code est monte en volume (./webapp:/app) pour iterer sans rebuild
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
