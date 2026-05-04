FROM python:3.12-slim
RUN pip install --no-cache-dir trino==0.330.0
WORKDIR /app
COPY seed.py .
CMD ["python", "/app/seed.py"]
