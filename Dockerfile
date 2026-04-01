FROM python:3.11-slim

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV JAVA_HOME=/usr/lib/jvm/default-java
