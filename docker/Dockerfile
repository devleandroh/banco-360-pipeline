# Imagem do pipeline: Python 3.11 + Java 17 + PySpark 3.5.4 (mesmas versões do AWS Glue 5.0)
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    LAKE_DIR=/data/lake \
    LANDING_DIR=/data/landing

WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY . .

ENTRYPOINT ["python", "-m", "banco360"]
CMD ["--help"]
