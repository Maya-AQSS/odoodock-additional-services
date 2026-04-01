 
FROM python:3.12-slim

WORKDIR /app

# Dependencias del sistema mínimas
RUN apt-get update -q && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY keycloak_webhook.py .

# Usuario no root
RUN useradd -m -u 1001 webhook
USER webhook

EXPOSE 8000

# Verficación de si la aplicación dentro del contenedor realmente está funcionando
HEALTHCHECK --interval=90s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "keycloak_webhook:app", "--host", "0.0.0.0", "--port", "8000"]
