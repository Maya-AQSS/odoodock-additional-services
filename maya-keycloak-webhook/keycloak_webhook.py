"""
keycloak_webhook.py
Maya AQSS — Receptor de eventos Keycloak → RabbitMQ

Recibe eventos HTTP de Keycloak y los publica en RabbitMQ.
Keycloak 24 soporta webhooks HTTP nativos desde Admin Console:
  Realm Settings → Events → Event listeners → Add listener (http)

Eventos mapeados a colas:
  ADMIN: CREATE_USER   → kc.user.created
  ADMIN: UPDATE_USER   → kc.user.updated
  ADMIN: DELETE_USER   → kc.user.disabled
"""

import json
import logging
import os
import time
from contextlib import asynccontextmanager

import pika
import pika.exceptions
from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.responses import JSONResponse

# Configuración
RABBIT_HOST     = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBIT_PORT     = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBIT_USER     = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBIT_PASS     = os.getenv("RABBITMQ_DEFAULT_PASSWORD", "guest")
RABBIT_VHOST    = os.getenv("RABBITMQ_VHOST", "maya_sync")
RABBIT_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "ex.keycloak.events")

# Token secreto compartido con Keycloak para validar las llamadas
WEBHOOK_SECRET  = os.getenv("KEYCLOAK_WEBHOOK_SECRET", "")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("keycloak-webhook")

#Mapeo de eventos Keycloak → routing keys

# Eventos de administración (type = "ADMIN_EVENT", resourceType = "USER")
ADMIN_EVENT_MAP = {
    "CREATE": "kc.user.created",
    "UPDATE": "kc.user.updated",
    "DELETE": "kc.user.disabled",
}

# Conexión RabbitMQ con reconexión automática
class RabbitPublisher:
  """
  Gestiona la conexión a RabbitMQ con reconexión automática.
  """

  def __init__(self):
    self._connection = None
    self._channel    = None

  def _connect(self):
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
      host=RABBIT_HOST,
      port=RABBIT_PORT,
      virtual_host=RABBIT_VHOST,
      credentials=credentials,
      heartbeat=60,
      blocked_connection_timeout=30,
    )
    self._connection = pika.BlockingConnection(params)
    self._channel    = self._connection.channel()

    # Declarar el exchange como idempotente (no falla si ya existe)
    self._channel.exchange_declare(
        exchange=RABBIT_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )
    log.info("Conectado a RabbitMQ en %s:%s vhost=%s", RABBIT_HOST, RABBIT_PORT, RABBIT_VHOST)

  def connect_with_retry(self, retries: int = 10, delay: int = 3):
    for attempt in range(1, retries + 1):
      try:
        self._connect()
        return
      except Exception as e:
        log.warning("Intento %d/%d — RabbitMQ no disponible: %s", attempt, retries, e)
        if attempt < retries:
          time.sleep(delay)

    raise RuntimeError(f"No se pudo conectar a RabbitMQ tras {retries} intentos")

  def publish(self, routing_key: str, payload: dict) -> bool:
    """
    Publica un mensaje. Reconecta automáticamente si la conexión se perdió.
    """

    body = json.dumps(payload, ensure_ascii=False).encode()

    for attempt in range(3):
      try:
        if not self._connection or self._connection.is_closed:
          log.warning("Conexión cerrada, reconectando...")
          self._connect()

        self._channel.basic_publish(
            exchange=RABBIT_EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistente
            ),
        )
        log.info("Publicado [%s] → %s", routing_key, payload.get("keycloak_id", "?"))
        return True

      except (pika.exceptions.ConnectionClosed,
              pika.exceptions.ChannelClosed,
              pika.exceptions.StreamLostError) as e:
        log.warning("Error de conexión (intento %d/3): %s", attempt + 1, e)
        time.sleep(1)
      except Exception as e:
        log.error("Error publicando en RabbitMQ: %s", e)
        return False

    log.error("No se pudo publicar tras 3 intentos")
    return False

  def close(self):
      if self._connection and not self._connection.is_closed:
          self._connection.close()


# Instancia global del publisher
rabbit = RabbitPublisher()

# Ciclo de vida de la aplicación
@asynccontextmanager
async def lifespan(app: FastAPI):
  log.info("Iniciando keycloak-webhook...")
  rabbit.connect_with_retry()
  yield
  log.info("Cerrando keycloak-webhook...")
  rabbit.close()


app = FastAPI(
    title="Keycloak Webhook — Maya AQSS",
    description="Receptor de eventos Keycloak que publica en RabbitMQ",
    version="1.0.0",
    lifespan=lifespan,
)

def validate_secret(authorization: str | None) -> bool:
  """
  Valida el header Authorization: Bearer <secret>
  """

  if not WEBHOOK_SECRET:
      return True  # sin secreto configurado, acepta todo (solo para desarrollo)
  if not authorization:
      return False
  parts = authorization.split(" ", 1)
  if len(parts) != 2 or parts[0].lower() != "bearer":
      return False
  return parts[1] == WEBHOOK_SECRET

def build_payload(event_type: str, routing_key: str, raw: dict) -> dict:
    """
    Construye un payload normalizado
    """

    # Keycloak envía el userId en distintos campos según el tipo de evento
    user_id = (
        raw.get("userId")           # evento de usuario final
        or raw.get("resourceId")    # admin event sobre USER
        or raw.get("details", {}).get("userId", "")
    )

    realm = raw.get("realmId") or raw.get("realmName", "")

    return {
        "event_type":   event_type,
        "routing_key":  routing_key,
        "keycloak_id":  user_id,
        "realm":        realm,
        "timestamp":    raw.get("time", int(time.time() * 1000)),
        # Incluir detalles extra si los hay (email, username en algunos eventos)
        "details":      raw.get("details", {}),
        # Payload completo original por si n8n necesita campos adicionales
        "_raw":         raw,
    }

### ENDPOINTS

@app.post("/webhook/keycloak")
async def keycloak_webhook(request: Request, authorization: str | None = Header(default=None)):
    """
    Endpoint receptor de eventos de Keycloak.
    Configurar en Keycloak: Realm Settings → Events → Webhook URL
    """
    if not validate_secret(authorization):
      log.warning("Petición rechazada — token inválido desde %s", request.client.host)
      raise HTTPException(status_code=401, detail="Unauthorized")

    try:
      raw = await request.json()
    except Exception:
      raise HTTPException(status_code=400, detail="Invalid JSON")

    log.debug("Evento recibido: %s", json.dumps(raw, indent=2))

    event_class = raw.get("type", "")        # "EVENT" o "ADMIN_EVENT"
    event_type  = raw.get("operationType") or raw.get("type", "")

    routing_key = None

    #  Evento de administración sobre recurso USER
    if raw.get("resourceType") == "USER":
      operation = raw.get("operationType", "")
      routing_key = ADMIN_EVENT_MAP.get(operation)

    if not routing_key:
      log.debug("Evento ignorado: type=%s operationType=%s resourceType=%s",
                  event_class,
                  raw.get("operationType", "-"),
                  raw.get("resourceType", "-"))
      return JSONResponse({"status": "ignored", "reason": "event not mapped"})

    payload = build_payload(event_type, routing_key, raw)

    published = rabbit.publish(routing_key, payload)
    if not published:
      # Devuelve 503 para que Keycloak pueda reintentar
      raise HTTPException(status_code=503, detail="Could not publish to RabbitMQ")

    return JSONResponse({"status": "ok", "routing_key": routing_key})

@app.get("/health")
async def health():
  """Health check para Docker y Traefik."""

  rabbit_ok = (
    rabbit._connection is not None
    and not rabbit._connection.is_closed
  )

  status = "ok" if rabbit_ok else "degraded"
  
  return JSONResponse(
      {"status": status, "rabbit": "connected" if rabbit_ok else "disconnected"},
      status_code=200 if rabbit_ok else 503,
  )
