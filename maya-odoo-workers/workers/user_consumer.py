# -*- coding: utf-8 -*- 

import json
import logging
import os
import signal
import time

import pika
import xmlrpc.client

import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Configuración desde variables de entorno
ODOO_URL      = os.environ["ODOO_URL"]        # http://odoo:8069
ODOO_DB       = os.environ["ODOO_DB"]
ODOO_USER     = os.environ["ODOO_USER"]
ODOO_PASSWORD = os.environ["ODOO_PASSWORD"]

RABBIT_HOST   = os.environ["RABBITMQ_HOST"]     # rabbitmq
RABBIT_PORT   = os.environ["RABBITMQ_PORT"]     # 5672
RABBIT_USER   = os.environ["RABBITMQ_USER"]
RABBIT_PASS   = os.environ["RABBITMQ_PASS"]

VHOST         = os.environ.get("RABBITMQ_VHOST", "/")
QUEUE_NAME    = os.environ["QUEUE_NAME"]


# Conexión Odoo (se reutiliza entre mensajes) 
def connect_odoo():
  common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
  uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
  if not uid:
    raise RuntimeError("Autenticación Odoo fallida")
  models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
  log.info("Conectado a Odoo como uid=%s", uid)

  # obtiene los lenguajes instalados
  # crea al usuario con el idioma por defecto, que es el 0
  languages_output = models.execute_kw(
    ODOO_DB, uid, ODOO_PASSWORD, 
    'res.lang', 'search_read', 
    [[['active','=', True]]], { 'fields': ['code']}
  )

  language_default = languages_output[0]['code']

  return uid, models, language_default


# Lógica de negocio 
def upsert_user(models, uid, data, language):
  """
  Crea o actualiza un usuario en Odoo a partir del payload de Keycloak.
  Devuelve (accion, user_id): accion = 'created' | 'updated' | 'skipped'

  skipped no debería ocurrir. Solo tiene sentido en equivocaciones
  Si lo hace debe comprobar que el usuario no tiene ninguna dependencia 
  en todo el sistema, tanto odoo como el resto (DMS, etc)
  """
  
  rep_str = data.get("representation", "{}")
  representation = json.loads(rep_str)
      
  email = representation.get("email")
  if not email:
    raise ValueError("Payload sin campo 'email'")
  
  dni = representation.get("username")
  if not dni:
    raise ValueError("Payload sin campo 'username'")

  active = representation.get("enabled")
  if not active:
    raise ValueError("Payload sin campo 'enabled'")
  
  if not representation.get("firstName"):
    raise ValueError("Payload sin campo 'firstName'")
  
  if not representation.get("lastName"):
    raise ValueError("Payload sin campo 'lastName'")

  name = representation.get("firstName") + " " + representation.get("lastName")

  vals = {
    "name": name,
    "login":  dni,
    "email":  email,
    "company_ids": [1],  # compañias asignadas (solo CEEDCV, la main)
    "company_id": 1 # compañia por defecto (solo CEEDCV, la main)
  }

  existing = models.execute_kw(
    ODOO_DB, uid, ODOO_PASSWORD,
    "res.users", "search",
    [[["login", "=", dni]]],
  )

  if existing:
    vals['active'] = active

    models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "res.users", "write",
        [existing, vals],
    )
    return "updated", existing[0]
  else: # no existe lo creo
    vals['lang'] = language
    vals['active'] = True

    user_id = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "res.users", "create",
        [vals],
    )
    return "created", user_id

# Callback RabbitMQ
def make_callback(models, uid, language_default):
  def on_message(ch, method, properties, body):
    try:
      data = json.loads(body)
      action, user_id = upsert_user(models, uid, data, language_default)
      log.info("Usuario %s [id=%s] — %s", data.get("email"), user_id, action)
      ch.basic_ack(delivery_tag=method.delivery_tag)

    except (ValueError, json.JSONDecodeError) as e:
      # Mensaje malformado: lo descartamos (dead-letter)
      log.error("Mensaje inválido, descartando: %s", e)
      ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
      # Error transitorio: reencolar para reintentar
      log.error("Error procesando mensaje, reencolando: %s", e)
      ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

  return on_message


# Bucle principal con reconexión automática 
def run():
  uid, models, language_default = connect_odoo()
  retry_delay = 5

  while True:
    try:
      credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
      params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=credentials,
        virtual_host=VHOST,
        heartbeat=600,
        blocked_connection_timeout=300,
      )
      connection = pika.BlockingConnection(params)
      channel = connection.channel()

      # Cola durable: sobrevive a reinicios de RabbitMQ
      channel.queue_declare(queue=QUEUE_NAME, durable=True)
      
      # Dead-letter queue para mensajes inválidos
      channel.queue_declare(queue=f"{QUEUE_NAME}.failed", durable=True)

      # Procesa 1 mensaje a la vez
      channel.basic_qos(prefetch_count=1)
      channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=make_callback(models, uid, language_default),
      )

      log.info("Esperando mensajes en '%s'...", QUEUE_NAME)
      retry_delay = 5  # reset tras conexión exitosa
      channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
      log.warning("RabbitMQ no disponible, reintentando en %ss: %s", retry_delay, e)
      time.sleep(retry_delay)
      retry_delay = min(retry_delay * 2, 60)  # backoff exponencial

    except KeyboardInterrupt:
      log.info("Parada solicitada")
      break


if __name__ == "__main__":
  # Parada limpia con SIGTERM (docker stop)
  signal.signal(signal.SIGTERM, lambda *_: exit(0))
  run()
