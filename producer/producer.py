#!/usr/bin/env python3
"""
Producer de datos meteorológicos.

Este script:
  1) Carga configuración desde .env
  2) Se reconecta a RabbitMQ con reintentos
  3) Genera y publica datos JSON cada intervalo
  4) Marca los mensajes como persistentes
"""

import os
import sys
import time
import json
import random
import logging

import pika
from dotenv import load_dotenv

# ─── LOGGING ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)
log = logging.getLogger(__name__)

# ─── CARGA DE ENV ───────────────────────────────────────────────────────────────
HERE = os.path.dirname(__file__)
dotenv_path = os.path.join(HERE, "..", ".env")
load_dotenv(dotenv_path)

RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST",  "rabbitmq")
RABBITMQ_USER  = os.getenv("RABBITMQ_USER",  "user")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS",  "pass")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "weather_data")

# ─── ESTACIONES SIMULADAS ───────────────────────────────────────────────────────
STATIONS = ["S1", "S2", "S3", "S4"]

def generar_dato() -> dict:
    """Genera un diccionario con datos de estación meteorológica simulados."""
    return {
        "station_id": random.choice(STATIONS),
        "temperature": round(random.uniform(-20, 50), 2),
        "humidity":    round(random.uniform(0, 100), 2),
        "wind_speed":  round(random.uniform(0, 30), 2),
        "timestamp":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

def connect_rabbitmq(
    retries: int = 5,
    delay: float = 2.0
) -> pika.BlockingConnection:
    """
    Establece conexión con RabbitMQ, reintentando en caso de fallo.
    Sale con sys.exit(1) tras 'retries' intentos fallidos.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    for attempt in range(1, retries + 1):
        try:
            conn = pika.BlockingConnection(params)
            log.info("✅ Conectado a RabbitMQ %s:5672", RABBITMQ_HOST)
            return conn
        except pika.exceptions.AMQPConnectionError:
            log.warning(
                "RabbitMQ no disponible (%d/%d). Reintentando en %.0f s…",
                attempt, retries, delay
            )
            time.sleep(delay)

    log.critical("❌ No se pudo conectar a RabbitMQ tras %d intentos.", retries)
    sys.exit(1)

def main(interval: float = 5.0):
    """Loop principal: genera y publica datos cada `interval` segundos."""
    conn = connect_rabbitmq()
    channel = conn.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    log.info("▶️  Producer arrancado. Publicando en '%s'", RABBITMQ_QUEUE)
    try:
        while True:
            dato = generar_dato()
            msg = json.dumps(dato)

            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=msg,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            log.info("📤 Enviado: %s", msg)
            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("⏹ Producer detenido por usuario (CTRL+C)")

    finally:
        if conn.is_open:
            conn.close()
            log.info("🔌 Conexión RabbitMQ cerrada")

if __name__ == "__main__":
    main()
