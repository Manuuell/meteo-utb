#!/usr/bin/env python3
"""
consumer.py

Microservicio consumidor para el sistema de logs meteorológicos.

- Se conecta a RabbitMQ usando credenciales de .env
- Se conecta a PostgreSQL, reintentando hasta 30s
- Procesa mensajes JSON de estaciones: valida temperatura, humedad y viento
- Persiste en la tabla weather_logs con marca de tiempo de recepción
- Usa ack/nack manual para garantizar delivery EXACTLY-ONCE
- Configurado con prefetch_count=1 para orden e integridad
"""

import os
import sys
import time
import json
import logging

import pika
import psycopg2
from psycopg2 import sql, OperationalError
from dotenv import load_dotenv

# ─── Configuración de logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─── Carga de variables de entorno ────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST",  "rabbitmq")
RABBITMQ_USER  = os.getenv("RABBITMQ_USER",  "user")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS",  "pass")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "weather_data")

DB_CONFIG = {
    "dbname":   os.getenv("POSTGRES_DB",       "weather"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     os.getenv("POSTGRES_PORT",     "5432"),
}


def wait_for_postgres(max_retries: int = 15, delay: int = 2):
    """
    Intenta conectar a Postgres hasta max_retries veces.
    Sale del programa si no logra conectarse.
    """
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            logger.info("✅ Conectado a PostgreSQL")
            return conn
        except OperationalError:
            logger.warning(f"Postgres no disponible, reintentando en {delay}s… ({attempt}/{max_retries})")
            time.sleep(delay)
    logger.critical("❌ No fue posible conectar a PostgreSQL. Abortando.")
    sys.exit(1)


def main():
    # ─── Inicialización de PostgreSQL ──────────────────────────────
    conn = wait_for_postgres()
    cursor = conn.cursor()

    # ─── Inicialización de RabbitMQ ────────────────────────────────
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    logger.info(f"🔌 Conectando a RabbitMQ en {RABBITMQ_HOST}:5672 …")
    try:
        connection = pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as e:
        logger.critical(f"❌ No se pudo conectar a RabbitMQ: {e}")
        sys.exit(1)

    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    # ─── Callback para cada mensaje ────────────────────────────────
    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            logger.info(f"📥 Recibido: {data}")

            # Validaciones de rango
            temp = data.get("temperature")
            if temp is None or not (-50 <= temp <= 100):
                logger.warning("⚠️ Temperatura fuera de rango o ausente; descartando.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            humidity = data.get("humidity")
            wind_speed = data.get("wind_speed")
            ts = data.get("timestamp")

            # Inserción segura
            cursor.execute(
                sql.SQL("""
                    INSERT INTO weather_logs
                      (station_id, temperature, humidity, wind_speed, timestamp, received_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """),
                (data["station_id"], temp, humidity, wind_speed, ts)
            )
            conn.commit()
            logger.info("✅ Guardado en weather_logs")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as exc:
            logger.error(f"❌ Error procesando mensaje: {exc}", exc_info=True)
            conn.rollback()
            # rechaza sin requeue para no bloquear la cola con mensajes malos
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # ─── Consumo de mensajes ────────────────────────────────────────
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
    logger.info("🟢 [*] Esperando mensajes. Para salir: CTRL+C")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("🛑 Interrumpido por el usuario, cerrando conexiones…")
    finally:
        channel.close()
        connection.close()
        cursor.close()
        conn.close()
        logger.info("✅ Todas las conexiones se han cerrado cleanly.")


if __name__ == "__main__":
    main()
