#!/usr/bin/env python3
import os
import time
import json

import pika
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# 1️⃣ Carga el .env que está en la raíz del proyecto
#    Asegúrate de lanzar el contenedor con `env_file: - .env`
here = os.path.dirname(__file__)
load_dotenv(os.path.join(here, '..', '.env'))

# 2️⃣ Lee todas las variables de entorno
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'pass')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'weather_data')

DB_CONFIG = {
    'dbname':   os.getenv('POSTGRES_DB',       'weather'),
    'user':     os.getenv('POSTGRES_USER',     'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'admin'),
    'host':     os.getenv('POSTGRES_HOST',     'postgres'),
    'port':     os.getenv('POSTGRES_PORT',     '5432'),
}

# 3️⃣ Espera a que Postgres esté listo (hasta 30s)
print("⏳ Esperando a que PostgreSQL responda...")
for i in range(15):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Conectado a PostgreSQL")
        break
    except psycopg2.OperationalError:
        print(f"   Postgres no disponible, reintentando en 2s… ({i+1}/15)")
        time.sleep(2)
else:
    print("❌ No fue posible conectar a PostgreSQL. Saliendo.")
    exit(1)

# 4️⃣ Conexión a RabbitMQ con credenciales y host dinámico
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
rabbit_params = pika.ConnectionParameters(host=RABBITMQ_HOST,
                                          credentials=credentials)
print(f"🔌 Conectando a RabbitMQ en {RABBITMQ_HOST}:5672 …")
connection = pika.BlockingConnection(rabbit_params)
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

# 5️⃣ Función callback para procesar cada mensaje
def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        print(f"📥 Recibido: {data}")

        # validación de temperatura
        temp = data.get("temperature")
        if temp is None or not (-50 <= temp <= 100):
            print("⚠️ Temperatura fuera de rango o ausente; descartando.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # inserción con psycopg2 y psycopg2.sql
        cursor.execute(
            sql.SQL("""
                INSERT INTO weather_logs
                  (station_id, temperature, humidity, wind_speed, timestamp, received_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """),
            (
                data["station_id"],
                temp,
                data.get("humidity"),
                data.get("wind_speed"),
                data.get("timestamp")
            )
        )
        conn.commit()
        print("✅ Guardado en weather_logs")

        # ack manual
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"❌ Error al procesar mensaje: {e}")
        # en caso de error, se rechaza el mensaje (sin requeue):
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# 6️⃣ Prefetch=1 y comienzo de consumo
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)

print("🟢 [*] Esperando mensajes. Para salir: CTRL+C")
channel.start_consuming()
