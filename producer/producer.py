#!/usr/bin/env python3
import os
import json
import time
import random
import pika
from dotenv import load_dotenv

# ————————————————
# Carga el .env que está una carpeta arriba
# ————————————————
here = os.path.dirname(__file__)
dotenv_path = os.path.join(here, '..', '.env')
load_dotenv(dotenv_path)

# ————————————————
# Variables de entorno
# ————————————————
RABBITMQ_HOST  = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER  = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS  = os.getenv('RABBITMQ_PASS', 'guest')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'weather_data')

# ————————————————
# Conexión a RabbitMQ
# ————————————————
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters  = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
connection  = pika.BlockingConnection(parameters)
channel     = connection.channel()

# Cola durable para evitar pérdida de mensajes
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

# ————————————————
# Datos simulados
# ————————————————
STATIONS = ['S1', 'S2', 'S3', 'S4']

def generar_dato():
    return {
        'station_id': random.choice(STATIONS),
        'temperature': round(random.uniform(-20, 50), 2),  # °C
        'humidity':    round(random.uniform(0, 100), 2),   # %
        'wind_speed':  round(random.uniform(0, 30), 2),    # m/s
        'timestamp':   time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }

# ————————————————
# Loop principal
# ————————————————
try:
    print(f'▶️  Producer arrancado. Publicando en {RABBITMQ_HOST}:{RABBITMQ_QUEUE}')
    while True:
        dato = generar_dato()
        mensaje = json.dumps(dato)
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=mensaje,
            properties=pika.BasicProperties(delivery_mode=2)  # marca como persistent
        )
        print(f'📤 Enviado: {mensaje}')
        time.sleep(5)

except KeyboardInterrupt:
    print('\n⏹ Producer detenido por el usuario')

finally:
    connection.close()
    print('🔌 Conexión cerrada')
