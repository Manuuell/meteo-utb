# 📦 meteo-utb

Sistema de gestión de logs de estaciones meteorológicas utilizando RabbitMQ, PostgreSQL y Docker.

Permite simular y procesar datos JSON de estaciones (temperatura, humedad, velocidad de viento), publicarlos en RabbitMQ y almacenarlos en PostgreSQL.

---

## 🎯 Características

* **Producers** en Python 3.13+ que generan datos JSON y los publican en un *exchange* o cola de RabbitMQ con mensajes persistentes.
* **Broker** RabbitMQ configurado con cola durable (`weather_data`) y dashboard de administración (puerto 15672).
* **Consumers** en Python que:

  * Procesan mensajes con `manual_ack` y `prefetch_count=1`.
  * Validan rango de temperatura (-50ºC a 100ºC).
  * Persiste en PostgreSQL (tabla `weather_logs`).
  * Manejan errores con `nack` y reconexión automática.
* **Base de datos** PostgreSQL con script de inicialización (`db/init.sql`).
* **Docker Compose** para levantar servicios:

  * rabbitmq (5672/15672)
  * postgres (5432)
  * producer y consumer con volúmenes stateful.
* **Logging** de eventos en consola y persistencia en DB.

---

## 🛠 Instalación y ejecución

1. **Clona el repositorio**

   ```bash
   git clone https://github.com/Manuuell/meteo-utb.git
   cd meteo-utb
   ```

2. **Configura variables de entorno**

   Crea un archivo `.env` en la raíz con:

   ```ini
   RABBITMQ_USER=user
   RABBITMQ_PASS=pass
   RABBITMQ_HOST=rabbitmq
   RABBITMQ_PORT=5672

   POSTGRES_DB=weather
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=admin
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432
   ```

3. **Inicializa contenedores**

   ```bash
   docker compose up -d --build
   ```

4. **Carga el esquema de la base de datos**

   ```bash
   docker exec -i postgres psql -U postgres -d weather < db/init.sql
   ```

5. **Verifica estado**

   * RabbitMQ dashboard: [http://localhost:15672](http://localhost:15672) (user/pass)
   * PostgreSQL: `docker compose exec postgres psql -U postgres -d weather`

6. **Ejecuta logs**

   ```bash
   docker compose logs -f producer  
   docker compose logs -f consumer
   ```

---

## 📂 Estructura de archivos

```
meteo-utb/
├── .env                 
├── db/
│   └── init.sql  
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
├── consumer/
│   ├── Dockerfile
│   ├── consumer.py
│   └── requirements.txt
└── docker-compose.yml
```

---

## 🔧 Detalles técnicos

* **Python** 3.13, **librerías**: `pika`, `psycopg2-binary`, `python-dotenv`.
* **RabbitMQ**: cola durable y ACK manual para evitar pérdida.
* **PostgreSQL**: esquema stateful en volumen Docker.
* **Docker Compose**: orquesta arranque ordenado (`depends_on`) y reinicios automáticos (`restart: always`).

---

