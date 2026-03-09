# Configuración de Kafka – Paso a paso

Documentación para desplegar el broker Kafka (KRaft), el **producer de pronósticos (forecast)** y el flujo hasta el consumer Spark. Un solo documento de referencia para el módulo Kafka.

---

## Flujo del proceso Kafka (forecast)

1. **Producer** (`producer.py`, en contenedor o con `nohup`): cada `POLL_SECS` segundos consulta la API OpenWeatherMap Forecast por cada ciudad en `OPENWEATHER_CITIES`, serializa los mensajes y los publica en el topic `weather_forecast`.
2. **Broker**: mantiene el topic; clientes externos (p. ej. la instancia Spark) se conectan con la IP de esta instancia y el puerto 9092.
3. **Consumer Spark** (en la instancia Spark): el job `spark/app/consumer_forecast.py` lee de `weather_forecast`, particiona por ciudad/año/mes y escribe Parquet en `s3a://BUCKET_NAME_RAW/forecast/`. Documentado en `spark/CONFIGURACION_SPARK_EC2.md` (sección "Consumer de forecast").

---

## Topic y esquema: forecast vs current weather

- **`weather_forecast`** (este producer): datos de **pronóstico** (API `/forecast`). Cada mensaje es una predicción con campos como `forecast_time`, `extraction_at`, `temp_c`, `humidity`, `wind_speed`, `clouds`, `weather_desc`, etc. Esquema distinto al de tiempo real actual.
- **`openweather_topic`**: si en el proyecto se usa otro flujo con **current weather** (API `/weather`), ese topic tendría un esquema con `ts`, `datetime_utc`, `pressure`, etc.

**Importante:** El pipeline (Silver/Gold) o un consumer nuevo que lea de Kafka debe **tratar el esquema de `weather_forecast` aparte** del de "current" en `openweather_topic`. No son intercambiables: son dos contratos distintos (pronóstico vs observado actual).

---

## 1. Levantar instancia (broker)

- **Requerimientos:** Mismos que para Airflow y Spark (recomendado: 8 GB RAM, 2 vCPUs).
- **Llaves:** Mismo par de claves SSH que uses para el resto del proyecto.
- **Almacenamiento:** 20 GiB.
- **Configuración de red (grupo de seguridad):**

| Puerto | Protocolo | ¿Para qué sirve? | Recomendación de seguridad |
|--------|-----------|-------------------|----------------------------|
| 22 | TCP (SSH) | Entrar al servidor por terminal. | Tu IP |
| 9092 | TCP | Puerto principal de Kafka. Aquí se envían los datos. | Anywhere o la IP de tus otras instancias (Spark, etc.). |
| 2181 | TCP | Puerto de Zookeeper. | No se usa en este proyecto (Kafka usa modo KRaft). Solo IP interna o Docker interno si aplicara. |
| 9093 | TCP | (Opcional) Controller interno KRaft / SSL. | No necesario para este test. |

---

## 2. Conectarse por SSH

Entra a Cursor (o tu cliente) y conéctate por SSH a la instancia Kafka:

```bash
ssh -i "tu-clave.pem" ubuntu@<IP_PUBLICA_KAFKA>
```

---

## 3. Instalar Docker y Docker Compose

En la terminal de la instancia:

**Actualizar el sistema:**

```bash
sudo apt update && sudo apt upgrade -y
```

**Instalar Docker y Docker Compose:**

```bash
sudo apt install docker.io -y
sudo apt install docker-compose -y
```

**Configurar permisos:**

```bash
sudo usermod -aG docker $USER
newgrp docker
```

**Verificar:**

```bash
docker --version && docker-compose --version
```

---

## 4. Estructura de carpetas

En la instancia (o clonando el repo), la carpeta `kafka/` debe tener:

```
kafka/
├── docker-compose.yaml   # Servicio Kafka (broker) + producer
├── Dockerfile            # Imagen del producer (Python + kafka-python, requests, python-dotenv)
├── producer.py           # Productor: API OpenWeather Forecast → topic weather_forecast
├── .env                  # Variables de entorno (no subir al repo; copiar desde .env.example)
├── .env.example          # Plantilla de variables (broker + producer)
├── CONFIGURACION_KAFKA.md # Este documento
└── data/                 # Persistencia del broker (se crea al levantar o vacío al inicio)
```

En este proyecto el **consumidor** es un job de Spark Structured Streaming (`spark/app/consumer_forecast.py`), que corre en la instancia Spark, no dentro de la carpeta Kafka.

---

## 5. Configurar variables de entorno

1. Copiar la plantilla: `cp .env.example .env`
2. Editar `.env` y completar al menos:
   - **`KAFKA_IP`**: IP privada de esta instancia (para que Spark y otros clientes conecten al broker).
   - **`KAFKA_CLUSTER_ID`**: ID del cluster KRaft. Generar con `docker run --rm apache/kafka:3.7.0 kafka-storage random-uuid` o usar un valor fijo que no cambies (si la imagen no incluye el script, usar `uuidgen` o similar en el servidor).
   - **`OPENWEATHER_API_KEY`**: Clave de OpenWeatherMap (para que el producer envíe datos de forecast).
   - **`OPENWEATHER_CITIES`**: Ciudades separadas por `;` (ej: `Patagonia,AR;Riohacha,CO`).

El resto de variables están documentadas en `.env.example` (puertos, topic, POLL_SECS, etc.).

---

## 6. Crear `docker-compose.yaml`

Usar el `docker-compose.yaml` del repositorio en `kafka/`: define el servicio **kafka** (broker) y el servicio **producer** (OpenWeather Forecast → topic). Si lo creas a mano, debe incluir `env_file: .env` y las variables de KRaft (KAFKA_IP, KAFKA_CLUSTER_ID, etc.).

---

## 7. Carpeta `data` y permisos

Crear la carpeta de datos si no existe y dar permisos:

```bash
mkdir -p data
sudo chmod -R 777 data
```

---

## 8. Levantar Docker Compose

```bash
cd kafka
docker-compose up -d --build
```

Con `--build` se construye la imagen del producer (Dockerfile) y se levantan broker y producer.

---

## 9. Verificar que los contenedores estén arriba

```bash
docker ps
```

El estado debe mostrar `Up` (y si aplica `healthy`) para los contenedores `kafka` y `kafka-producer`.

---

## 10. Verificar que el broker se anunció bien

Diagnóstico de salud; debe aparecer la IP que usan los clientes:

```bash
docker logs kafka | grep "advertised.listeners"
```

Debe mostrarse la IP privada de la instancia en el puerto 9092.

---

## 11. Crear el topic (opcional, para confirmar que el servidor funciona)

El topic se puede auto-crear cuando el producer publica; si quieres crearlo a mano:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic weather_forecast \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Si todo sale bien: `Created topic weather_forecast.`

*(Nota: en imágenes recientes de Apache Kafka la ruta del script puede ser distinta; si falla, revisar la estructura dentro del contenedor con `docker exec -it kafka ls /opt/kafka/bin/`.)*

---

## 12. Producer (forecast)

En este proyecto el producer va **con el compose**: al hacer `docker-compose up -d --build`, el contenedor `kafka-producer` arranca y ejecuta `producer.py` automáticamente.

### Comportamiento del producer en el compose

| Fase | Qué pasa |
|------|----------|
| **Arranque** | Al hacer `docker-compose up -d`, el contenedor del producer arranca y ejecuta `main()` enseguida. Hace la primera ráfaga (todas las ciudades → API forecast → envío a Kafka). |
| **Ciclo** | Tras ese envío llega a `time.sleep(POLL_SECS)`. El contenedor sigue encendido pero "dormido" ese tiempo (por defecto 600 s = 10 min). |
| **Repetición** | Pasado `POLL_SECS`, el script despierta, hace otra ráfaga y vuelve a dormir. Así en bucle. |
| **Persistencia** | Si reinicias el servidor, Docker (con `restart: unless-stopped`) vuelve a levantar el contenedor y el ciclo empieza de nuevo. |

Si Kafka aún no está listo al arrancar, el producer puede fallar la primera vez; con `restart: unless-stopped` se reintentará hasta que el broker acepte conexiones.

### Si quieres ejecutar el producer manualmente (sin compose)

Con Kafka ya levantado solo como broker:

```bash
cd kafka
nohup python3 producer.py >> producer.log 2>&1 &
```

### Verificar que los datos llegan al topic

En otra terminal (en la instancia Kafka):

```bash
sudo docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic weather_forecast \
  --from-beginning
```

Si se muestran los JSON que envía el producer, está funcionando. Para ver actividad del contenedor producer: `docker logs -f kafka-producer` (si está en sleep no mostrará salida hasta la siguiente ráfaga).

---

## 13. Consumer (Spark Structured Streaming)

El consumidor **no** corre en la instancia Kafka; corre en la **instancia Spark** (donde están las capas Silver y Gold). Los datos que escribe son RAW (forecast) en S3.

1. **Levantar la instancia Spark:** `docker-compose up -d` en el proyecto Spark.
2. **Probar conectividad desde Spark hacia Kafka** (en el servidor Spark):
   ```bash
   nc -zv <IP_PRIVADA_KAFKA> 9092
   ```
   Debe aparecer: `Connection to <IP> 9092 port [tcp/*] succeeded!`
3. **Probar que Spark tenga internet:** `ping -c 3 google.com`
4. **Crear/copiar el script del consumer:** El script `consumer_forecast.py` va en la instancia Spark (p. ej. en `spark-project/app/` o donde montes el código). Si lo tienes en tu máquina:
   ```bash
   docker cp ~/spark-project/app/consumer_forecast.py spark-master:/app/consumer_forecast.py
   ```
   (Ajusta rutas según tu estructura; el objetivo es que el archivo esté dentro del contenedor en la ruta que uses con `spark-submit`.)
5. **Configurar variables de entorno en Spark:** En el `.env` de la carpeta Spark definir `KAFKA_BOOTSTRAP_SERVERS=<IP_PRIVADA_KAFKA>:9092` y `BUCKET_NAME_RAW`. Ver `spark/CONFIGURACION_SPARK_EC2.md`, sección "Consumer de forecast" y "Archivo .env".
6. **Lanzar el proceso de streaming:**
   ```bash
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
     /app/consumer_forecast.py
   ```
   Si el contenedor no recibe el `.env`, pasar las variables a mano:
   ```bash
   docker exec -it -e KAFKA_BOOTSTRAP_SERVERS="172.31.3.189:9092" -e BUCKET_NAME_RAW="tu-bucket-raw" spark-master /opt/spark/bin/spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
     /app/consumer_forecast.py
   ```
7. **Verificar:** Revisar en el bucket RAW la ruta `forecast/` (particiones por city/year/month). Los Parquet deben aparecer según el trigger del consumer (cada 60 s o el valor configurado).

Documentación detallada del consumer: **`spark/CONFIGURACION_SPARK_EC2.md`**, sección **"Consumer de forecast (Kafka → S3 RAW)"**.

---

## Comandos útiles (Kafka)

| Acción | Comando |
|--------|--------|
| Ver logs del producer | `docker-compose logs -f producer` |
| Ver logs de Kafka (broker) | `docker-compose logs -f kafka` |
| Parar todo | `docker-compose down` |

---

## Contenido del módulo Kafka

| Archivo | Descripción |
|---------|-------------|
| `docker-compose.yaml` | Servicios `kafka` (broker) y `producer` (OpenWeather Forecast → topic `weather_forecast`). |
| `Dockerfile` | Imagen del producer (Python + kafka-python, requests, python-dotenv). |
| `producer.py` | Script que consulta la API de forecast y publica en Kafka. Variables en `.env`. |
| `.env` / `.env.example` | Variables de entorno del broker y del producer. No subir `.env` al repositorio. |
| `CONFIGURACION_KAFKA.md` | Este documento (única guía de configuración del módulo Kafka). |
