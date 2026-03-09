# Configuración de Spark en AWS EC2 – Paso a paso

Documentación del procedimiento para desplegar Apache Spark (master + worker) en una instancia EC2 con Docker.

---

## 1. AWS EC2 – Lanzar instancia

### Parámetros recomendados

| Parámetro | Valor / Nota |
|-----------|--------------|
| **Nombre** | Servidor-Data-Spark (o el que prefieras) |
| **Sistema operativo** | Ubuntu (más cómodo para Docker, Spark y Kafka) |
| **AMI** | Ubuntu Server 24.04 LTS (HVM), SSD Volume Type |
| **Tipo de instancia** | M7i-flex.large (8 GB RAM: Spark master/workers, Airflow, Kafka) |
| **Par de claves** | Crear nuevo par: tipo RSA, descargar `.pem` y guardarlo en lugar seguro |
| **Red** | VPC por defecto, subred sin preferencia, IP pública asignada automáticamente |
| **Almacenamiento** | 30 GB, gp3 (buen equilibrio precio/rendimiento) |

### Grupo de seguridad (reglas de entrada)

| Puerto | Servicio | Uso |
|--------|----------|-----|
| 22 | SSH | Acceso a la terminal de Ubuntu |
| 8080 | Spark Master / Airflow | Interfaz web de Spark (o Airflow si se mapea ahí) |
| 4040 | Spark Application UI | Progreso de los jobs (capa_silver / capa_gold) |
| 8081 | Airflow / Kafka UI | Interfaz de Airflow u otras UIs |
| 9092 | Kafka Broker | (Opcional) Conexión a Kafka desde fuera de AWS |

---

## 2. Instalar Docker en la instancia

### Conexión SSH

1. Ubicarse en la carpeta donde está el archivo `.pem`.
2. Conectar (reemplaza `<tu-clave>.pem` y `<IP_PUBLICA>` por tus valores):

   ```bash
   ssh -i "tu-clave.pem" ubuntu@<IP_PUBLICA>
   ```

3. En Windows, si SSH se queja de permisos del archivo de llave, en PowerShell:

   ```powershell
   icacls.exe "tu-clave.pem" /inheritance:r
   icacls.exe "tu-clave.pem" /grant:r "$($env:USERNAME):R"
   ```

4. Aceptar el fingerprint con `yes` si es la primera vez.

### Comandos en Ubuntu

```bash
# Actualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar Docker
sudo apt install docker.io -y

# Instalar Docker Compose
sudo apt install docker-compose -y

# Añadir usuario al grupo docker (evitar sudo en cada comando)
sudo usermod -aG docker $USER
newgrp docker

# Verificar
docker --version && docker-compose --version
```

---

## 3. Configurar IAM

### Crear rol para EC2

1. En AWS: **IAM** → **Roles** → **Create role**.
2. **Trusted entity**: AWS service → **EC2** → Next.
3. **Permisos**: buscar y marcar **AmazonS3FullAccess** (lectura/escritura en Raw, Silver y Gold).
4. **Nombre del rol**: por ejemplo `Rol-EC2-Data-Engineer` → Create role.

### Asignar el rol a la instancia

1. **EC2** → **Instances** → seleccionar la instancia.
2. **Actions** → **Security** → **Modify IAM role**.
3. Elegir el rol creado → **Update IAM role**.

### Verificar acceso a S3 (opcional)

```bash
# Instalar AWS CLI si no está
sudo apt update
sudo apt install unzip -y
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Comprobar que la instancia ve los buckets
aws s3 ls
```

Si se listan los buckets, el rol tiene los permisos necesarios. **No hace falta configurar access key ni secret en el servidor**; los scripts usan el IAM Role.

---

## 4. Estructura de carpetas en el servidor

En la instancia (por ejemplo en `~/spark-project`):

```text
/home/ubuntu/
└── spark-project/              ← Directorio del proyecto (SPARK_PROJECT_PATH para Airflow)
    ├── app/                    ← Código montado en /app en el contenedor
    │   ├── capa_silver.py
    │   ├── capa_gold.py
    │   ├── consumer_forecast.py   # Streaming: Kafka → RAW forecast
    │   ├── silver_forecast.py    # ETL: RAW forecast → Silver forecast
    │   └── gold_forecast.py      # ETL: Silver forecast → Gold forecast
    ├── Dockerfile
    ├── docker-compose.yaml
    └── .env
```

Comandos:

```bash
mkdir -p spark-project/app
cd spark-project
```

---

## 5. Archivo `.env`

Un solo archivo `.env` en la carpeta Spark alimenta **todos** los jobs: capa_silver, capa_gold, consumer_forecast, silver_forecast y gold_forecast. **No es necesario añadir variables** para los ETL de forecast; usan los mismos buckets. **No incluir credenciales AWS** si se usa IAM Role.

```bash
nano .env
```

Contenido (reemplazar por tus nombres de bucket y región):

```env
# Buckets S3 (usados por todos los scripts: Silver, Gold, consumer y ETL forecast)
BUCKET_NAME_RAW=tu-bucket-raw
BUCKET_NAME_SILVER=tu-bucket-silver
BUCKET_NAME_GOLD=tu-bucket-gold

# Región (importante para Spark y S3)
S3_REGION=us-east-2

# Kafka (solo para consumer_forecast.py): IP:puerto del broker en la instancia Kafka
KAFKA_BOOTSTRAP_SERVERS=IP_KAFKA:9092
```

Si usas el consumer de forecast (`app/consumer_forecast.py`), define `KAFKA_BOOTSTRAP_SERVERS` con la IP privada (y puerto) de la instancia donde corre Kafka. Opcional: `KAFKA_TOPIC=weather_forecast` (por defecto ya es ese valor).

### Variables de entorno por script

| Script | Variables que usa | ¿Nuevas? |
|--------|-------------------|----------|
| capa_silver.py | BUCKET_NAME_RAW, BUCKET_NAME_SILVER | — |
| capa_gold.py | BUCKET_NAME_SILVER, BUCKET_NAME_GOLD | — |
| consumer_forecast.py | BUCKET_NAME_RAW, KAFKA_BOOTSTRAP_SERVERS (o KAFKA_IP + KAFKA_PORT), KAFKA_TOPIC opcional | — |
| silver_forecast.py | BUCKET_NAME_RAW, BUCKET_NAME_SILVER | No; mismas que Silver/Gold |
| gold_forecast.py | BUCKET_NAME_SILVER, BUCKET_NAME_GOLD | No; mismas que capa_gold |

Con el contenido de `.env` anterior no hace falta modificar nada para ejecutar los ETL de forecast.

Guardar: `Ctrl+O`, Enter, `Ctrl+X`. Verificar: `cat .env`.

---

## 6. Dockerfile

Desde `~/spark-project`:

```bash
nano Dockerfile
```

Contenido (el del proyecto en `spark/Dockerfile`): imagen base `apache/spark:3.5.0`, Python, pip, `pandas`, `pyspark==3.5.0`, `boto3`, JARs S3A (hadoop-aws, aws-java-sdk-bundle), JARs Kafka (spark-sql-kafka-0-10, spark-token-provider-kafka-0-10, kafka-clients, commons-pool2) para `consumer_forecast.py`, `WORKDIR /app`, `COPY ./app /app`, `CMD ["tail", "-f", "/dev/null"]`.

---

## 7. Docker Compose

Desde `~/spark-project`:

```bash
nano docker-compose.yaml
```

Configuración típica (ajustar memoria según tu instancia):

- **spark-master**: build `.`, volumen `./app:/app`, `env_file: .env`, puertos 8080, 7077, 4040. Variables: `SPARK_MODE=master`, `SPARK_DRIVER_MEMORY=2G`, `SPARK_EXECUTOR_MEMORY=4G`, límite de memoria 7G (dejando ~1 GB para el SO).
- **spark-worker**: mismo build y volumen, `depends_on: spark-master`, `SPARK_MODE=worker`, `SPARK_MASTER_URL=spark://spark-master:7077`, `SPARK_WORKER_MEMORY=5G`, `SPARK_WORKER_CORES=2`, límite 7G.

Referencia exacta en el repositorio: `spark/docker-compose.yaml`.

---

## 8. Construir y levantar

```bash
cd ~/spark-project
docker-compose up --build -d
```

Comprobar:

```bash
docker ps
docker exec -it spark-master ls /app
```

Deben verse `capa_silver.py`, `capa_gold.py`, `consumer_forecast.py`, `silver_forecast.py` y `gold_forecast.py`.

---

## 9. Probar conexión a S3 (opcional)

Crear `app/prueba_s3.py`:

```python
from pyspark.sql import SparkSession
import os

bucket_raw = os.getenv("BUCKET_NAME_RAW")
spark = SparkSession.builder.appName("PruebaConexionS3").getOrCreate()

print(f"\n>>> Intentando listar en: s3a://{bucket_raw}/")
try:
    df = spark.read.text(f"s3a://{bucket_raw}/")
    df.show(5)
    print("\n✅ CONEXIÓN EXITOSA. Spark puede leer S3.")
except Exception as e:
    print(f"\n❌ ERROR: {e}")
spark.stop()
```

Ejecutar:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /app/prueba_s3.py
```

Si todo va bien, después se puede borrar `app/prueba_s3.py`.

---

## 10. Ejecutar ETL Silver y Gold

```bash
# ETL Silver (Raw → Silver, Parquet – clima stream/históricos)
docker exec -it spark-master /opt/spark/bin/spark-submit /app/capa_silver.py

# ETL Gold (Silver → Gold, Parquet)
docker exec -it spark-master /opt/spark/bin/spark-submit /app/capa_gold.py

# ETL Silver Forecast (RAW forecast → Silver forecast; ver 10c)
docker exec -it spark-master /opt/spark/bin/spark-submit /app/silver_forecast.py

# ETL Gold Forecast (Silver forecast → Gold forecast; ver 10d)
docker exec -it spark-master /opt/spark/bin/spark-submit /app/gold_forecast.py
```

Interfaz web de Spark: `http://<IP_PUBLICA>:8080`. Progreso del job: `http://<IP_PUBLICA>:4040` mientras corre un script.

---

## 10b. Consumer de forecast (Kafka → S3 RAW)

El script **`app/consumer_forecast.py`** es un job de **Spark Structured Streaming** que lee mensajes del topic **`weather_forecast`** (Kafka), los particiona por ciudad/año/mes y escribe Parquet en **`s3a://BUCKET_NAME_RAW/forecast/`**. Es el downstream del producer de Kafka (ver `kafka/CONFIGURACION_KAFKA.md`).

### Qué hace el proceso

| Paso | Descripción |
|------|-------------|
| Entrada | Topic Kafka `weather_forecast` (mensajes publicados por `kafka/producer.py` con esquema forecast). |
| Lectura | `readStream` desde el broker configurado en `KAFKA_BOOTSTRAP_SERVERS` (o `KAFKA_IP` + `KAFKA_PORT`). |
| Transformación | Parsea JSON, deriva `year` y `month` de `forecast_time`, aplica `coalesce(1)` por micro-batch. |
| Salida | Parquet en `s3a://BUCKET_NAME_RAW/forecast/` con `partitionBy("city", "year", "month")`. El detalle por día queda en la columna `forecast_time`. |
| Trigger | Cada 60 s (o el valor configurado en el script) procesa un micro-batch. |

### Variables de entorno necesarias

En el `.env` de la carpeta Spark (el que carga el contenedor):

- **`BUCKET_NAME_RAW`**: bucket donde se escribe la ruta `forecast/` (mismo que para Silver).
- **`KAFKA_BOOTSTRAP_SERVERS`**: `IP_KAFKA:9092` (IP privada de la instancia donde corre Kafka). Opcional: `KAFKA_IP` y `KAFKA_PORT` por separado.
- Opcional: **`KAFKA_TOPIC`** (por defecto `weather_forecast`).

Ver sección **5. Archivo `.env`** en este mismo documento para el ejemplo de contenido.

### Cómo ejecutarlo

Es un job **de larga duración** (streaming): corre hasta que se detenga manualmente. Ejecutar desde el contenedor master. El Dockerfile incluye los JARs de Kafka en `/opt/spark/jars/`; hay que pasarlos con `--jars` para que Spark cargue el data source `kafka`:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar \
  /app/consumer_forecast.py
```

El job mantendrá la sesión abierta; los datos se escriben en S3 según el trigger. Para dejarlo en segundo plano, usar `-d` o ejecutar en una sesión persistente (screen/nohup).

### Documentación relacionada

- **Kafka (producer y broker):** `kafka/CONFIGURACION_KAFKA.md`, `kafka/.env.example`.
- **Esquema del topic:** coincide con el producer en `kafka/producer.py` (source, city, lat, lon, extraction_at, forecast_time, temp_c, humidity, wind_speed, clouds, weather_desc).

---

## 10c. Silver Forecast (RAW forecast → Silver forecast)

El script **`app/silver_forecast.py`** lee los Parquet de **`s3a://BUCKET_NAME_RAW/forecast/`** (escritos por `consumer_forecast.py`), aplica limpieza, deduplicación por (city, forecast_time), enriquecimiento (p. ej. `sky_condition` por nubosidad) y escribe en **`s3a://BUCKET_NAME_SILVER/forecast/`** particionado por **city** y **year**, con overwrite dinámico (solo se sobrescriben las particiones escritas en esa ejecución).

### Variables de entorno

Las mismas que el resto de los ETL: **`BUCKET_NAME_RAW`** y **`BUCKET_NAME_SILVER`** (ver sección 5). No requiere variables adicionales.

### Cuándo ejecutarlo

Después de que el **consumer_forecast** haya escrito datos en RAW (ruta `forecast/`). Se puede ejecutar a demanda o integrarse en un cron/DAG (p. ej. después de un batch de consumer o en paralelo a capa_silver).

### Comando

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /app/silver_forecast.py
```

---

## 10d. Gold Forecast (Silver forecast → Gold)

El script **`app/gold_forecast.py`** lee la capa Silver forecast (**`s3a://BUCKET_NAME_SILVER/forecast/`**), normaliza los nombres de columnas (p. ej. `wind_speed` → `velocidad_viento_m_s`, `temp_c` → `temperatura_c`, `clouds` → `nubes_porcentaje`) y aplica las mismas fórmulas de potencial que la Gold de clima: **WPI** (Wind Potential Index) y **SPI** (Solar Potential Index). Escribe en Gold:

- **`s3a://BUCKET_NAME_GOLD/gold/forecast/resumen_clima_diario/`** (particionado por ciudad, mes_anio)
- **`s3a://BUCKET_NAME_GOLD/gold/forecast/patrones_horarios/`** (particionado por ciudad, mes_anio)

Variables de entorno: `BUCKET_NAME_SILVER`, `BUCKET_NAME_GOLD` (mismas que el resto de los ETL). Ejecutar después de `silver_forecast.py`:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /app/gold_forecast.py
```

---

## 11. Cómo se cubre la consigna

### 1. Scripts PySpark para preguntas de negocio

**Capa Silver — Transformaciones aplicadas:**

| Entrada (Raw) | Transformación | Salida (Silver) |
|---------------|----------------|-----------------|
| JSON anidado (OpenWeatherMap) | Aplana estructuras (`safe_flatten_structs`) | Columnas planas |
| Nombres variados (`main.temp`, `wind.speed`) | Estandariza columnas | Nombres consistentes (`temperatura_c`, `velocidad_viento_m_s`) |
| Datos sucios (nulos, inválidos) | Filtra registros sin ciudad/temperatura/timestamp | Datos limpios |
| JSON / JSONL.GZ | Convierte formato | Parquet particionado (ciudad/event_year) |
| Históricos + Stream separados | Une con `unionByName` | Dataset unificado |

**Capa Gold — Transformaciones aplicadas:**

| Entrada (Silver) | Transformación | Salida (Gold) |
|------------------|----------------|---------------|
| `timestamp_iso` (timestamp) | Extrae `fecha_solo`, `hora_solo`, `mes_anio` | Campos temporales para agrupación |
| `velocidad_viento_m_s` | Calcula WPI = `(velocidad³) / 100` | `potencial_eolico_index` |
| `indice_uv`, `nubes_porcentaje` | Calcula SPI = `UV × (1 - nubes/100)` | `potencial_solar_index` |
| Registros horarios | `groupBy(ciudad, mes_anio, fecha_solo)` + agregaciones | `resumen_clima_diario` (1 fila/día) |
| Registros horarios | `groupBy(ciudad, mes_anio, hora_solo)` + agregaciones | `patrones_horarios` (24 filas/mes) |
| Parquet (Silver) | Convierte a Parquet particionado | Parquet (ciudad/mes_anio) |

**Cálculos de índices de potencial energético:**

| Índice | Fórmula | Descripción |
|--------|---------|-------------|
| **WPI** (Wind Potential Index) | `round(pow(velocidad_viento_m_s, 3) / 100, 2)` | Potencial eólico proporcional al cubo de la velocidad del viento. Se divide por 100 para normalizar el rango. |
| **SPI** (Solar Potential Index) | `round(indice_uv × (1 - nubes_porcentaje/100), 2)` | Potencial solar basado en UV, atenuado por nubosidad. Si UV es nulo, usa valor base de 5.0. |

**Tabla `resumen_clima_diario` — Agregaciones por día:**

| Campo | Cálculo | Descripción |
|-------|---------|-------------|
| `promedio_potencial_eolico_diario` | `avg(potencial_eolico_index)` | Promedio del WPI del día |
| `max_potencial_eolico_diario` | `max(potencial_eolico_index)` | Máximo WPI del día |
| `min_potencial_eolico_diario` | `min(potencial_eolico_index)` | Mínimo WPI del día |
| `promedio_potencial_solar_diario` | `avg(potencial_solar_index)` | Promedio del SPI del día |
| `max_potencial_solar_diario` | `max(potencial_solar_index)` | Máximo SPI del día |
| `min_potencial_solar_diario` | `min(potencial_solar_index)` | Mínimo SPI del día |
| `promedio_temperatura_diaria_c` | `avg(temperatura_c)` | Temperatura promedio del día |
| `max_temperatura_registro_c` | `max(temperatura_c)` | Temperatura máxima del día |
| `min_temperatura_registro_c` | `min(temperatura_c)` | Temperatura mínima del día |
| `promedio_humedad_diaria` | `avg(humedad_porcentaje)` | Humedad promedio del día |
| `total_precipitacion_diaria_mm` | `sum(lluvia_1h_mm)` | Precipitación total acumulada |
| `max_velocidad_viento_m_s` | `max(velocidad_viento_m_s)` | Ráfaga máxima del día |
| `promedio_nubes_diario` | `avg(nubes_porcentaje)` | Nubosidad promedio del día |
| `fecha_procesamiento_gold` | `current_timestamp()` | Timestamp de auditoría |

**Tabla `patrones_horarios` — Agregaciones por hora/mes:**

| Campo | Cálculo | Descripción |
|-------|---------|-------------|
| `promedio_eolico_por_hora_mes` | `avg(potencial_eolico_index)` | Promedio WPI por hora del día en el mes |
| `promedio_solar_por_hora_mes` | `avg(potencial_solar_index)` | Promedio SPI por hora del día en el mes |
| `promedio_temperatura_por_hora_mes` | `avg(temperatura_c)` | Temperatura promedio por hora en el mes |
| `promedio_humedad_por_hora_mes` | `avg(humedad_porcentaje)` | Humedad promedio por hora en el mes |
| `id_analisis_patron` | `concat(ciudad, mes_anio, hora_solo)` | ID único del patrón (ej: "Patagonia-2026-03-14") |
| `fecha_procesamiento_gold` | `current_timestamp()` | Timestamp de auditoría |

**Flujo de transformación Gold:**

```
Silver (Parquet)
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Lectura condicional (primera ejecución vs incremental)  │
│     - Primera: lee todo Silver                              │
│     - Incremental: filtra event_year + mes_anio actual      │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Preparación de campos temporales                        │
│     - fecha_hora_utc = cast(timestamp_iso)                  │
│     - fecha_solo = to_date(fecha_hora_utc)                  │
│     - hora_solo = date_format(HH)                           │
│     - mes_anio = date_format(yyyy-MM)                       │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Cálculo de índices de potencial energético              │
│     - WPI = (velocidad_viento³) / 100                       │
│     - SPI = UV × (1 - nubes/100)                            │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Cache del DataFrame (optimización)                      │
│     - Evita releer S3 en las dos agregaciones               │
└─────────────────────────────────────────────────────────────┘
     │
     ├────────────────────────┬────────────────────────────────┐
     ▼                        ▼                                │
┌──────────────────┐   ┌──────────────────┐                    │
│ groupBy(ciudad,  │   │ groupBy(ciudad,  │                    │
│ mes_anio,        │   │ mes_anio,        │                    │
│ fecha_solo)      │   │ hora_solo)       │                    │
│                  │   │                  │                    │
│ → Agregaciones   │   │ → Agregaciones   │                    │
│   diarias        │   │   por hora/mes   │                    │
└────────┬─────────┘   └────────┬─────────┘                    │
         │                      │                              │
         ▼                      ▼                              │
┌──────────────────┐   ┌──────────────────┐                    │
│ resumen_clima_   │   │ patrones_        │                    │
│ diario           │   │ horarios         │                    │
│ (Parquet)        │   │ (Parquet)        │                    │
│ ciudad/mes_anio  │   │ ciudad/mes_anio  │                    │
└──────────────────┘   └──────────────────┘                    │
```

Las preguntas de negocio se responden en el **reporte** del proyecto usando estas tablas.

### 2. Combinación de datos no estructurados y estructurados

- **Origen**: JSON (históricos) y JSONL.GZ (stream) en S3.
- **Transformación**: `safe_flatten_structs` para unificar esquemas anidados en una estructura tabular.
- **Resultado**: análisis integral en el Data Lake con fuentes heterogéneas.

### 3. Optimización en Apache Spark

El pipeline implementa múltiples optimizaciones para reducir tiempo de ejecución y costos de S3.

**Ver documento detallado:** [`OPTIMIZACIONES_PIPELINE.md`](./OPTIMIZACIONES_PIPELINE.md)

**Resumen de técnicas aplicadas:**
- Dynamic Partition Overwrite (Silver y Gold)
- Partition Pruning en lecturas
- Uso estratégico de caché
- Control de shuffle partitions
- Formato Parquet columnar con particionamiento

### 4. Configuración del entorno

- **Docker**: memoria limitada por contenedor (p. ej. 7G) para no colapsar la instancia.
- **Driver/Executor**: configuración vía variables de entorno en el compose (SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY) o en el propio compose.

### 5. Ejecución vía Spark-Submit

- Los jobs se lanzan con el comando de producción:

  ```bash
  docker exec -it spark-master /opt/spark/bin/spark-submit /app/capa_silver.py
  docker exec -it spark-master /opt/spark/bin/spark-submit /app/capa_gold.py
  ```

- Opcional: orquestación desde Airflow con SSH al servidor y el mismo `spark-submit` (o `python3 /app/...` dentro del contenedor).

---

## 12. Puntos clave para la entrega

1. **Scripts PySpark**: `capa_silver.py` y `capa_gold.py` transforman datos para responder preguntas de negocio.
2. **Formato Parquet**: Silver y Gold almacenan datos en formato columnar particionado.
3. **Particionamiento**: Silver por `ciudad/event_year`, Gold por `ciudad/mes_anio`.
4. **Optimizaciones**: Ver [`OPTIMIZACIONES_PIPELINE.md`](./OPTIMIZACIONES_PIPELINE.md) para detalles de Dynamic Partition Overwrite, caché, shuffle y partition pruning.

---

## 13. Estructura de particiones en Silver

Después de ejecutar Silver, la estructura en S3 es:

```
s3://bucket-silver/silver/clima_procesado/
├── ciudad=Patagonia/
│   ├── event_year=2023/
│   │   └── part-00000.parquet
│   ├── event_year=2024/
│   │   └── part-00000.parquet
│   ├── event_year=2025/
│   │   └── part-00000.parquet
│   └── event_year=2026/          ← Se sobrescribe cada ejecución
│       └── part-00000.parquet
└── ciudad=Riohacha/
    ├── event_year=2023/
    ├── event_year=2024/
    ├── event_year=2025/
    └── event_year=2026/          ← Se sobrescribe cada ejecución
```

**Beneficios de esta estructura**:
- Los datos históricos (2023-2025) permanecen intactos después de la primera carga.
- Las ejecuciones diarias solo reprocesan y sobrescriben la partición del año actual (2026).
- Gold lee todas las particiones como un solo dataset unificado.

---

## 14. Estructura de particiones en Gold

```
s3://bucket-gold/gold/
├── resumen_clima_diario/
│   ├── ciudad=Patagonia/
│   │   ├── mes_anio=2024-01/
│   │   │   └── part-00000.parquet
│   │   └── ... (un folder por mes)
│   └── ciudad=Riohacha/
│       └── ... (misma estructura)
└── patrones_horarios/
    └── ... (misma estructura)
```

**Para detalles de optimización y beneficios, ver:** [`OPTIMIZACIONES_PIPELINE.md`](./OPTIMIZACIONES_PIPELINE.md)

---

*Documentación generada para el proyecto data-engineering-proyecto-4 (Módulo 4).*
