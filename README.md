# data-engineering-proyecto-4

Pipeline de datos para clima y pronósticos: ingesta desde OpenWeather (históricos, stream Airbyte y forecast vía Kafka), procesamiento ETL con Apache Spark (Raw → Silver → Gold) y orquestación con Airflow. Incluye dashboard Streamlit para consumo de datos Gold.

---

## Resumen del pipeline

| Flujo | Origen | Destino | Orquestación |
|-------|--------|---------|--------------|
| **Clima (histórico + stream)** | JSON en S3 (históricos + Airbyte `stream/`) | Silver → Gold (resumen diario, patrones horarios) | DAG `spark_etl_pipeline`: Airbyte sync → Spark Silver → Spark Gold |
| **Forecast** | API OpenWeather Forecast → Kafka → consumer Spark | RAW forecast → Silver forecast → Gold forecast | DAG `forecast_pipeline`: producer Kafka (RUN_ONCE) → consumer (timeout 5 min) → Silver → Gold |

Los datos Gold (clima y forecast) se consumen desde el mismo bucket S3 mediante el dashboard en **streamlit/**.

---

## Estructura del proyecto

```
data-engineering-proyecto-4/
├── airflow/              # Orquestación (DAGs, docker-compose, .env)
├── diseño_pipeline/      # Documentación de diseño (notebook, diagrama ELTL)
├── docs/                 # Documentación central (GitHub Actions, índice)
├── ingesta/              # Scripts y docs: JSON locales → S3, Airbyte → S3
├── kafka/                # Broker KRaft + producer forecast (OpenWeather → topic)
├── spark/                # ETL Spark (Silver, Gold, consumer forecast, Docker)
├── streamlit/            # Dashboard Gold (clima + forecast)
├── .github/workflows/    # CI: lint, validación DAGs, build Docker
├── .env.example          # Variables de entorno (raíz; ingesta/local)
└── requirements.txt      # Dependencias Python (boto3, dotenv, kafka, requests)
```

---

## Documentación por componente

| Componente | Documento | Contenido |
|------------|-----------|-----------|
| **Airflow** | [airflow/CONFIGURACION_Y_PUESTA_EN_MARCHA.md](airflow/CONFIGURACION_Y_PUESTA_EN_MARCHA.md) | EC2, Docker, SSH a Spark/Kafka, DAGs `spark_etl_pipeline` y `forecast_pipeline`, variables `.env` |
| **Spark** | [spark/CONFIGURACION_SPARK_EC2.md](spark/CONFIGURACION_SPARK_EC2.md) | EC2, Docker (master + worker), ETL Silver/Gold, consumer forecast (Kafka → S3), scripts en `/app` |
| **Spark** | [spark/OPTIMIZACIONES_PIPELINE.md](spark/OPTIMIZACIONES_PIPELINE.md) | Dynamic partition overwrite, partition pruning, caché, Parquet, shuffle partitions |
| **Kafka** | [kafka/CONFIGURACION_KAFKA.md](kafka/CONFIGURACION_KAFKA.md) | Broker KRaft, producer forecast, topic `weather_forecast`, integración con Spark consumer |
| **Ingesta** | [ingesta/INGESTA_JSON_LOCAL.md](ingesta/INGESTA_JSON_LOCAL.md) | Subida de JSON históricos desde `data/` a S3 (`historicos/`) |
| **Ingesta** | [ingesta/INGESTA_S3_AIRBYTE.md](ingesta/INGESTA_S3_AIRBYTE.md) | Ingesta a S3 con Airbyte (stream OpenWeather → `stream/`), integración con Airflow |
| **Streamlit** | [streamlit/README.md](streamlit/README.md) | Dashboard Gold, rutas S3, instalación, variables `.env` |
| **CI/CD** | [docs/GITHUB_ACTIONS.md](docs/GITHUB_ACTIONS.md) | Workflows: CI (Ruff, sintaxis), validación DAGs, Docker build |
| **Índice** | [docs/README.md](docs/README.md) | Índice de toda la documentación del proyecto |

---

## Orden recomendado de despliegue

1. **S3**: Crear buckets Raw, Silver y Gold (y configurar IAM si aplica).
2. **Ingesta inicial**: Subir históricos con `ingesta/upload_historicos_to_s3.py`; configurar Airbyte para `stream/` (ver [INGESTA_JSON_LOCAL.md](ingesta/INGESTA_JSON_LOCAL.md) y [INGESTA_S3_AIRBYTE.md](ingesta/INGESTA_S3_AIRBYTE.md)).
3. **Kafka** (instancia EC2): Broker + producer según [kafka/CONFIGURACION_KAFKA.md](kafka/CONFIGURACION_KAFKA.md).
4. **Spark** (instancia EC2): Master + worker, `.env` con buckets y `KAFKA_BOOTSTRAP_SERVERS`; montar `app/` en el contenedor según [spark/CONFIGURACION_SPARK_EC2.md](spark/CONFIGURACION_SPARK_EC2.md).
5. **Airflow** (instancia EC2): Docker Compose, `.env` (Airbyte, SSH), conexiones SSH a Spark y Kafka; copiar DAGs según [airflow/CONFIGURACION_Y_PUESTA_EN_MARCHA.md](airflow/CONFIGURACION_Y_PUESTA_EN_MARCHA.md).
6. **Streamlit** (opcional): Local o EC2/Streamlit Cloud; configurar `BUCKET_NAME_GOLD` y credenciales AWS si no hay rol IAM según [streamlit/README.md](streamlit/README.md).

---

## Variables de entorno por componente

Cada módulo tiene su propio `.env` (y `.env.example`). **No subir `.env` al repositorio.**

| Ubicación | Variables principales | Referencia |
|-----------|------------------------|------------|
| **Raíz** | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (para scripts de ingesta local) | `.env.example` |
| **airflow/** | `AIRFLOW_UID`, `_AIRFLOW_WWW_USER_*`, Airbyte (`AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `*_CONNECTION_ID_*`), `SPARK_PROJECT_PATH`, `KAFKA_PROJECT_PATH`, SSH conn IDs | [CONFIGURACION_Y_PUESTA_EN_MARCHA.md](airflow/CONFIGURACION_Y_PUESTA_EN_MARCHA.md) §5, `airflow/.env.example` |
| **kafka/** | `KAFKA_IP`, `KAFKA_CLUSTER_ID`, `OPENWEATHER_API_KEY`, `OPENWEATHER_CITIES`, `KAFKA_TOPIC`, `POLL_SECS`, `RUN_ONCE` | [CONFIGURACION_KAFKA.md](kafka/CONFIGURACION_KAFKA.md), `kafka/.env.example` |
| **spark/** | `BUCKET_NAME_RAW`, `BUCKET_NAME_SILVER`, `BUCKET_NAME_GOLD`, `S3_REGION`, `KAFKA_BOOTSTRAP_SERVERS` | [CONFIGURACION_SPARK_EC2.md](spark/CONFIGURACION_SPARK_EC2.md) §5, `spark/.env` |
| **streamlit/** | `BUCKET_NAME_GOLD`, opcionalmente `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | [streamlit/README.md](streamlit/README.md), `streamlit/.env.example` |

---

## Requisitos

- Python 3.10+ (3.11 recomendado para Airflow y CI).
- Docker y Docker Compose en las instancias EC2 (Airflow, Spark, Kafka).
- Cuenta AWS (S3, EC2, IAM); opcionalmente Airbyte Cloud y API key de OpenWeatherMap.

---

## Licencia y uso

Proyecto de referencia para el Módulo 4 (Data Engineering). Sustituir siempre placeholders (IPs, buckets, IDs, secretos) por valores propios y no exponer credenciales en el repositorio.
