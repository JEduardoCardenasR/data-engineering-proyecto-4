# Índice de documentación — data-engineering-proyecto-4

Este directorio agrupa documentación transversal. La documentación por componente está en cada carpeta del proyecto.

---

## Documentación en este directorio (`docs/`)

| Documento | Descripción |
|-----------|-------------|
| [GITHUB_ACTIONS.md](GITHUB_ACTIONS.md) | Workflows de GitHub Actions: CI (Ruff + sintaxis), validación de DAGs de Airflow, build de imágenes Docker (Spark y Kafka). Cuándo se ejecutan, qué revisan y cómo reproducir en local. |

---

## Documentación por componente

| Componente | Ruta | Documentos |
|------------|------|------------|
| **Visión general** | Raíz | [README.md](../README.md) — Resumen del pipeline, estructura, orden de despliegue, variables de entorno. |
| **Airflow** | [../airflow/](../airflow/) | [CONFIGURACION_Y_PUESTA_EN_MARCHA.md](../airflow/CONFIGURACION_Y_PUESTA_EN_MARCHA.md) — EC2, Docker, SSH, DAGs `spark_etl_pipeline` y `forecast_pipeline`. |
| **Spark** | [../spark/](../spark/) | [CONFIGURACION_SPARK_EC2.md](../spark/CONFIGURACION_SPARK_EC2.md) — Despliegue EC2, ETL Silver/Gold, consumer forecast.<br>[OPTIMIZACIONES_PIPELINE.md](../spark/OPTIMIZACIONES_PIPELINE.md) — Optimizaciones del pipeline ETL. |
| **Kafka** | [../kafka/](../kafka/) | [CONFIGURACION_KAFKA.md](../kafka/CONFIGURACION_KAFKA.md) — Broker, producer forecast, topic, integración con Spark. |
| **Ingesta** | [../ingesta/](../ingesta/) | [INGESTA_JSON_LOCAL.md](../ingesta/INGESTA_JSON_LOCAL.md) — JSON históricos → S3.<br>[INGESTA_S3_AIRBYTE.md](../ingesta/INGESTA_S3_AIRBYTE.md) — Airbyte → S3, integración Airflow. |
| **Streamlit** | [../streamlit/](../streamlit/) | [README.md](../streamlit/README.md) — Dashboard Gold, instalación, configuración. |
| **Diseño** | [../diseño_pipeline/](../diseño_pipeline/) | Notebook de documentación del pipeline y diagrama ELTL (Draw.io). |

---

Para una guía rápida de despliegue y variables, usar el [README principal del proyecto](../README.md).
