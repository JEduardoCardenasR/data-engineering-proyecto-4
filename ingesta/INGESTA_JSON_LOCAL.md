# Documentación: Ingesta desde archivos JSON locales a S3

**Fecha:** 2 de marzo de 2026  
**Proyecto:** data-engineering-proyecto-4  
**Documento de referencia (solo lectura).**

Este documento describe la **subida de datos históricos desde archivos JSON locales** al bucket S3. Para la ingesta mediante Airbyte (streaming desde OpenWeather), véase **[INGESTA_S3_AIRBYTE.md](INGESTA_S3_AIRBYTE.md)**.

---

## 1. Contexto: bucket y rutas destino

Los archivos JSON históricos se suben al mismo bucket que utiliza el proyecto:

| Campo | Valor |
|-------|--------|
| **Nombre del bucket** | `data-engineer-modulo-cuatro-raw` |
| **Región** | `us-east-2` (EE. UU. Este – Ohio) |

**Rutas en S3 donde se cargan los históricos:**

| Ruta en S3 | Contenido |
|------------|-----------|
| `historicos/Patagonia/` | Archivo histórico de Patagonia (`Patagonia_-41.json`) |
| `historicos/Riohacha/` | Archivo histórico de Riohacha (`Riohacha_11_538415.json`) |

---

## 2. Componentes

| Archivo | Descripción |
|---------|-------------|
| `ingesta/con_s3.py` | Módulo con la lógica de conexión a S3 y función de subida (usa credenciales del `.env` en la raíz del proyecto). |
| `ingesta/upload_historicos_to_s3.py` | Script que sube `Riohacha_11_538415.json` y `Patagonia_-41.json` (desde `data/`) a `historicos/Riohacha/` y `historicos/Patagonia/` respectivamente. |

---

## 3. Requisitos

- **Variables en `.env`** (raíz del proyecto): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
- **Dependencias:** `pip install -r requirements.txt` (boto3, python-dotenv).
- **Archivos JSON de entrada:** deben estar en la carpeta `data/` con los nombres:
  - `Riohacha_11_538415.json`
  - `Patagonia_-41.json`

---

## 4. Ejecución

Desde la raíz del proyecto:

```bash
python ingesta/upload_historicos_to_s3.py
```

El script leerá los JSON desde `data/` y los subirá a las rutas `historicos/Riohacha/` y `historicos/Patagonia/` del bucket S3.

---

## 5. Verificación en S3

Para comprobar que los archivos se subieron correctamente, revisar en la consola de AWS S3:

- `s3://data-engineer-modulo-cuatro-raw/historicos/Patagonia/`
- `s3://data-engineer-modulo-cuatro-raw/historicos/Riohacha/`

---

*Documento de referencia de la ingesta desde archivos JSON locales a S3 para el proyecto data-engineering-proyecto-4. Para la ingesta mediante Airbyte, véase [INGESTA_S3_AIRBYTE.md](INGESTA_S3_AIRBYTE.md).*
