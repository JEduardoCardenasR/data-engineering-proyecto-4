# Documentación: Ingesta de datos a S3 y configuración Airbyte

**Fecha:** 2 de marzo de 2026  
**Proyecto:** data-engineering-proyecto-4  
**Documento de referencia (solo lectura).**

---

## 1. Bucket S3 y estructura

| Campo | Valor |
|-------|--------|
| **Nombre del bucket** | `data-engineer-modulo-cuatro-raw` |
| **Región** | `us-east-2` (EE. UU. Este – Ohio) |

### Estructura de carpetas

```
data-engineer-modulo-cuatro-raw/
├── historicos/
│   ├── Patagonia/
│   └── Riohacha/
└── stream/
    ├── Patagonia/
    └── Riohacha/
```

| Ruta en S3 | Uso |
|------------|-----|
| `historicos/Patagonia/` | Archivos históricos de Patagonia (subida manual desde código) |
| `historicos/Riohacha/` | Archivos históricos de Riohacha (subida manual desde código) |
| `stream/Patagonia/` | Datos ingeridos por Airbyte (conexión Patagonia) |
| `stream/Riohacha/` | Datos ingeridos por Airbyte (conexión Riohacha) |

---

## 2. IAM – Usuario Airbyte

| Campo | Valor |
|-------|--------|
| **Nombre del usuario IAM** | `airbyte` |
| **Acceso a la consola AWS** | Desactivado (solo acceso programático con claves) |

### Política de permisos

El usuario `airbyte` tiene asignada una política que permite operaciones S3 sobre el bucket utilizado por Airbyte para escribir los datos de las conexiones.

**Resumen:** Permite todas las acciones S3 (`s3:*`) sobre recursos S3. Se utiliza para que Airbyte Cloud pueda escribir en el bucket `data-engineer-modulo-cuatro-raw` (destinos S3 de las conexiones Patagonia y Riohacha).

**Política (referencia):**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": "*"
    }
  ]
}
```

Las credenciales (Access Key ID y Secret Access Key) generadas para este usuario se configuran en Airbyte Cloud como destino S3, no en este repositorio.

---

## 3. OpenWeather

| Campo | Valor |
|-------|--------|
| **Tipo de cuenta** | OpenWeather One Call |
| **Uso de la API key** | Solo en Airbyte Cloud (conexiones Patagonia y Riohacha) |
| **Obtención de la API key** | Dashboard de OpenWeather → sección API keys |

La API key se configura únicamente en el **Source** de OpenWeather dentro de Airbyte; no se almacena en este repositorio ni en archivos `.env` del proyecto.

---

## 4. Airbyte

| Campo | Valor |
|-------|--------|
| **Plataforma** | Airbyte Cloud ([cloud.airbyte.com](https://cloud.airbyte.com)) |

### Configuración realizada

1. **Source:** OpenWeather (configurado con la API key de OpenWeather).
2. **Destinations:** Dos destinos basados en S3, uno por ubicación:
   - **Patagonia** → escribe en el bucket en la carpeta correspondiente.
   - **Riohacha** → escribe en el bucket en la carpeta correspondiente.
3. **Conexiones:** Dos conexiones (Source OpenWeather → Destination S3):
   - Una para **Patagonia** (carpeta destino en S3: `stream/Patagonia/`).
   - Una para **Riohacha** (carpeta destino en S3: `stream/Riohacha/`).

### Parámetros de las conexiones

| Parámetro | Valor |
|-----------|--------|
| **Frecuencia de sync** | 24 horas |
| **Carpeta destino en S3** | La respectiva para cada conexión (`stream/Patagonia/`, `stream/Riohacha/`) |
| **Formato de destino** | JSON comprimido GZIP |
| **Aplanamiento** | Sin aplanamiento |

---

## 5. Verificación de datos en S3

Se verificó que los datos ingeridos por Airbyte y los subidos por código aparecen correctamente en la consola de S3:

- **Rutas exactas de verificación:**
  - `s3://data-engineer-modulo-cuatro-raw/stream/Patagonia/`
  - `s3://data-engineer-modulo-cuatro-raw/stream/Riohacha/`
  - `s3://data-engineer-modulo-cuatro-raw/historicos/Patagonia/`
  - `s3://data-engineer-modulo-cuatro-raw/historicos/Riohacha/`

La comprobación se realizó revisando la presencia y estructura de los objetos en dichas rutas desde la consola de AWS S3.

---

## 6. Subida de datos desde código (históricos)

Los archivos históricos (Patagonia y Riohacha) se suben al bucket mediante scripts del propio repositorio, no por Airbyte.

### Componentes

| Archivo | Descripción |
|---------|-------------|
| `data/con_s3.py` | Módulo con la lógica de conexión a S3 y función de subida (usa credenciales del `.env` en la raíz del proyecto). |
| `data/upload_historicos_to_s3.py` | Script que sube `Riohacha_11_538415.json` y `Patagonia_-41.json` a `historicos/Riohacha/` y `historicos/Patagonia/` respectivamente. |

### Requisitos

- Variables en `.env` (raíz del proyecto): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
- Dependencias: `pip install -r requirements.txt` (boto3, python-dotenv).

### Ejecución

Desde la raíz del proyecto:

```bash
python data/upload_historicos_to_s3.py
```

Los archivos deben estar en la carpeta `data/` con los nombres `Riohacha_11_538415.json` y `Patagonia_-41.json`.

---

## 7. Diagrama del pipeline

El diagrama de arquitectura del pipeline ETLT se encuentra en la carpeta **`diseño_pipeline/`** (incluye el notebook de documentación y el diagrama en formato Draw.io).

---

*Documento generado como referencia de la configuración de ingesta a S3 y uso de Airbyte para el proyecto data-engineering-proyecto-4.*
