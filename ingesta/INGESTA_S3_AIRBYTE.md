# Documentación: Ingesta a S3 mediante Airbyte

**Fecha:** 2 de marzo de 2026  
**Proyecto:** data-engineering-proyecto-4  
**Documento de referencia (solo lectura).**

Este documento describe únicamente la **ingesta de datos a S3 a través de Airbyte** (datos en streaming desde OpenWeather). Para la subida de archivos JSON locales (históricos) a S3, véase **[INGESTA_JSON_LOCAL.md](INGESTA_JSON_LOCAL.md)**.

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

*Los datos en `historicos/` se cargan desde archivos JSON locales; véase [INGESTA_JSON_LOCAL.md](INGESTA_JSON_LOCAL.md).*

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

## 5. Verificación de datos en S3 (Airbyte)

Se verificó que los datos ingeridos por Airbyte aparecen correctamente en la consola de S3 en las rutas de streaming:

- **Rutas de verificación (Airbyte):**
  - `s3://data-engineer-modulo-cuatro-raw/stream/Patagonia/`
  - `s3://data-engineer-modulo-cuatro-raw/stream/Riohacha/`

La comprobación se realizó revisando la presencia y estructura de los objetos en dichas rutas desde la consola de AWS S3.

---

## 6. Diagrama del pipeline

El diagrama de arquitectura del pipeline ETLT se encuentra en la carpeta **`diseño_pipeline/`** (incluye el notebook de documentación y el diagrama en formato Draw.io).

---

*Documento de referencia de la ingesta a S3 mediante Airbyte para el proyecto data-engineering-proyecto-4. Para la ingesta desde archivos JSON locales, véase [INGESTA_JSON_LOCAL.md](INGESTA_JSON_LOCAL.md).*
