# GitHub Actions — Documentación

Este documento describe la configuración de **GitHub Actions** en el repositorio `data-engineering-proyecto-4`. Los workflows solo realizan **verificación y validación**; no ejecutan pipelines en producción ni modifican servicios desplegados.

---

## Estructura

```
data-engineering-proyecto-4/
├── .github/
│   └── workflows/
│       ├── ci.yml              # Lint y sintaxis Python
│       ├── validate-dags.yml   # Validación de DAGs de Airflow
│       └── docker-build.yml   # Build de imágenes Docker (Spark y Kafka)
└── docs/
    ├── GITHUB_ACTIONS.md    # Este archivo
    └── README.md            # Índice de toda la documentación del proyecto
```

Para el índice completo de documentación (Airflow, Spark, Kafka, ingesta, Streamlit), véase [README.md](README.md) en esta carpeta o el [README principal del proyecto](../README.md).

---

## Cuándo se ejecutan

Todos los workflows se disparan en:

- **Push** a las ramas `main` o `master`
- **Pull request** hacia `main` o `master`

No se ejecutan en otras ramas a menos que se modifiquen los archivos en `.github/workflows/` o que se configure un trigger adicional.

---

## Workflows

### 1. CI (`ci.yml`)

**Nombre en GitHub:** CI

**Objetivo:** Revisar que el código Python sea válido y no tenga errores de sintaxis o de imports/undefined.

**Pasos:**

| Paso | Descripción |
|------|-------------|
| Checkout | Obtiene el código del repositorio. |
| Set up Python | Configura Python 3.11. |
| Install dependencies | Crea un venv e instala `requirements.txt`, `streamlit/requirements.txt` y **Ruff**. |
| Lint with Ruff | Ejecuta `ruff check` sobre `streamlit/`, `spark/app/`, `kafka/` e `ingesta/` con **solo reglas E y F** (errores de estilo tipo pycodestyle y problemas tipo pyflakes: sintaxis, imports, variables no definidas). No aplica reglas de formato/estilo estéticas. |
| Check Python syntax | Ejecuta `python -m py_compile` sobre los `.py` principales para comprobar sintaxis. |

**Rutas revisadas:**

- `streamlit/app.py`
- `kafka/producer.py`
- `ingesta/upload_historicos_to_s3.py`, `ingesta/con_s3.py`
- Todos los `.py` en `spark/app/`

**Nota:** Los DAGs de Airflow (`airflow/dags/`) no se incluyen aquí porque requieren Airflow para importar; se validan en el workflow **Validate Airflow DAGs**.

---

### 2. Validate Airflow DAGs (`validate-dags.yml`)

**Nombre en GitHub:** Validate Airflow DAGs

**Objetivo:** Comprobar que los DAGs en `airflow/dags/` se carguen sin errores de sintaxis o de configuración. **No ejecuta tareas ni se conecta a Spark, Kafka ni a ningún servicio.**

**Pasos:**

| Paso | Descripción |
|------|-------------|
| Checkout | Obtiene el código del repositorio. |
| Set up Python | Configura Python 3.11. |
| Install Airflow | Instala `apache-airflow==2.7.3` y `apache-airflow-providers-ssh` usando el archivo de constraints oficial para versiones compatibles. |
| Validate DAGs | Carga la carpeta `airflow/dags` con `DagBag`, comprueba que no haya `import_errors` y lista los `dag_ids` cargados. Si hay errores, el job falla. |

**Variables de entorno usadas:**

- `AIRFLOW_HOME=/tmp/airflow_validate` — Evita usar `~/airflow` y no afecta instalaciones locales.

**Actualizar versión de Airflow:** Editar la variable `AIRFLOW_VERSION` en el workflow (y, si se cambia Python, `PYTHON_VERSION`) y asegurarse de que exista el archivo de constraints para esa versión en el repositorio de Airflow.

---

### 3. Docker Build (`docker-build.yml`)

**Nombre en GitHub:** Docker Build

**Objetivo:** Verificar que las imágenes Docker del proyecto se construyan correctamente. **No hace push a ningún registro ni despliegue.**

**Jobs (se ejecutan en paralelo):**

| Job | Contexto | Dockerfile | Descripción |
|-----|----------|------------|-------------|
| build-spark | `./spark` | `./spark/Dockerfile` | Imagen con Spark y dependencias para el pipeline. |
| build-kafka-producer | `./kafka` | `./kafka/Dockerfile` | Imagen del producer de Kafka (OpenWeatherMap → topic). |

**Detalles:**

- Se usa **Docker Buildx** y la acción `docker/build-push-action` con `push: false`.
- Las imágenes se etiquetan como `spark-local:test` y `kafka-producer:test` solo para el build; no se suben.
- Se usa caché de GitHub Actions (`cache-from` / `cache-to`) para acelerar builds posteriores.

---

## Cómo ver los resultados

1. En GitHub, abre el repositorio **data-engineering-proyecto-4**.
2. Ve a la pestaña **Actions**.
3. Verás una lista de *workflow runs*. Haz clic en uno para ver el detalle de cada job y cada paso (logs, fallos, etc.).

En cada *commit* o PR verás los checks asociados (CI, Validate Airflow DAGs, Docker Build) en la página del commit o del Pull Request.

---

## Ejecución local (opcional)

Para reproducir de forma aproximada lo que hace cada workflow:

**CI (Ruff + sintaxis):**

```bash
cd data-engineering-proyecto-4
python -m venv .venv
# En Windows: .venv\Scripts\activate
# En Linux/macOS: . .venv/bin/activate
pip install -r requirements.txt -r streamlit/requirements.txt ruff
ruff check streamlit/ spark/app/ kafka/ ingesta/ --select E,F
python -m py_compile streamlit/app.py kafka/producer.py ingesta/upload_historicos_to_s3.py ingesta/con_s3.py
# y los .py en spark/app/
```

**Validación de DAGs:**

```bash
pip install apache-airflow apache-airflow-providers-ssh
export AIRFLOW_HOME=/tmp/airflow_validate
python -c "
from airflow.models import DagBag
import os
bag = DagBag(dags_folder=os.path.join(os.getcwd(), 'airflow', 'dags'), include_examples=False)
if bag.import_errors:
    print(bag.import_errors)
    exit(1)
print('DAGs:', list(bag.dag_ids))
"
```

**Docker:**

```bash
docker build -f spark/Dockerfile ./spark
docker build -f kafka/Dockerfile ./kafka
```

---

## Fallos frecuentes y qué revisar

| Workflow | Si falla… | Revisar |
|----------|-----------|--------|
| CI | Paso "Lint with Ruff" | Errores E/F en los `.py` de `streamlit/`, `spark/app/`, `kafka/`, `ingesta/` (sintaxis, imports, variables no definidas). |
| CI | Paso "Check Python syntax" | Que los archivos listados existan y que no tengan errores de sintaxis. |
| Validate Airflow DAGs | "Install Airflow" | Que la versión en constraints exista para la versión de Python usada; en ese caso probar otra `AIRFLOW_VERSION` o `PYTHON_VERSION`. |
| Validate Airflow DAGs | "Validate DAGs" | Mensaje de error impreso: suele indicar el archivo del DAG y el error (import, sintaxis, configuración). Corregir el DAG y volver a hacer push. |
| Docker Build | Build Spark o Kafka | Logs del paso de build: dependencias, rutas en el Dockerfile, archivos que se copian (por ejemplo `producer.py` en Kafka). |

---

## Qué no hacen estos workflows

- No ejecutan el pipeline de Spark ni tareas de Airflow en ningún entorno.
- No se conectan a Kafka, S3, Airbyte ni a bases de datos.
- No usan secretos (AWS, API keys, etc.) ni despliegan nada.
- No modifican código ni configuraciones existentes; solo leen el repositorio y ejecutan lint, validación de DAGs y build de imágenes.

Para añadir despliegue automático (por ejemplo a EC2 o a un registro de imágenes), habría que crear **nuevos** workflows o pasos que usen secretos y que se disparen de forma explícita (por ejemplo con `workflow_dispatch` o en pushes a una rama de release).
