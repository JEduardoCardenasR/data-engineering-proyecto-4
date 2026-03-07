# Configuración y puesta en marcha de Airflow

Documentación paso a paso para crear la instancia de Airflow en AWS, configurarla y conectarla con Spark para orquestar el pipeline ETL (Raw → Silver → Gold).

> **Seguridad:** Este documento no incluye datos sensibles (IPs reales, IDs de cuenta AWS, rutas personales ni contraseñas). Sustituye siempre los placeholders por tus propios valores y **nunca** subas al repositorio archivos `.env`, `.pem` o configuraciones con credenciales reales.

---

## 1. Creación de la instancia EC2 en AWS

### Parámetros de la instancia

| Parámetro | Valor |
|-----------|--------|
| **Nombre** | El que definas (distinto al de Spark) |
| **Sistema operativo** | Ubuntu |
| **AMI** | Ubuntu Server 24.04 |
| **Arquitectura** | 64 bits (x86) |
| **Tipo de instancia** | m7i-flex-large (apto para la capa gratuita) |
| **Par de claves** | El mismo que usas para Spark (inicio de sesión) |

### Configuraciones de red

- **Nombre y descripción**: distintos a los de Spark.
- **Reglas del grupo de seguridad**: crear reglas con los siguientes puertos. Los IDs de regla los asigna AWS en tu cuenta (consulta la consola):

| Puerto | Protocolo | Origen | Descripción |
|--------|-----------|--------|-------------|
| 22 | TCP | 0.0.0.0/0 | SSH |
| 8080 | TCP | 0.0.0.0/0 | Airflow Web UI |
| 5432 | TCP | 0.0.0.0/0 | Conexión DB externa |
| 8793 | TCP | 0.0.0.0/0 | Airflow Worker Log |

### Almacenamiento

- **Tamaño**: 30 GiB.

Luego **Lanzar instancia**.

---

## 2. Rol IAM y acceso a S3

- Asignar a la instancia de Airflow **el mismo rol IAM** que tiene la instancia de Spark, para que Airflow (o los procesos que orqueste) puedan acceder a los buckets de S3 si lo necesitan.

---

## 3. Instalación de Docker y Docker Compose

- Instalar **Docker** y **Docker Compose** en la instancia de Airflow de la misma forma que en la instancia de Spark (según la documentación o guía del proyecto).

---

## 4. Estructura de carpetas

En la instancia EC2 (por ejemplo en `~/airflow`):

```
airflow/
├── dags/
│   └── spark_etl_dag.py
├── logs/
├── config/
├── plugins/
├── .env
└── docker-compose.yaml
```

Crear las carpetas si no existen:

```bash
mkdir -p ~/airflow/{dags,logs,config,plugins}
```

---

## 5. Archivo `.env`

Crear el archivo de variables de entorno:

```bash
cat > ~/airflow/.env << 'EOF'
AIRFLOW_UID=1000
_AIRFLOW_WWW_USER_USERNAME=tu_usuario_airflow
_AIRFLOW_WWW_USER_PASSWORD=tu_contraseña_segura
EOF
```

### Explicación de las variables

| Variable | Descripción |
|----------|-------------|
| **AIRFLOW_UID** | Valor numérico del usuario en Ubuntu. Obtenerlo en la EC2 con: `echo $(id -u)`. En Ubuntu el primer usuario (p. ej. `ubuntu`) suele tener UID `1000`. Si no coincide, los archivos en `logs/`, `dags/`, etc. pueden quedar con permisos incorrectos. |
| **_AIRFLOW_WWW_USER_USERNAME** | Usuario con el que accederás a la interfaz web de Airflow. |
| **_AIRFLOW_WWW_USER_PASSWORD** | Contraseña para la interfaz web. **No uses valores por defecto en producción**; define una contraseña segura. |

---

## 6. Configuración SSH para conectarte desde Cursor

En tu máquina local, editar el archivo de configuración SSH (por ejemplo `~/.ssh/config` en Linux/Mac o la ruta equivalente en Windows) y añadir:

```
# Conexión Airflow (Orquestador)
Host airflow-aws
    HostName IP_PUBLICA_DE_TU_INSTANCIA_AIRFLOW
    User ubuntu
    IdentityFile "RUTA_ABSOLUTA_A_TU_ARCHIVO.pem"
```

Sustituir `IP_PUBLICA_DE_TU_INSTANCIA_AIRFLOW` por la IP pública de tu instancia de Airflow y `RUTA_ABSOLUTA_A_TU_ARCHIVO.pem` por la ruta real de tu par de claves (.pem). Luego podrás conectarte con: `ssh airflow-aws`.

---

## 7. Docker Compose y arranque de Airflow

### Uso de LocalExecutor

Esta configuración usa **LocalExecutor**: el Scheduler ejecuta las tareas en la misma máquina, sin workers externos. Adecuado para la simplicidad del proyecto.

### Inicialización de la base de datos (solo la primera vez)

```bash
cd ~/airflow
docker-compose up airflow-init
```

Cuando termine sin errores, continuar.

### Levantar Airflow

```bash
docker-compose up -d
```

Comprobar que los contenedores estén en ejecución:

```bash
docker-compose ps
```

Deberías ver al menos: `postgres`, `airflow-webserver`, `airflow-scheduler`.

---

## 8. Conectar Airflow con Spark (SSH)

Para que Airflow ejecute `spark-submit` en la instancia de Spark por SSH.

### 8.1 Generar llave SSH en la instancia de Airflow

En la instancia de Airflow (por SSH):

```bash
ssh-keygen -t rsa -b 4096 -C "airflow-to-spark"
```

Dar Enter en las preguntas (ruta por defecto, passphrase vacía) hasta que termine.

### 8.2 Copiar la llave pública

En la instancia de Airflow:

```bash
cat ~/.ssh/id_rsa.pub
```

Copiar **toda** la línea (desde `ssh-rsa` hasta el comentario, p. ej. `ubuntu@ip-xxx-xx-x-xxx`), **sin espacios extra ni saltos de línea** (una sola línea continua).

### 8.3 Autorizar la llave en la instancia de Spark

En **otra terminal**, conéctate a la instancia de Spark por SSH. Luego:

```bash
nano ~/.ssh/authorized_keys
```

- **No borres** las llaves que ya existan.
- Al final del archivo, deja un espacio y pega la nueva llave en una sola línea.
- Guardar y salir: `Ctrl+O`, Enter, `Ctrl+X`.

### 8.4 Verificación desde Airflow

En la terminal de la instancia de Airflow:

```bash
ssh ubuntu@IP_PRIVADA_DE_SPARK
```

Si pide confirmación de fingerprint, escribe `yes`. Si todo está bien, entrarás sin contraseña. Salir con `exit`.

### 8.5 Montar `~/.ssh` en el contenedor de Airflow

En el `docker-compose.yaml` de Airflow, en la sección de volúmenes de los servicios que usan la imagen común (x-airflow-common), debe estar:

```yaml
volumes:
  - ~/.ssh:/opt/airflow/.ssh:ro
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  # ... resto de volúmenes
```

Reiniciar los contenedores:

```bash
docker-compose down
docker-compose up -d
```

### 8.6 Crear la conexión SSH en la UI de Airflow

1. Abrir en el navegador: `http://IP_PUBLICA_AIRFLOW:8080`.
2. Ir a **Admin → Connections**.
3. **Add a new record** y configurar:

| Campo | Valor |
|-------|--------|
| **Connection Id** | `ssh_spark_server` |
| **Connection Type** | SSH |
| **Description** | Conexión SSH a la instancia de Spark para ejecutar procesos de la Capa Silver y Gold |
| **Host** | IP privada de tu instancia Spark (ej. `172.31.x.x` o `10.x.x.x`) |
| **Username** | ubuntu |
| **Password** | (dejar vacío si usas llave) |
| **Port** | 22 |
| **Extra** | `{"key_file": "/opt/airflow/.ssh/id_rsa", "no_host_key_check": true}` |

Guardar (**Save**).

### 8.7 Habilitar “Test” de conexión (si no aparece el botón)

En `docker-compose.yaml`, en la sección `environment` de `x-airflow-common`, añadir:

```yaml
AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'
```

Luego `docker-compose down` y `docker-compose up -d`. En la lista de conexiones, editar `ssh_spark_server` y usar el botón **Test**. Si el resultado es verde, Airflow puede ejecutar órdenes en Spark por SSH.

---

## 9. DAG (spark_etl_pipeline)

### Puntos importantes

- **Ruta del proyecto en la EC2**: En el DAG se usa la variable `SPARK_PROJECT_PATH` (p. ej. `/home/ubuntu/spark-project`). Debe coincidir con la ruta real del proyecto en la instancia de Spark.
- **Schedule**: Dejar `schedule_interval=None` para pruebas manuales; después se puede cambiar a un intervalo (p. ej. cada 12 h).
- **spark-submit**: El DAG usa `spark-submit` dentro del contenedor `spark-master`.
- **Flag `-T`**: Se usa `docker-compose exec -T` para evitar problemas de TTY en ejecución no interactiva.

### Desplegar el DAG

- Copiar `spark_etl_dag.py` en la carpeta `dags/` **dentro de la instancia** (en `~/airflow/dags/`).
- Reiniciar o asegurarse de que los contenedores estén levantados: `docker-compose up -d`.

El DAG hace:

1. **check_spark_connection**: Comprueba que `spark-master` y `spark-worker` estén arriba.
2. **run_capa_silver_etl**: Ejecuta `spark-submit /app/capa_silver.py` (Raw → Silver).
3. **run_capa_gold_etl**: Ejecuta `spark-submit /app/capa_gold.py` (Silver → Gold).

---

## 10. Ejecutar el DAG en Airflow

### Preparar las instancias

- **Airflow**: contenedores en marcha en AWS (`docker-compose up -d` en la instancia de Airflow).
- **Spark**: contenedores en marcha en AWS (`docker-compose up -d` en la instancia de Spark).

### En la interfaz de Airflow

1. Abrir `http://IP_AIRFLOW:8080`.
2. Activar el DAG: poner el **toggle** junto al nombre del DAG en **ON**.
3. Ejecutar una vez: botón **Trigger DAG** (icono de play a la derecha).

### Qué ocurre al ejecutar

1. El **Scheduler** detecta el trigger.
2. **Tarea 1**: Airflow abre una conexión SSH a la IP de Spark y comprueba que `spark-master` y `spark-worker` estén activos. Si responden, la tarea pasa a éxito (verde).
3. **Tarea 2**: Por el mismo túnel SSH, Airflow ejecuta `spark-submit /app/capa_silver.py` en el contenedor `spark-master`. Spark procesa y escribe en la capa Silver.
4. **Tarea 3**: Igual para la capa Gold con `spark-submit /app/capa_gold.py`.

---

## 11. Entender el estado de las tareas (Recent Tasks)

En la vista del DAG, la columna **Recent Tasks** usa un código de colores:

| Color / Estado | Significado |
|----------------|-------------|
| **Verde oscuro (Success)** | Tarea terminada correctamente. |
| **Verde claro (Running)** | Tarea ejecutándose. |
| **Rojo (Failed)** | Error (código, conexión, etc.). |
| **Amarillo (Up for Retry)** | Falló y Airflow la reintentará según `retries`. |
| **Gris claro (Queued)** | En cola, esperando ejecución. |
| **Gris oscuro (Scheduled)** | Programada pero aún no en cola. |

El DAG ha terminado cuando todos los estados van al **verde oscuro (Success)** y la columna **Last Run** muestra un círculo verde sólido.

---

## 12. Ver el progreso (logs y Graph)

1. En la UI, hacer clic en el **nombre del DAG** (`spark_etl_pipeline`).
2. Ir a la pestaña **Graph**: se ven las tres tareas y sus dependencias.
3. Clic en una tarea (p. ej. `run_capa_silver_etl`) y elegir **Logs**. Ahí se ve la salida de Spark y cualquier `print()` de tu código.

Mientras una tarea está en verde claro (Running), también puedes abrir en otra pestaña la **Spark Master UI**: `http://IP_PUBLICA_SPARK:8080`, y en “Running Applications” ver el job de la capa Silver/Gold. Para historial de aplicaciones, usar el Spark History Server (puerto 18080 si está configurado).

---

## 13. Verificación de datos

- Revisar en **S3** que se generen los archivos esperados en las rutas de Silver y Gold.
- Descargar o inspeccionar muestras para comprobar que Silver esté limpio y que Gold tenga la agregación o formato deseado.

---

## 14. Programar el DAG (schedule_interval)

Para pruebas, se deja `schedule_interval=None` (solo ejecución manual). Para ejecución automática, editar el DAG y definir un intervalo, por ejemplo:

```python
schedule_interval='0 */12 * * *',  # cada 12 horas (cron)
# o
schedule_interval=timedelta(hours=12),
```

Guardar el archivo en `dags/`; el Scheduler cargará los cambios. Ajustar el cron o el `timedelta` según necesidad.

---

## 15. Servicios y puertos

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| Airflow Web UI | 8080 | Interfaz web |
| PostgreSQL | 5432 (interno) | Metadatos de Airflow |

---

## 16. Comandos útiles

```bash
# Ver logs del webserver
docker-compose logs -f airflow-webserver

# Ver logs del scheduler
docker-compose logs -f airflow-scheduler

# Reiniciar servicios
docker-compose restart

# Detener servicios
docker-compose down

# Detener y eliminar volúmenes (cuidado: borra BD de Airflow)
docker-compose down -v
```

---

## Resumen rápido

1. Crear EC2 (Ubuntu 24.04, m7i-flex-large, 30 GiB, grupo de seguridad con 22, 8080, 5432, 8793).
2. Asignar el mismo rol IAM que Spark (acceso S3).
3. Instalar Docker y Docker Compose.
4. Crear estructura `airflow/` con `dags/`, `logs/`, `config/`, `plugins/`, `.env`, `docker-compose.yaml`.
5. Configurar `.env` (AIRFLOW_UID, usuario y contraseña web).
6. Configurar SSH en tu máquina para acceder a la instancia Airflow (y opcionalmente a Spark).
7. Ejecutar `docker-compose up airflow-init` y luego `docker-compose up -d`.
8. Generar llave SSH en Airflow, copiarla a `authorized_keys` de Spark, montar `~/.ssh` en el compose y crear la conexión `ssh_spark_server` en Admin → Connections.
9. Copiar el DAG a `dags/`, activar el DAG en la UI y hacer Trigger.
10. Revisar estados en Recent Tasks, logs en cada tarea y datos en S3; después ajustar `schedule_interval` si quieres ejecución automática.
