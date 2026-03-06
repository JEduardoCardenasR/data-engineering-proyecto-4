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
    │   └── capa_gold.py
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

Solo son necesarios los nombres de buckets y la región. **No incluir credenciales AWS** si se usa IAM Role.

```bash
nano .env
```

Contenido (reemplazar por tus nombres de bucket y región):

```env
# Buckets S3
BUCKET_NAME_RAW=tu-bucket-raw
BUCKET_NAME_SILVER=tu-bucket-silver
BUCKET_NAME_GOLD=tu-bucket-gold

# Región (importante para Spark y S3)
S3_REGION=us-east-2
```

Guardar: `Ctrl+O`, Enter, `Ctrl+X`. Verificar: `cat .env`.

---

## 6. Dockerfile

Desde `~/spark-project`:

```bash
nano Dockerfile
```

Contenido (el del proyecto en `spark/Dockerfile`): imagen base `apache/spark:3.5.0`, Python, pip, `pandas`, `pyspark==3.5.0`, `boto3`, JARs S3A (hadoop-aws, aws-java-sdk-bundle), `WORKDIR /app`, `COPY ./app /app`, `CMD ["tail", "-f", "/dev/null"]`.

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

Deben verse `capa_silver.py` y `capa_gold.py`.

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
# ETL Silver (Raw → Silver, Parquet)
docker exec -it spark-master /opt/spark/bin/spark-submit /app/capa_silver.py

# ETL Gold (Silver → Gold, Parquet)
docker exec -it spark-master /opt/spark/bin/spark-submit /app/capa_gold.py
```

Interfaz web de Spark: `http://<IP_PUBLICA>:8080`. Progreso del job: `http://<IP_PUBLICA>:4040` mientras corre un script.

---

## 11. Cómo se cubre la consigna

### 1. Scripts PySpark para preguntas de negocio

- **Capa Silver**: limpieza, aplanado de esquemas (JSON/JSONL) y escritura en Parquet.
- **Capa Gold**: agregaciones, índices de potencial eólico (WPI) y solar (SPI).
- Las preguntas de negocio se responden en el **reporte** del proyecto usando estas tablas.

### 2. Combinación de datos no estructurados y estructurados

- **Origen**: JSON (históricos) y JSONL.GZ (stream) en S3.
- **Transformación**: `safe_flatten_structs` para unificar esquemas anidados en una estructura tabular.
- **Resultado**: análisis integral en el Data Lake con fuentes heterogéneas.

### 3. Optimización en Apache Spark

- **Particionamiento**: Silver por `ingestion_year`, `ingestion_month`, `ingestion_day`; Gold por `ciudad` (y `mes_anio` en patrones). Mejora lecturas y partition pruning.
- **Uso estratégico de caché**: `df.cache()` y `df.count()` para materializar antes de varias acciones (count, show, write), evitando releer y recalcular desde S3.
- **Shuffle**: `spark.sql.shuffle.partitions` (100 en Silver, 20 en Gold) para controlar tareas y evitar demasiados archivos pequeños en los `groupBy`.

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

## 12. Tres puntos clave para la entrega

1. **Caché**: Uso de `.cache()` para evitar lecturas redundantes a S3 y reducir tiempo y coste.
2. **Shuffle**: Ajuste de `spark.sql.shuffle.partitions` para no generar demasiadas tareas vacías y mejorar los `groupBy`.
3. **Parquet**: Formato columnar en Silver y Gold para que las lecturas usen solo las columnas necesarias (partition pruning y mejor compresión).

---

*Documentación generada para el proyecto data-engineering-proyecto-4 (Módulo 4).*
