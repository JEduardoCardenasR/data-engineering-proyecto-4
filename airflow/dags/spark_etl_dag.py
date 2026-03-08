from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests

# --- CONFIGURACIÓN (desde variables de entorno; ver .env) ---
SSH_CONN_ID = os.environ.get("AIRFLOW_SSH_CONN_ID", "ssh_spark_server")
SPARK_PROJECT_PATH = os.environ.get("SPARK_PROJECT_PATH", "/home/ubuntu/spark-project")

# Credenciales e IDs de Airbyte (obligatorios; se cargan desde .env en el contenedor)
CLIENT_ID = os.environ.get("AIRBYTE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AIRBYTE_CLIENT_SECRET")
ID_PATAGONIA = os.environ.get("AIRBYTE_CONNECTION_ID_PATAGONIA")
ID_RIOHACHA = os.environ.get("AIRBYTE_CONNECTION_ID_RIOHACHA")
# ---------------------

def run_airbyte_sync(connection_id, city_name):
    """
    Función para disparar la sincronización vía API con OAuth2.
    Se utiliza el flujo de Client Credentials para obtener un token temporal.
    Requiere AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET y el connection_id en .env.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError(
            "Faltan AIRBYTE_CLIENT_ID o AIRBYTE_CLIENT_SECRET en el archivo .env. "
            "Copia las variables desde la documentación o desde .env.example."
        )
    if not connection_id:
        raise ValueError(f"Falta el connection_id de Airbyte para {city_name} en .env.")
    print(f"--- Iniciando proceso para {city_name} ---")
    
    # PASO 1: Obtener el Access Token (OAuth2)
    # Airbyte Cloud requiere que los datos de auth se envíen como form-data
    auth_url = "https://api.airbyte.com/v1/applications/token"
    auth_payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    
    print(f"Solicitando token de acceso a Airbyte Cloud...")
    auth_res = requests.post(auth_url, json=auth_payload)
    
    if auth_res.status_code != 200:
        error_msg = f"Error de autenticación ({auth_res.status_code}): {auth_res.text}"
        print(error_msg)
        raise Exception(error_msg)
    
    token = auth_res.json().get("access_token")
    print("Token obtenido exitosamente.")

    # PASO 2: Disparar el Job de Sincronización
    sync_url = "https://api.airbyte.com/v1/jobs"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    sync_payload = {
        "connectionId": connection_id,
        "jobType": "sync"
    }
    
    print(f"Enviando señal de sincronización para connectionId: {connection_id}...")
    sync_res = requests.post(sync_url, json=sync_payload, headers=headers)
    
    if sync_res.status_code not in [200, 201]:
        error_msg = f"Error al disparar sync ({sync_res.status_code}): {sync_res.text}"
        print(error_msg)
        raise Exception(error_msg)
    
    job_data = sync_res.json()
    job_id = job_data.get("jobId")
    print(f"¡LOGRADO! Job disparado para {city_name}. Job ID: {job_id}")
    print(f"Estado inicial: {job_data.get('status')}")

    # Pausa de seguridad: Esperamos 60 segundos para que Airbyte termine de escribir en S3
    print("Esperando 60 segundos para asegurar la persistencia de datos en S3...")
    time.sleep(60) 
    print(f"Pausa finalizada. Procediendo con el pipeline para {city_name}.")

def notificar_error(context):
    """Función que se ejecuta automáticamente si una tarea falla"""
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    exec_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    
    # Generaremos un log de ALERTA CRÍTICA:
    print(f"--- 🚨 ALERTA DE FALLO EN EL PIPELINE ---")
    print(f"DAG: {dag_id}")
    print(f"TAREA FALLIDA: {task_id}")
    print(f"FECHA: {exec_date}")
    print(f"REVISAR LOGS AQUÍ: {log_url}")
    print(f"-----------------------------------------")

default_args = {
    'owner': 'eduardo-data-eng',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 3,                 # <--- INTENTOS: Si falla, reintenta 3 veces
    'retry_delay': timedelta(minutes=2), # <--- ESPERA: Espera 2 min entre reintentos
    'on_failure_callback': notificar_error, # <--- CALLBACK: Si falla, ejecuta notificar_error
}

with DAG(
    'spark_etl_pipeline',
    description='Pipeline ETL: Airbyte (Python OAuth) -> Spark (SSH)',
    start_date=datetime(2026, 3, 1),
    schedule_interval=None, 
    catchup=False,
    default_args=default_args,
    tags=['etl', 'spark', 'airbyte', 'proyecto-4'],
) as dag:

    # --- FASE 1: INGESTA (AIRBYTE vía PythonOperator) ---
    
    trigger_airbyte_patagonia = PythonOperator(
        task_id='trigger_airbyte_patagonia',
        python_callable=run_airbyte_sync,
        op_kwargs={'connection_id': ID_PATAGONIA, 'city_name': 'Patagonia'}
    )

    trigger_airbyte_riohacha = PythonOperator(
        task_id='trigger_airbyte_riohacha',
        python_callable=run_airbyte_sync,
        op_kwargs={'connection_id': ID_RIOHACHA, 'city_name': 'Riohacha'}
    )

    # --- FASE 2: VERIFICACIÓN Y PROCESAMIENTO (SPARK vía SSH) ---

    check_spark_connection = SSHOperator(
        task_id='check_spark_connection',
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose ps | grep -E "spark-master|spark-worker"',
        cmd_timeout=60,
    )

    run_capa_silver = SSHOperator(
        task_id='run_capa_silver_etl',
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose exec -T spark-master /opt/spark/bin/spark-submit /app/capa_silver.py',
        cmd_timeout=1800,
    )

    run_capa_gold = SSHOperator(
        task_id='run_capa_gold_etl',
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose exec -T spark-master /opt/spark/bin/spark-submit /app/capa_gold.py',
        cmd_timeout=1800,
    )

    # --- DEFINICIÓN DEL FLUJO ---
    [trigger_airbyte_patagonia, trigger_airbyte_riohacha] >> check_spark_connection >> run_capa_silver >> run_capa_gold