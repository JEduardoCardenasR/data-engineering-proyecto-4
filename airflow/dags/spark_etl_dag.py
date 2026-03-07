from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# 1. ID de la conexión
SSH_CONN_ID = "ssh_spark_server" 

# 2. Ruta del proyecto en la EC2
SPARK_PROJECT_PATH = "/home/ubuntu/spark-project"
# ---------------------

default_args = {
    'owner': 'eduardo-data-eng',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_etl_pipeline',
    description='Pipeline ETL: Raw → Silver → Gold usando Spark Cluster',
    start_date=datetime(2026, 3, 1), # Ajustado a fecha actual
    schedule_interval=None,          # Lo ponemos manual para tus primeras pruebas posteriormente se cambiara a un intervalo de tiempo
    catchup=False,
    default_args=default_args,
    tags=['etl', 'spark', 'proyecto-4'],
) as dag:

    # Tarea 1: Verificar que el cluster de Spark esté arriba
    check_spark_connection = SSHOperator(
        task_id='check_spark_connection',
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose ps | grep -E "spark-master|spark-worker"',
        cmd_timeout=60,
    )

    # Tarea 2: Ejecutar ETL Capa Silver usando spark-submit
    # Uso de -T en docker compose para evitar errores de terminal (TTY)
    run_capa_silver = SSHOperator(
        task_id='run_capa_silver_etl',
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose exec -T spark-master /opt/spark/bin/spark-submit /app/capa_silver.py',
        cmd_timeout=1800, 
    )

    # Tarea 3: Ejecutar ETL Capa Gold usando spark-submit
    run_capa_gold = SSHOperator(
        task_id='run_capa_gold_etl',
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose exec -T spark-master /opt/spark/bin/spark-submit /app/capa_gold.py',
        cmd_timeout=1800,
    )

    # Definir flujo
    check_spark_connection >> run_capa_silver >> run_capa_gold