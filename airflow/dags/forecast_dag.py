"""
DAG Forecast: Kafka (producer una vez) → Consumer Spark (timeout 5 min) → Silver → Gold.

Flujo: trigger_producer >> run_consumer_timeout >> run_silver_forecast >> run_gold_forecast.
Opcional: check_spark_connection al inicio.
Requiere conexiones SSH: ssh_spark_server (Spark) y ssh_kafka_server (Kafka).
Variables de entorno: SPARK_PROJECT_PATH, KAFKA_PROJECT_PATH; opcional AIRFLOW_SSH_CONN_ID, AIRFLOW_SSH_KAFKA_CONN_ID.
"""
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import os

# --- CONFIGURACIÓN (desde variables de entorno; ver .env) ---
SSH_CONN_ID = os.environ.get("AIRFLOW_SSH_CONN_ID", "ssh_spark_server")
SSH_KAFKA_CONN_ID = os.environ.get("AIRFLOW_SSH_KAFKA_CONN_ID", "ssh_kafka_server")
SPARK_PROJECT_PATH = os.environ.get("SPARK_PROJECT_PATH", "/home/ubuntu/spark-project")
KAFKA_PROJECT_PATH = os.environ.get("KAFKA_PROJECT_PATH", "/home/ubuntu/kafka")

# Timeout del consumer: 300 s (5 min); trigger del consumer cada 60 s, así se escriben varios batches en RAW
CONSUMER_TIMEOUT_SECONDS = 300

# JARs del conector Kafka (incluidos en la imagen Spark por el Dockerfile; --jars asegura que se carguen)
SPARK_KAFKA_JARS = (
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
    "/opt/spark/jars/kafka-clients-3.4.1.jar,"
    "/opt/spark/jars/commons-pool2-2.11.1.jar"
)


def notificar_error(context):
    """Se ejecuta si una tarea falla (tras agotar reintentos)."""
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    exec_date = context.get("task_instance").execution_date
    log_url = context.get("task_instance").log_url
    print("--- 🚨 ALERTA DE FALLO EN EL PIPELINE ---")
    print(f"DAG: {dag_id}")
    print(f"TAREA FALLIDA: {task_id}")
    print(f"FECHA: {exec_date}")
    print(f"REVISAR LOGS AQUÍ: {log_url}")
    print("-----------------------------------------")


default_args = {
    "owner": "eduardo-data-eng",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notificar_error,
}

with DAG(
    "forecast_pipeline",
    description="Pipeline Forecast: Kafka producer (RUN_ONCE) → Consumer (timeout) → Silver → Gold",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["forecast", "kafka", "gold", "proyecto-4"],
) as dag:

    check_spark_connection = SSHOperator(
        task_id="check_spark_connection",
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose ps | grep -E "spark-master|spark-worker"',
        cmd_timeout=60,
    )

    trigger_producer = SSHOperator(
        task_id="trigger_producer",
        ssh_conn_id=SSH_KAFKA_CONN_ID,
        command=f'cd {KAFKA_PROJECT_PATH} && docker-compose run --rm -e RUN_ONCE=true producer',
        cmd_timeout=180,
    )

    # Exit 124 = proceso terminado por timeout (intencional); lo tratamos como éxito para que el pipeline continúe.
    _consumer_cmd = (
        f'cd {SPARK_PROJECT_PATH} && timeout {CONSUMER_TIMEOUT_SECONDS} '
        'docker-compose exec -T spark-master /opt/spark/bin/spark-submit '
        f'--jars {SPARK_KAFKA_JARS} /app/consumer_forecast.py ; '
        'ret=$? ; if [ "$ret" -eq 124 ]; then exit 0; else exit $ret; fi'
    )
    run_consumer_timeout = SSHOperator(
        task_id="run_consumer_timeout",
        ssh_conn_id=SSH_CONN_ID,
        command=_consumer_cmd,
        cmd_timeout=CONSUMER_TIMEOUT_SECONDS + 120,
    )

    run_silver_forecast = SSHOperator(
        task_id="run_silver_forecast",
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose exec -T spark-master /opt/spark/bin/spark-submit /app/silver_forecast.py',
        cmd_timeout=1800,
    )

    run_gold_forecast = SSHOperator(
        task_id="run_gold_forecast",
        ssh_conn_id=SSH_CONN_ID,
        command=f'cd {SPARK_PROJECT_PATH} && docker-compose exec -T spark-master /opt/spark/bin/spark-submit /app/gold_forecast.py',
        cmd_timeout=1800,
    )

    check_spark_connection >> trigger_producer >> run_consumer_timeout >> run_silver_forecast >> run_gold_forecast
