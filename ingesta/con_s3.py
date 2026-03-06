"""
Módulo para conectar con S3 y subir archivos.
Usa credenciales desde .env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# Script está en ingesta/; .env está en la raíz del proyecto (carpeta padre)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
load_dotenv(PROJECT_ROOT / ".env")

BUCKET_NAME = "data-engineer-modulo-cuatro-raw"
S3_PREFIX = "historicos/"
AWS_REGION = "us-east-2"


def get_client():
    """Crea cliente S3 usando credenciales del .env."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        print(
            "ERROR: Faltan AWS_ACCESS_KEY_ID o AWS_SECRET_ACCESS_KEY en el archivo .env",
            file=sys.stderr,
        )
        sys.exit(1)
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_file(local_path: Path, bucket: str, s3_key: str) -> bool:
    """Sube un archivo local a S3. Devuelve True si tuvo éxito."""
    if not local_path.exists():
        print(f"ERROR: No existe el archivo local: {local_path}", file=sys.stderr)
        return False
    client = get_client()
    try:
        client.upload_file(str(local_path), bucket, s3_key)
        print(f"OK: Subido {local_path} -> s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print(f"ERROR al subir a S3: {e}", file=sys.stderr)
        return False
