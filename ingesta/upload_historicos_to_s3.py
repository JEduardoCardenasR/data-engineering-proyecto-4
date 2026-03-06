"""
Sube los archivos históricos (Riohacha y Patagonia) al bucket S3.
Riohacha → historicos/Riohacha/, Patagonia → historicos/Patagonia/.
Usa el módulo con_s3 para la conexión y subida.
"""

from pathlib import Path

from con_s3 import BUCKET_NAME, S3_PREFIX, upload_file

SCRIPT_DIR = Path(__file__).resolve().parent
# Los JSON históricos están en data/ (raíz del proyecto)
DATA_DIR = SCRIPT_DIR.parent / "data"

# Archivos a subir: (archivo_local, clave_en_s3)
ARCHIVOS = [
    (DATA_DIR / "Riohacha_11_538415.json", f"{S3_PREFIX}Riohacha/Riohacha_11_538415.json"),
    (DATA_DIR / "Patagonia_-41.json", f"{S3_PREFIX}Patagonia/Patagonia_-41.json"),
]


def main():
    for local_path, s3_key in ARCHIVOS:
        upload_file(local_path, BUCKET_NAME, s3_key)


if __name__ == "__main__":
    main()
