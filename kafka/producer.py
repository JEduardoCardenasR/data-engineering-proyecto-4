"""
Producer de Kafka para ingesta de pronósticos (forecast) desde OpenWeatherMap API.

Publica predicciones climáticas en el topic configurado (por defecto weather_forecast).
Esquema distinto al de "current weather" (openweather_topic): cada mensaje es una
predicción con forecast_time, extraction_at, clouds, weather_desc, etc. El pipeline
(Silver/Gold o un consumer nuevo) debe tratar este esquema aparte del de tiempo real actual.
"""
import os
import sys
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Cargar .env desde el directorio del script (kafka/)
_script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_script_dir, ".env"))

# -----------------------------------------------------------------------------
# Configuración desde variables de entorno
# -----------------------------------------------------------------------------
KAFKA_IP = os.getenv("KAFKA_IP", "").strip()
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092").strip()
BROKER = f"{KAFKA_IP}:{KAFKA_PORT}" if KAFKA_IP else ""
TOPIC = os.getenv("KAFKA_TOPIC", "weather_forecast")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()

# Lista de ciudades: separadas por punto y coma. Ej: "Patagonia,AR;Riohacha,CO"
_cities_raw = os.getenv("OPENWEATHER_CITIES", "Patagonia,AR;Riohacha,CO").strip()
CITIES = [c.strip() for c in _cities_raw.split(";") if c.strip()]

POLL_SECS = int(os.getenv("POLL_SECS", "600"))  # 10 min recomendado para forecast
MAX_RUNS = int(os.getenv("MAX_RUNS", "0"))  # 0 = sin límite (ráfagas)
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in {"1", "true", "yes"}


def _validate_config():
    """Valida API key y broker al inicio."""
    if not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY in ("CLAVE_DE_EJEMPLO_O_VACÍA", ""):
        print("ERROR: OPENWEATHER_API_KEY no configurada en .env.", file=sys.stderr)
        sys.exit(1)
    if not KAFKA_IP:
        print("ERROR: KAFKA_IP debe estar definida en .env (IP privada de la instancia Kafka).", file=sys.stderr)
        sys.exit(1)
    if not CITIES:
        print("ERROR: OPENWEATHER_CITIES debe tener al menos una ciudad (ej: Patagonia,AR;Riohacha,CO).", file=sys.stderr)
        sys.exit(1)


def build_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
    )


def fetch_forecast(city: str) -> list[dict]:
    """Obtiene pronóstico (40 intervalos) y devuelve lista de registros."""
    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?q={city}&appid={OPENWEATHER_API_KEY}&units=metric&lang=es"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    city_name = data["city"]["name"]
    lat = data["city"]["coord"]["lat"]
    lon = data["city"]["coord"]["lon"]
    extraction_at = int(time.time() * 1000)

    records = []
    for entry in data.get("list", []):
        records.append({
            "source": "openweather_forecast",
            "city": city_name,
            "lat": lat,
            "lon": lon,
            "extraction_at": extraction_at,
            "forecast_time": entry.get("dt_txt"),
            "temp_c": entry["main"]["temp"],
            "humidity": entry["main"]["humidity"],
            "wind_speed": entry["wind"]["speed"],
            "clouds": entry["clouds"]["all"],
            "weather_desc": entry["weather"][0]["description"],
        })
    return records


def main():
    _validate_config()

    # --- Bucle de reintento de conexión inicial ---
    producer = None
    print(f"📡 Intentando conectar al broker en {BROKER}...")
    
    while producer is None:
        try:
            producer = build_producer()
            # Si llega aquí, la conexión fue exitosa
        except NoBrokersAvailable:
            print("⏳ Kafka aún no está listo. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"❌ Error inesperado al conectar: {e}")
            time.sleep(5)
    # ----------------------------------------------

    print(f"[producer] ✅ Conectado! broker={BROKER} topic={TOPIC} cities={CITIES} (forecast)")

    burst_count = 0
    while True:
        total_sent = 0
        for city in CITIES:
            try:
                records = fetch_forecast(city)
                for record in records:
                    producer.send(TOPIC, record)
                total_sent += len(records)
                print(f"[producer] Enviadas {len(records)} predicciones para {city}")
            except Exception as e:
                print(f"[producer] ERROR en {city}: {e}", file=sys.stderr)

        producer.flush()
        burst_count += 1
        print(f"[producer] Ráfaga {burst_count}: {total_sent} mensajes enviados.")

        if RUN_ONCE or (MAX_RUNS and burst_count >= MAX_RUNS):
            break
        print(f"[producer] Esperando {POLL_SECS} s para la próxima ráfaga...")
        time.sleep(POLL_SECS)

    print(f"[producer] Terminado tras {burst_count} ráfaga(s).")


if __name__ == "__main__":
    main()
