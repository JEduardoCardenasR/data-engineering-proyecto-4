# Imagen para el producer de Kafka (OpenWeatherMap → topic).
# Se construye y ejecuta junto con el servicio kafka en docker-compose.
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python requests python-dotenv

COPY producer.py .

CMD ["python", "producer.py"]
