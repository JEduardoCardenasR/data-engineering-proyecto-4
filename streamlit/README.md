# Consumo Gold - Dashboard Streamlit

Aplicación Streamlit que consume los datos Gold (clima histórico y forecast) desde el mismo bucket S3, con pestañas, filtros por ciudad y mes-año, tablas y gráficos.

## Rutas S3

| Origen   | resumen_clima_diario | patrones_horarios |
|----------|----------------------|--------------------|
| Clima    | `s3://BUCKET_GOLD/gold/resumen_clima_diario/` | `s3://BUCKET_GOLD/gold/patrones_horarios/` |
| Forecast | `s3://BUCKET_GOLD/gold/forecast/resumen_clima_diario/` | `s3://BUCKET_GOLD/gold/forecast/patrones_horarios/` |

Los datos están particionados por **ciudad** y **mes_anio**.

## Requisitos

- Python 3.10+
- Variables de entorno (o archivo `.env`): ver más abajo.

## Instalación

```bash
cd Consumo
pip install -r requirements.txt
```

## Configuración

1. Copia `.env.example` a `.env`:
   ```bash
   cp .env.example .env
   ```
2. Edita `.env` y define:
   - **BUCKET_NAME_GOLD**: nombre del bucket Gold (el mismo que usan `capa_gold.py` y `gold_forecast.py`).
   - **AWS** (solo si la app corre en tu PC o en un entorno sin rol IAM):
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`  
     En EC2 con rol IAM con acceso al bucket, no hace falta configurar estas variables.

## Ejecución local

```bash
streamlit run app.py
```

Se abrirá la app en el navegador (por defecto `http://localhost:8501`).

## Despliegue

- **EC2**: ejecutar `streamlit run app.py` en la instancia; si tiene rol IAM con acceso al bucket Gold, no es necesario configurar keys en la app.
- **Streamlit Cloud**: configurar en la UI las variables/secretos `BUCKET_NAME_GOLD` y, si aplica, `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY`.

## Estructura de la app

- **Pestaña "Clima (Gold histórico)"**: resumen diario y patrones horarios del Gold de clima.
- **Pestaña "Forecast (Gold pronóstico)"**: resumen diario y patrones horarios del Gold de forecast.
- En cada pestaña: filtros por **Ciudad** y **Mes-Año**, tablas (`st.dataframe`) y gráficos (línea para serie diaria, barras para patrones por hora).
