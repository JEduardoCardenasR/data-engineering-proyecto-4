"""
Consumo Gold - Streamlit dashboard.
Lee Parquet desde S3 (clima Gold histórico y forecast Gold) y muestra tablas y gráficos.
Variables de entorno: BUCKET_NAME_GOLD; opcionalmente AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
"""
import os
from pathlib import Path

import pandas as pd
import streamlit as st

# Cargar .env si existe (local)
env_path = Path(__file__).resolve().parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)

BUCKET = os.environ.get("BUCKET_NAME_GOLD", "").strip()

# Rutas S3 (mismo bucket, dos conjuntos)
PATH_CLIMA_DAILY = f"s3://{BUCKET}/gold/resumen_clima_diario/" if BUCKET else ""
PATH_CLIMA_PATTERNS = f"s3://{BUCKET}/gold/patrones_horarios/" if BUCKET else ""
PATH_FORECAST_DAILY = f"s3://{BUCKET}/gold/forecast/resumen_clima_diario/" if BUCKET else ""
PATH_FORECAST_PATTERNS = f"s3://{BUCKET}/gold/forecast/patrones_horarios/" if BUCKET else ""


@st.cache_data(ttl=300)
def load_parquet_safe(path: str):
    """Carga Parquet desde S3. Retorna DataFrame o None y mensaje de error. Cache 5 min."""
    if not path or not path.startswith("s3://"):
        return None, "Bucket no configurado (BUCKET_NAME_GOLD)."
    try:
        df = pd.read_parquet(path)
        if df is None or df.empty:
            return None, "No hay datos en esta ruta."
        return df, None
    except Exception as e:
        return None, str(e)


def render_tab(df_daily: pd.DataFrame | None, df_patterns: pd.DataFrame | None, title: str):
    """Renderiza una pestaña: filtros, tablas y gráficos para daily y patterns."""
    if df_daily is None and df_patterns is None:
        st.warning("No hay datos disponibles para esta sección.")
        return

    # Filtros a partir de los datos disponibles
    cities = []
    months = []
    if df_daily is not None and not df_daily.empty:
        cities = sorted(df_daily["ciudad"].dropna().unique().tolist())
        months = sorted(df_daily["mes_anio"].dropna().unique().tolist())
    if df_patterns is not None and not df_patterns.empty:
        for c in df_patterns["ciudad"].dropna().unique():
            if c not in cities:
                cities.append(c)
        cities = sorted(cities)
        for m in df_patterns["mes_anio"].dropna().unique():
            if m not in months:
                months.append(m)
        months = sorted(months)

    col1, col2 = st.columns(2)
    with col1:
        sel_city = st.selectbox("Ciudad", options=["Todas"] + cities, key=f"city_{title}")
    with col2:
        sel_month = st.selectbox("Mes-Año", options=["Todos"] + months, key=f"month_{title}")

    # Aplicar filtros
    def filter_df(df: pd.DataFrame):
        if df is None or df.empty:
            return df
        out = df.copy()
        if sel_city != "Todas":
            out = out[out["ciudad"] == sel_city]
        if sel_month != "Todos":
            out = out[out["mes_anio"] == sel_month]
        return out

    daily_f = filter_df(df_daily) if df_daily is not None else None
    patterns_f = filter_df(df_patterns) if df_patterns is not None else None

    # Resumen diario
    if daily_f is not None and not daily_f.empty:
        st.subheader("Resumen clima diario")
        st.dataframe(daily_f, use_container_width=True)
        if "fecha_solo" in daily_f.columns and daily_f["fecha_solo"].notna().any():
            daily_sorted = daily_f.sort_values("fecha_solo")
            col_metric = "promedio_temperatura_diaria_c"
            if col_metric in daily_sorted.columns:
                st.line_chart(daily_sorted.set_index("fecha_solo")[[col_metric]])
    else:
        st.info("Sin datos de resumen diario para los filtros seleccionados.")

    # Patrones horarios
    if patterns_f is not None and not patterns_f.empty:
        st.subheader("Patrones horarios")
        st.dataframe(patterns_f, use_container_width=True)
        col_metric = "promedio_temperatura_por_hora_mes"
        if "hora_solo" in patterns_f.columns and col_metric in patterns_f.columns:
            by_hour = patterns_f.groupby("hora_solo", as_index=False)[col_metric].mean()
            by_hour = by_hour.sort_values("hora_solo")
            st.bar_chart(by_hour.set_index("hora_solo")[[col_metric]])
    else:
        st.info("Sin datos de patrones horarios para los filtros seleccionados.")


def render_preguntas_negocio(
    df_clima_daily: pd.DataFrame | None,
    df_clima_patt: pd.DataFrame | None,
    df_forecast_daily: pd.DataFrame | None,
    df_forecast_patt: pd.DataFrame | None,
):
    """Renderiza la pestaña Preguntas de negocio: respuestas con datos Gold."""
    # Filtros comunes a partir de clima (histórico)
    cities = []
    months = []
    if df_clima_daily is not None and not df_clima_daily.empty:
        cities = sorted(df_clima_daily["ciudad"].dropna().unique().tolist())
        months = sorted(df_clima_daily["mes_anio"].dropna().unique().tolist())
    if df_clima_patt is not None and not df_clima_patt.empty:
        for c in df_clima_patt["ciudad"].dropna().unique():
            if c not in cities:
                cities.append(c)
        cities = sorted(cities)
        for m in df_clima_patt["mes_anio"].dropna().unique():
            if m not in months:
                months.append(m)
        months = sorted(months)

    col1, col2 = st.columns(2)
    with col1:
        sel_city = st.selectbox("Ciudad", options=["Todas"] + cities, key="preg_city")
    with col2:
        sel_month = st.selectbox("Mes-Año", options=["Todos"] + months, key="preg_month")

    def _filter(df: pd.DataFrame | None):
        if df is None or df.empty:
            return df
        out = df.copy()
        if sel_city != "Todas":
            out = out[out["ciudad"] == sel_city]
        if sel_month != "Todos":
            out = out[out["mes_anio"] == sel_month]
        return out

    d_c = _filter(df_clima_daily) if df_clima_daily is not None else None
    p_c = _filter(df_clima_patt) if df_clima_patt is not None else None
    d_f = _filter(df_forecast_daily) if df_forecast_daily is not None else None
    p_f = _filter(df_forecast_patt) if df_forecast_patt is not None else None

    # --- Pregunta 1: Variación potencial solar día/mes ---
    with st.expander("**1. ¿Cómo varía el potencial solar estimado a lo largo del día y del mes en Riohacha y en la Patagonia?**", expanded=True):
        st.caption("Fuente: Gold clima — resumen diario y patrones horarios (SPI).")
        if p_c is not None and not p_c.empty and "promedio_solar_por_hora_mes" in p_c.columns and "hora_solo" in p_c.columns:
            by_hour = p_c.groupby("hora_solo", as_index=False)["promedio_solar_por_hora_mes"].mean()
            by_hour = by_hour.sort_values("hora_solo")
            st.markdown("**Potencial solar por hora del día (promedio del mes)**")
            st.bar_chart(by_hour.set_index("hora_solo")[["promedio_solar_por_hora_mes"]])
        if d_c is not None and not d_c.empty and "promedio_potencial_solar_diario" in d_c.columns and "fecha_solo" in d_c.columns:
            daily_s = d_c.sort_values("fecha_solo")
            st.markdown("**Potencial solar diario a lo largo del periodo**")
            st.line_chart(daily_s.set_index("fecha_solo")[["promedio_potencial_solar_diario"]])
        if (d_c is None or d_c.empty) and (p_c is None or p_c.empty):
            st.info("Sin datos de clima para los filtros seleccionados.")

    # --- Pregunta 2: Patrones históricos potencial eólico ---
    with st.expander("**2. ¿Qué patrones históricos se observan en el potencial eólico de ambas ubicaciones?**", expanded=True):
        st.caption("Fuente: Gold clima — patrones horarios (WPI).")
        if p_c is not None and not p_c.empty and "promedio_eolico_por_hora_mes" in p_c.columns and "hora_solo" in p_c.columns:
            if "ciudad" in p_c.columns and sel_city == "Todas" and p_c["ciudad"].nunique() > 1:
                by_city_hour = p_c.groupby(["ciudad", "hora_solo"], as_index=False)["promedio_eolico_por_hora_mes"].mean()
                for city in sorted(by_city_hour["ciudad"].unique()):
                    sub = by_city_hour[by_city_hour["ciudad"] == city].sort_values("hora_solo")
                    st.markdown(f"**{city}**")
                    st.line_chart(sub.set_index("hora_solo")[["promedio_eolico_por_hora_mes"]])
            else:
                by_hour = p_c.groupby("hora_solo", as_index=False)["promedio_eolico_por_hora_mes"].mean()
                by_hour = by_hour.sort_values("hora_solo")
                st.bar_chart(by_hour.set_index("hora_solo")[["promedio_eolico_por_hora_mes"]])
        else:
            st.info("Sin datos de patrones eólicos para los filtros seleccionados.")

    # --- Pregunta 3: Condiciones climáticas asociadas a reducciones ---
    with st.expander("**3. ¿Qué condiciones climáticas están asociadas con reducciones significativas en el potencial renovable?**", expanded=True):
        st.caption("Días con menor potencial (inferior al percentil 25) y sus condiciones.")
        if d_c is not None and not d_c.empty:
            cols_needed = ["fecha_solo", "ciudad", "promedio_potencial_solar_diario", "promedio_potencial_eolico_diario",
                          "promedio_nubes_diario", "total_precipitacion_diaria_mm", "promedio_humedad_diaria", "promedio_temperatura_diaria_c"]
            have = [c for c in cols_needed if c in d_c.columns]
            if len(have) >= 4:
                solar_col = "promedio_potencial_solar_diario" if "promedio_potencial_solar_diario" in d_c.columns else None
                eolic_col = "promedio_potencial_eolico_diario" if "promedio_potencial_eolico_diario" in d_c.columns else None
                if solar_col:
                    q25 = d_c[solar_col].quantile(0.25)
                    low = d_c[d_c[solar_col] <= q25][have]
                elif eolic_col:
                    q25 = d_c[eolic_col].quantile(0.25)
                    low = d_c[d_c[eolic_col] <= q25][have]
                else:
                    low = d_c[have].head(0)
                if not low.empty:
                    st.dataframe(low, use_container_width=True)
                else:
                    st.info("No hay suficientes días para calcular percentil 25.")
            else:
                st.dataframe(d_c[have] if have else d_c.head(10), use_container_width=True)
        else:
            st.info("Sin datos de resumen diario.")

    # --- Pregunta 4: Predicciones vs observado ---
    with st.expander("**4. ¿Cómo se comportan las predicciones meteorológicas en comparación con las condiciones observadas en el pasado reciente?**", expanded=True):
        st.caption("Comparación: Clima (observado) vs Forecast (pronóstico) para la misma ciudad y mes.")
        if d_c is not None and not d_c.empty and d_f is not None and not d_f.empty:
            metric_cols = ["promedio_temperatura_diaria_c", "promedio_potencial_solar_diario", "promedio_potencial_eolico_diario"]
            exist = [c for c in metric_cols if c in d_c.columns and c in d_f.columns]
            if exist:
                agg_c = d_c[exist].mean().to_frame().T
                agg_c["origen"] = "Observado (clima)"
                agg_f = d_f[exist].mean().to_frame().T
                agg_f["origen"] = "Pronóstico (forecast)"
                compare = pd.concat([agg_c, agg_f], ignore_index=True)
                st.dataframe(compare[["origen"] + exist], use_container_width=True)
            else:
                st.info("Columnas de métricas no disponibles en ambos conjuntos.")
        else:
            st.info("Se necesitan datos de Clima y Forecast para comparar.")

    # --- Pregunta 5: Días con mayor y menor potencial ---
    with st.expander("**5. ¿Cuáles fueron los días con mayor y menor potencial energético en cada ubicación durante el periodo de análisis?**", expanded=True):
        st.caption("Fuente: Gold clima — resumen_clima_diario. Top y bottom por potencial solar/eólico.")
        if d_c is not None and not d_c.empty:
            solar_col = "promedio_potencial_solar_diario" if "promedio_potencial_solar_diario" in d_c.columns else None
            eolic_col = "promedio_potencial_eolico_diario" if "promedio_potencial_eolico_diario" in d_c.columns else None
            disp = ["ciudad", "fecha_solo", "promedio_potencial_solar_diario", "promedio_potencial_eolico_diario", "max_potencial_eolico_diario", "max_potencial_solar_diario"]
            disp = [c for c in disp if c in d_c.columns]
            if not disp:
                disp = list(d_c.columns[:6])
            n = 5
            if "ciudad" in d_c.columns:
                for city in sorted(d_c["ciudad"].unique()):
                    sub = d_c[d_c["ciudad"] == city]
                    st.markdown(f"**{city}**")
                    sort_col = solar_col or eolic_col or disp[-1]
                    if sort_col:
                        top = sub.nlargest(n, sort_col)[disp]
                        bot = sub.nsmallest(n, sort_col)[disp]
                        st.markdown("Mayor potencial")
                        st.dataframe(top, use_container_width=True)
                        st.markdown("Menor potencial")
                        st.dataframe(bot, use_container_width=True)
            else:
                sort_col = solar_col or eolic_col
                if sort_col:
                    top = d_c.nlargest(n, sort_col)[disp]
                    bot = d_c.nsmallest(n, sort_col)[disp]
                    st.dataframe(top, use_container_width=True)
                    st.dataframe(bot, use_container_width=True)
        else:
            st.info("Sin datos de resumen diario para los filtros seleccionados.")


def main():
    st.set_page_config(page_title="Consumo Gold", layout="wide")
    st.title("Consumo Gold: Clima histórico vs Forecast")

    if not BUCKET:
        st.error(
            "Configura **BUCKET_NAME_GOLD** en el archivo `.env` o en las variables de entorno."
        )
        st.stop()

    # Carga única de los cuatro conjuntos Gold (cache por load_parquet_safe)
    df_c_daily, err_c_d = load_parquet_safe(PATH_CLIMA_DAILY)
    df_c_patt, err_c_p = load_parquet_safe(PATH_CLIMA_PATTERNS)
    df_f_daily, err_f_d = load_parquet_safe(PATH_FORECAST_DAILY)
    df_f_patt, err_f_p = load_parquet_safe(PATH_FORECAST_PATTERNS)

    tab_clima, tab_forecast, tab_preguntas = st.tabs([
        "Clima (Gold histórico)", "Forecast (Gold pronóstico)", "Preguntas de negocio"
    ])

    with tab_clima:
        st.caption("Datos desde gold/resumen_clima_diario y gold/patrones_horarios")
        if err_c_d:
            st.warning(f"Resumen diario (clima): {err_c_d}")
        if err_c_p:
            st.warning(f"Patrones (clima): {err_c_p}")
        render_tab(df_c_daily, df_c_patt, "clima")

    with tab_forecast:
        st.caption("Datos desde gold/forecast/resumen_clima_diario y gold/forecast/patrones_horarios")
        if err_f_d:
            st.warning(f"Resumen diario (forecast): {err_f_d}")
        if err_f_p:
            st.warning(f"Patrones (forecast): {err_f_p}")
        render_tab(df_f_daily, df_f_patt, "forecast")

    with tab_preguntas:
        st.caption("Respuestas analíticas a partir de las tablas Gold (clima y forecast).")
        if err_c_d and err_c_p and err_f_d and err_f_p:
            st.warning("No hay datos Gold cargados. Revisa el bucket y las rutas S3.")
        else:
            render_preguntas_negocio(df_c_daily, df_c_patt, df_f_daily, df_f_patt)


if __name__ == "__main__":
    main()
