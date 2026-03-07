# Optimizaciones del Pipeline ETL

Este documento centraliza todas las optimizaciones implementadas en el pipeline de datos del proyecto.

---

## 1. Manejo de formatos de datos heterogéneos (Silver)

**Problema:** Los datos de entrada tienen estructuras diferentes según su origen:

| Origen | Formato | Ubicación de temperatura | Campo ciudad |
|--------|---------|--------------------------|--------------|
| Históricos | JSON anidado (OpenWeatherMap) | `main.temp` | `name` |
| Stream (Airbyte) | JSON con wrapper `_airbyte_data` | `_airbyte_data.current.temp` | No existe |

**Solución implementada:**

1. **Detección automática del formato:** El código detecta si el DataFrame contiene `_airbyte_data` (Airbyte onecall) o `main` (históricos) para aplicar la lógica de extracción correcta.

2. **Extracción de ciudad desde ruta S3:** Para datos de Airbyte que no incluyen el nombre de la ciudad, se extrae de la ruta del archivo:
   ```
   s3a://bucket/stream/Patagonia/onecall/archivo.jsonl.gz
                       ^^^^^^^^^ → ciudad = "Patagonia"
   ```

3. **Limpieza de metadatos Airbyte:** Se eliminan las columnas `_airbyte_*` que son metadatos de Airbyte y no datos útiles para el análisis.

**Archivo modificado:** `capa_silver.py` - función `safe_flatten_structs()`

---

## 2. Conversión de Kelvin a Celsius (Silver)

**Problema:** OpenWeatherMap API devuelve temperaturas en Kelvin por defecto. El campo `temperatura_c` contenía valores en Kelvin (ej: 287.37) cuando debería contener Celsius (ej: 14.22).

**Solución implementada:**

Aplicar la conversión `°C = K - 273.15` en el momento de extracción de la temperatura:

```python
# Antes (incorrecto):
df = df.withColumn("temperatura_c", round(temp_field, 2))

# Después (correcto):
df = df.withColumn("temperatura_c", round(temp_field - lit(273.15), 2))
```

**Ubicaciones del cambio:**
- Datos Airbyte onecall: extracción de `_airbyte_data.current.temp`
- Datos históricos: extracción de `main.temp`

**Archivo modificado:** `capa_silver.py` - función `safe_flatten_structs()`

**Beneficios:**
- Los datos almacenados son semánticamente correctos
- Todos los consumidores reciben datos en la unidad esperada (Celsius)
- Se evita duplicar lógica de conversión en cada consumidor
- Se elimina el riesgo de análisis incorrectos por unidades equivocadas

---

## 3. Dynamic Partition Overwrite (Silver)

**Problema:** Sin optimización, cada ejecución del ETL Silver reprocesaría todos los datos (históricos + stream) y sobrescribiría todo el contenido de Silver, incluyendo años que no cambiaron.

**Solución implementada:**

- **Configuración:** `spark.sql.sources.partitionOverwriteMode = dynamic`
- **Particionamiento:** Silver se particiona por `ciudad` y `event_year` (año del evento).

**Lógica de ejecución:**

| Tipo de ejecución | Datos procesados | Particiones sobrescritas |
|-------------------|------------------|--------------------------|
| Primera ejecución | Históricos + stream | Todas (2023, 2024, 2025, 2026) |
| Ejecuciones posteriores | Solo stream | Solo año actual (2026) |

**Detección de primera ejecución:**
- Función `check_historical_data_exists()` verifica si existen particiones de años anteriores **sin leer datos**, solo consultando metadatos de S3 (operaciones ListObjects).
- Costo: ~$0.00002 por verificación.

**Beneficios:**

| Métrica | Sin optimización | Con optimización | Ahorro |
|---------|-----------------|------------------|--------|
| Registros procesados | ~35,000 | ~1,400 | **96%** |
| Tiempo de ejecución | ~5 min | ~30 seg | **90%** |
| Requests S3 (lectura) | ~730 | ~30 | **96%** |

**Archivo modificado:** `capa_silver.py`

---

## 4. Dynamic Partition Overwrite (Gold)

**Problema:** Sin optimización, cada ejecución del ETL Gold reprocesaría todo Silver y recalcularía agregaciones de todos los meses históricos.

**Solución implementada:**

- **Configuración:** `spark.sql.sources.partitionOverwriteMode = dynamic`
- **Particionamiento:** Gold se particiona por `ciudad` y `mes_anio`.

**Lógica de ejecución:**

| Tipo de ejecución | Datos de Silver leídos | Particiones Gold sobrescritas |
|-------------------|------------------------|-------------------------------|
| Primera ejecución | Todo Silver | Todas (2024-01 a 2026-03) |
| Ejecuciones posteriores | Solo mes actual | Solo mes actual (2026-03) |

**Detección de primera ejecución:**
- Función `check_gold_historical_exists()` verifica si existen particiones de meses anteriores **sin leer datos**.

**Beneficios:**

| Métrica | Sin optimización | Con optimización | Ahorro |
|---------|-----------------|------------------|--------|
| Registros leídos de Silver | ~35,000 (2 años) | ~1,400 (1 mes) | **96%** |
| Tiempo de ejecución | ~5 min | ~30 seg | **90%** |
| Requests S3 (GET/PUT) | ~730 | ~14 | **98%** |

**Archivo modificado:** `capa_gold.py`

---

## 5. Partition Pruning en lectura de Silver (Gold)

**Problema:** Silver está particionado por `ciudad/event_year`, pero Gold necesita filtrar por `mes_anio`. Sin optimización, Gold leería todos los años de Silver antes de filtrar por mes.

**Solución implementada:**

Filtrado en dos pasos para aprovechar el partition pruning:

```python
# Paso 1: Filtrar por event_year (PARTITION PRUNING - solo lee año actual)
df_silver_year = spark.read.parquet(INPUT_PATH).filter(
    col("event_year") == CURRENT_YEAR  # Solo lee partición 2026
)

# Paso 2: Filtrar por mes_anio (dentro del año ya cargado)
df_silver = df_silver_year.filter(col("mes_anio") == CURRENT_YEAR_MONTH)
```

**Beneficio:** En ejecuciones incrementales, Spark solo lee la partición del año actual (~1/3 de los datos) antes de filtrar por mes.

**Archivo modificado:** `capa_gold.py`

---

## 6. Uso estratégico de caché (Silver y Gold)

**Problema:** Sin caché, los DataFrames se recalculan cada vez que se ejecuta una acción (count, show, write), lo que implica múltiples lecturas de S3.

**Solución implementada:**

```python
# Cachear después de transformaciones costosas
df_silver_final.cache()
df_silver_final.count()  # Materializa el caché

# Usar el DataFrame cacheado para múltiples acciones
df_silver_final.show(5)
df_silver_final.write.parquet(OUTPUT_PATH)

# Liberar caché después de la escritura
df_silver_final.unpersist()
```

**Beneficio:** Evita releer S3 y recalcular transformaciones para cada acción.

**Archivos modificados:** `capa_silver.py`, `capa_gold.py`

---

## 7. Control de shuffle partitions

**Problema:** El valor por defecto de `spark.sql.shuffle.partitions` (200) genera demasiadas tareas vacías en datasets pequeños/medianos, creando muchos archivos pequeños.

**Solución implementada:**

| Capa | Valor | Justificación |
|------|-------|---------------|
| Silver | 100 | Dataset moderado (~35K registros) |
| Gold | 20 | Dataset pequeño (agregaciones) |

**Beneficio:** Reduce overhead de scheduling y evita archivos pequeños en S3.

**Archivos modificados:** `capa_silver.py`, `capa_gold.py`

---

## 8. Formato Parquet columnar

**Problema:** Los datos en JSON/JSONL ocupan más espacio y las lecturas son menos eficientes.

**Solución implementada:**

Almacenar Silver y Gold en formato Parquet con particionamiento:

| Capa | Particiones | Beneficio |
|------|-------------|-----------|
| Silver | `ciudad/event_year` | Partition pruning por año |
| Gold | `ciudad/mes_anio` | Partition pruning por mes |

**Beneficios:**
- Compresión columnar (~70% menos espacio vs JSON)
- Lecturas selectivas (solo columnas necesarias)
- Partition pruning (solo carpetas necesarias)

---

## Resumen de ahorros estimados

| Optimización | Ahorro en tiempo | Ahorro en costos S3 |
|--------------|------------------|---------------------|
| Dynamic Partition Overwrite (Silver) | ~90% | ~96% requests |
| Dynamic Partition Overwrite (Gold) | ~90% | ~98% requests |
| Partition Pruning (Gold→Silver) | ~70% | ~70% datos leídos |
| Caché | ~50% | ~50% requests |
| Parquet | ~30% | ~70% almacenamiento |

---

## Estructura de particiones resultante

### Silver
```
s3://bucket-silver/silver/clima_procesado/
├── ciudad=Patagonia/
│   ├── event_year=2024/    ← Se procesa UNA VEZ
│   ├── event_year=2025/    ← Se procesa UNA VEZ
│   └── event_year=2026/    ← Se actualiza cada ejecución
└── ciudad=Riohacha/
    └── ... (misma estructura)
```

### Gold
```
s3://bucket-gold/gold/
├── resumen_clima_diario/
│   ├── ciudad=Patagonia/
│   │   ├── mes_anio=2024-01/    ← Se procesa UNA VEZ (~31 filas)
│   │   ├── mes_anio=2024-02/    ← Se procesa UNA VEZ
│   │   │   ...
│   │   └── mes_anio=2026-03/    ← Se actualiza cada ejecución
│   └── ciudad=Riohacha/
│       └── ... (misma estructura)
└── patrones_horarios/
    └── ... (misma estructura, ~24 filas/mes)
```

---

*Documentación de optimizaciones para el proyecto data-engineering-proyecto-4 (Módulo 4).*
