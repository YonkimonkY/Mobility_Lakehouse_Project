# Mobility Lakehouse Project

Proyecto de lakehouse para analisis de movilidad con capas Bronze, Silver y Gold usando DuckDB y datos de MITMA/INE. Incluye ingesta, transformaciones, agregaciones y visualizacion en mapa.

## Objetivo
- Unificar fuentes de movilidad y contexto socioeconomico.
- Construir un modelo dimensional (Silver) y tablas analiticas (Gold).
- Generar insights y un mapa interactivo de flujos OD.

## Tecnologias
- DuckDB (incluye extension spatial)
- Python
- Airflow (DAGs)
- Pandas / GeoPandas / Kepler.gl

## Estructura del repositorio
- `src/` scripts principales del pipeline local.
- `src/sql/` SQL de Bronze, Silver y Gold.
- `dags/` DAGs de Airflow para ejecucion en S3/DuckLake.
- `scripts/` utilidades y validaciones.
- `data/` datos fuente y salidas locales.
- `reports/` reportes HTML (mapa y revisiones).

## Requisitos
- Python 3.9+ recomendado.
- Paquetes en `requirements.txt`.
- Datos en `data/raw` con la estructura esperada.

## Instalacion
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Datos (estructura esperada)
```
data/
  raw/
    mitma/
      *Viajes_distritos.csv.gz
      *Personas_dia_distritos.csv.gz
      zonificacion/
        zonificacion_matriz_tiempos.shp (+ archivos auxiliares)
    ine/
      zones/
        nombres_distritos.csv
        nombres_municipios.csv
        relacion_ine_zonificacionMitma.csv
      demographics/
        demografico.csv
      economic/
        economica.csv
    calendario/
      calendario_laboral.csv
```

## Ejecucion local (pipeline completo)
1) Ingesta Bronze
```bash
python src/ingest_bronze.py
```

2) Transformacion Silver
```bash
python src/process_silver.py
```

3) Agregaciones Gold
```bash
python src/process_gold.py
```

4) Mapa interactivo
```bash
python src/visualization.py
```

## Airflow
En `dags/` hay DAGs para ejecucion en S3/DuckLake. Requiere credenciales AWS y, si aplica, conexion a Postgres/Neon para metadata.

## Scripts utiles
- `scripts/verify_lakehouse.py` verificacion rapida (S3/DuckLake).
- `scripts/check_bronze.py` / `scripts/check_silver.py` chequeos basicos.
- `scripts/inspect_csv.py` inspeccion de archivos.

## Notas
- La ejecucion completa puede tardar varias horas segun el volumen de datos.
- La extension `spatial` de DuckDB debe instalarse al ejecutar los scripts.
