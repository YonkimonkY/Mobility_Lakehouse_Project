# Mobility Lakehouse Project

A modern Data Lakehouse implementation for mobility analysis using a Three Tier Architecture (Bronze, Silver, Gold). This project processes MITMA (Ministry of Transport) and INE (National Statistics Institute) data to generate spatial insights. This repository contains all the files created and used for the project. However, the final full execution only consists on the DAGs files where the Bronze ingestion, Silver and Gold transformation are fully executed using this DAGs. The files on the src and scripts folder were developed and creating during the sprints and here for testing local executions.

## Objetive

- Unification: Merge mobility flows with socioeconomic and demographic context.
- Build a dimensional model (Silver) and analytic tables (Gold).
- Generate insights and an interactive Origin-Destination (OD) map.

## Technologies

- Storage: Local Filesystem / AWS S3.
- Engine: DuckDB (utilizing the spatial extension).
- Orchestration: Apache Airflow.
- Data Processing: Python (Pandas, GeoPandas).
- Visualization: Kepler.gl

## Repository Structure
├── dags/                         # Airflow DAGs
│   ├── Bronze_Ingest_Dag.py      
│   ├── Silver_Transform_Dag.py
│   ├── Gold_Chunked_Dag.py
│   └── Demo.py
├── data/                         # Source data
│   ├── INE/                      # Socioeconomic & Demographic data
│   └── MITMA/                    # Mobility flows & Zoning
├── reports/                      # Output visualization & Reviews
│   ├── mapa_movilidad.html       # Interactive flow map
│   ├── Sprint2_Review.html
│   └── Sprint2_Technical_Deep_Dive.html
├── scripts/                      # Utilities & Quality checks
│   ├── check_bronze.py           # Data quality validation
│   ├── download_mitma_2022.py    # Automated data retrieval
│   ├── run_pipeline.py           # Local execution wrapper
│   └── verify_lakehouse.py       # S3/Cloud connectivity test
├── src/                          # Main source code
│   ├── sql/                      # SQL transformation logic
│   │   ├── bronze.sql
│   │   ├── silver.sql
│   │   └── gold.sql
│   ├── ingest_bronze.py          # Python ingestion logic
│   ├── process_silver.py         
│   ├── process_gold.py
│   └── visualization.py          # Map generation logic
└── requirements.txt              # Project dependencies

## Requirements
- Python 3.9+.
- Packages used in the `requirements.txt`.
- Data in `data/raw` with the required structured.

## Instalation
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Required structure in the data
```
data/raw/
├── mitma/
│   ├── *Viajes_distritos.csv.gz              # Mobility flow records
│   ├── *Personas_dia_distritos.csv.gz        # Daily person-trip data
│   └── zonificacion/
│       └── zonificacion_matriz_tiempos.shp   # GIS boundaries (plus auxiliary files)
├── ine/
│   ├── zones/
│   │   ├── nombres_distritos.csv             # District mapping
│   │   ├── nombres_municipios.csv            # Municipality mapping
│   │   └── relacion_ine_zonificacionMitma.csv # MITMA-to-INE crosswalk
│   ├── demographics/
│   │   └── demografico.csv                   # Population stats
│   └── economic/
│       └── economica.csv                    # Income and economic indicators
└── calendario/
    └── calendario_laboral.csv                # Workday/Holiday reference

## Local Execution (pipeline completo)
1) Bronze Ingest
```bash
python src/ingest_bronze.py
```

2) Silver Transformation
```bash
python src/process_silver.py
```

3) Gold Aggregations
```bash
python src/process_gold.py
```

4) Interactive map
```bash
python src/visualization.py
```

## Airflow
`dags/` folder contains the DAGs for the execution using S3/DuckLake. It requires AWS credentials and a Postgres/Neon connection for metadata.

## Scripts
- `scripts/verify_lakehouse.py` fast verification (S3/DuckLake).
- `scripts/check_bronze.py` / `scripts/check_silver.py` basic checks.
- `scripts/inspect_csv.py` files inspection.

## Notas
- The full execution can last hours depending on the data volume.
- The  DuckDB `spatial` extension must be installed to execute the scripts.
