import duckdb
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# --- CONFIG ---
BUCKET_NAME = 'bigdatabucket-ducklake'
BRONZE_TABLE = 'bronze_mitma_viajes'

SILVER_FACT_TABLE = 'silver_fact_viajes_v2'
BRONZE_MAPA = 'bronze_zonificacion'


os.environ.setdefault('USUARIO_POSTGRES', '')
os.environ.setdefault('CONTR_POSTGRES', '')

default_args = {
    'owner': 'grupo_movilidad',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# --- 2. CONEX ---
def get_db_connection():
    print(f"Conectando a DuckDB... (v{duckdb.__version__})")
    con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
    
    con.execute("SET memory_limit='512MB';")
    con.execute("SET temp_directory='/tmp/';")
    con.execute("SET preserve_insertion_order=false;") 
    
    extensions = ['ducklake', 'postgres', 'spatial', 'httpfs', 'aws']
    for ext in extensions:
        try: con.execute(f"INSTALL {ext}; LOAD {ext};")
        except: pass 

    MY_ACCESS_KEY = '' 
    MY_SECRET_KEY = ''
    
    con.execute(f"""
        CREATE OR REPLACE SECRET secret_s3 (
            TYPE S3, KEY_ID '{MY_ACCESS_KEY}', SECRET '{MY_SECRET_KEY}', REGION 'eu-central-1'
        );
    """)

    if 'USUARIO_POSTGRES' in os.environ:
        con.execute(f"""
            CREATE OR REPLACE SECRET secreto_postgres (
                TYPE postgres, HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech', 
                PORT 5432, DATABASE neondb, USER '{os.environ["USUARIO_POSTGRES"]}', PASSWORD '{os.environ["CONTR_POSTGRES"]}'
            );
        """)

    con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")

    try:
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS bdet_upv (DATA_PATH 's3://{BUCKET_NAME}/ducklake/')")
        con.execute("USE bdet_upv")
    except Exception as e: print(f"Info DuckLake: {e}")
        
    return con


# --- TABLE ---
def task_process_dimensions(**kwargs):
    print("--- PROCESANDO DIMENSIONES SILVER ---")
    con = get_db_connection()
    
    try:
        
        print("Generando Silver Zonas...")
        con.execute("""
            CREATE OR REPLACE TABLE silver_dim_zonas AS 
            SELECT 
                z.id as zone_id,
                MAX(z.name) as zone_name,
                MAX(z.zone_type) as zone_level,
                CASE WHEN LENGTH(z.id) >= 2 THEN LEFT(z.id, 2) ELSE NULL END as provincia_code
            FROM bronze_ine_zones z
            WHERE z.id IS NOT NULL AND z.id != ''
            GROUP BY z.id
        """)
        
        
        print("Generando Silver Calendario...")
        con.execute("""
            CREATE OR REPLACE TABLE silver_dim_calendario AS
            SELECT DISTINCT 
                strptime(fecha, '%Y%m%d')::DATE as fecha,
                CAST(LEFT(fecha, 4) AS INTEGER) as anio,
                CAST(SUBSTRING(fecha, 5, 2) AS INTEGER) as mes,
                CAST(RIGHT(fecha, 2) AS INTEGER) as dia,
                dia_semana,
                (CASE WHEN tipo_dia = 'laborable' THEN TRUE ELSE FALSE END) as es_laborable,
                (CASE WHEN es_festivo_nacional = 1 THEN TRUE ELSE FALSE END) as es_festivo_nacional,
                (CASE WHEN tipo_dia = 'fin_de_semana' THEN TRUE ELSE FALSE END) as es_fin_de_semana
            FROM bronze_calendario_laboral
            WHERE zona_provincia IS NULL
        """)

        
        print("Generando Silver Jerarquía...")
        con.execute("""
            CREATE OR REPLACE TABLE silver_dim_zona_jerarquia AS
            SELECT 
                row_number() OVER () as jerarquia_id,
                r.distrito_mitma as distrito_id,
                r.municipio_mitma as municipio_id,
                r.gau_mitma as gau_id
            FROM bronze_ine_relacion r
            WHERE EXISTS (SELECT 1 FROM silver_dim_zonas WHERE zone_id = r.distrito_mitma)
        """)
        
       
        print("Generando Silver Atributos (Recuperando Distritos y Agregaciones)...")
        try:
            check = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{BRONZE_MAPA}'").fetchone()[0]
            if check > 0:
                con.execute(f"""
                    CREATE OR REPLACE TABLE silver_dim_zona_atributos AS
                    SELECT 
                        z.zone_id,
                        ST_Y(ST_Centroid(zg.geom)) as centroid_lat,
                        ST_X(ST_Centroid(zg.geom)) as centroid_lon,
                        ST_AsText(zg.geom) as geometry_wkt
                    FROM silver_dim_zonas z
                    LEFT JOIN {BRONZE_MAPA} zg 
                    ON 
                        -- 1. Coincidencia Exacta
                        z.zone_id = zg.ID 
                        
                        -- 2. Coincidencia Numérica simple (1 -> '00001')
                        OR z.zone_id = printf('%05d', TRY_CAST(zg.ID AS INTEGER))
                        
                        -- 3. Limpieza de Sufijos (_AM, _AD)
                        -- Ej: '01004_AM' -> '01004'
                        OR REPLACE(REPLACE(z.zone_id, '_AM', ''), '_AD', '') = printf('%05d', TRY_CAST(zg.ID AS INTEGER))
                        
                        -- 4. ¡NUEVO! Poda de Distritos (7 cifras -> 5 cifras)
                        -- Ej: '5200108' (Distrito) -> '52001' (Melilla)
                        OR (
                            LENGTH(z.zone_id) > 5 
                            AND LEFT(z.zone_id, 5) = printf('%05d', TRY_CAST(zg.ID AS INTEGER))
                            -- Nos aseguramos de que los 5 primeros sean números para no romper 'GAU...'
                            AND TRY_CAST(LEFT(z.zone_id, 5) AS INTEGER) IS NOT NULL
                        )
                """)
                
               
                total = con.execute("SELECT COUNT(*) FROM silver_dim_zona_atributos").fetchone()[0]
                nulos = con.execute("SELECT COUNT(*) FROM silver_dim_zona_atributos WHERE centroid_lon IS NULL").fetchone()[0]
                
                print(f"Geometría calculada v4. Total Zonas: {total}")
                print(f"Zonas 'fantasma' restantes: {nulos} (Debería haber bajado drásticamente)")
                
            else:
                print(f"La tabla {BRONZE_MAPA} no existe.")
        except Exception as e_geo:
            print(f"Aviso Geometría: {e_geo}")

    except Exception as e:
        print(f"Error crítico en Dimensiones: {e}")
        raise e
    finally:
        con.close()
# --- TASK 2 ---
def task_process_facts_daily(**kwargs):
    fecha_proceso = kwargs['ds_nodash'] 
    print(f"--- PROCESANDO HECHOS (FACTS) PARA: {fecha_proceso} ---")
    con = get_db_connection()
    
    try:
        
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {SILVER_FACT_TABLE} (
                viaje_id UBIGINT, -- ¡CAMBIO CRÍTICO! UBIGINT soporta IDs de hash
                fecha DATE,
                hora INTEGER,
                origen_zone_id VARCHAR,
                destino_zone_id VARCHAR,
                viajes DOUBLE,
                viajes_km DOUBLE
            );
        """)

       
        try:
            con.execute(f"DELETE FROM {SILVER_FACT_TABLE} WHERE fecha = strptime('{fecha_proceso}', '%Y%m%d')::DATE")
            print(f"Limpiados datos previos del día {fecha_proceso}.")
        except: pass

        
        print("Agregando e insertando datos...")
        con.execute(f"""
            INSERT INTO {SILVER_FACT_TABLE}
            SELECT 
                hash(fecha, periodo, origen, destino) as viaje_id,
                strptime(fecha, '%Y%m%d')::DATE as fecha,
                CAST(RIGHT(periodo, 2) AS INTEGER) as hora,
                origen as origen_zone_id,
                destino as destino_zone_id,
                SUM(viajes) as viajes,
                SUM(viajes_km) as viajes_km
            FROM {BRONZE_TABLE}
            WHERE fecha = '{fecha_proceso}'
            GROUP BY fecha, periodo, origen, destino
        """)
        
        count = con.execute(f"SELECT COUNT(*) FROM {SILVER_FACT_TABLE} WHERE fecha = strptime('{fecha_proceso}', '%Y%m%d')::DATE").fetchone()[0]
        print(f"Día {fecha_proceso}: Generados {count:,} registros en Silver.")

    except Exception as e:
        print(f"Error en Facts: {e}")
        raise e
    finally:
        con.close()

# --- DAG ---
@dag(
    dag_id="mobility_silver_daily_aggregation",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 12, 31),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['silver', 'daily', 'transform']
)
def mobility_silver_daily_aggregation():

    @task()
    def process_dimensions():
        task_process_dimensions()

    @task()
    def process_facts_daily(ds_nodash=None):
        task_process_facts_daily(ds_nodash=ds_nodash)

    dims = process_dimensions()
    facts = process_facts_daily()

    dims >> facts


mobility_silver_daily_aggregation()