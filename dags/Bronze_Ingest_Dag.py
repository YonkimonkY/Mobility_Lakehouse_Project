import duckdb
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# --- CONFIG ---
BUCKET_NAME = 'bigdatabucket-ducklake'


MY_ACCESS_KEY = '' 
MY_SECRET_KEY = ''


default_args = {
    'owner': 'grupo_movilidad',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# --- CONEX ---
def get_db_connection():
    print(f"Conectando a DuckDB... (v{duckdb.__version__})")
    con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
    
    
    con.execute("SET memory_limit='512MB';")
    con.execute("SET temp_directory='/tmp/';")
    con.execute("SET preserve_insertion_order=false;") 
    
    extensions = ['ducklake', 'postgres', 'spatial', 'httpfs', 'aws']
    for ext in extensions:
        try:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
        except:
            pass 

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
    except Exception as e:
        print(f"Info DuckLake: {e}")
        
    return con

# --- DAG ---
@dag(
    dag_id="mobility_bronze",
    default_args=default_args,
    max_active_runs=3,
    tags=['bronze', 'daily', 's3']
)
def mobility_dag():

    @task()
    def ingest_viajes_daily(ds_nodash=None):
        fecha_proceso = ds_nodash
        print(f"--- PROCESSING DAY: {fecha_proceso} ---")
        con = get_db_connection()
        
        s3_url_day = f"s3://{BUCKET_NAME}/raw/mitma/*{fecha_proceso}*Viajes_distritos.csv.gz"
        
        try:
            try:
                if con.execute(f"SELECT count(*) FROM glob('{s3_url_day}')").fetchone()[0] == 0:
                    print(f" Sin archivos para {fecha_proceso}. Skip.")
                    con.close()
                    return
            except: pass
            
            con.execute("""
                CREATE TABLE IF NOT EXISTS bronze_mitma_viajes (
                    fecha VARCHAR, periodo VARCHAR, origen VARCHAR, destino VARCHAR, 
                    actividad_origen VARCHAR, actividad_destino VARCHAR, residencia VARCHAR, 
                    edad VARCHAR, sexo VARCHAR, viajes DOUBLE, viajes_km DOUBLE, 
                    source_url VARCHAR 
                );
            """)
            
            try:
                con.execute(f"DELETE FROM bronze_mitma_viajes WHERE fecha = '{fecha_proceso}'")
            except: pass

            print(" Introducing data...")
            con.execute(f"""
                INSERT INTO bronze_mitma_viajes
                SELECT 
                    fecha, periodo, origen, destino, actividad_origen, actividad_destino, 
                    residencia, edad, sexo, 
                    TRY_CAST(viajes AS DOUBLE), 
                    TRY_CAST(viajes_km AS DOUBLE), 
                    filename as source_url
                FROM read_csv('{s3_url_day}', delim='|', header=True, filename=True, all_varchar=True);
            """)
            print(f" Day {fecha_proceso}: Completed Ingestion.")
        finally:
            con.close()

    @task()
    def ingest_ine_zones():
        con = get_db_connection()
        url_distritos = f"s3://{BUCKET_NAME}/raw/ine/zones/nombres_distritos.csv"
        url_munis     = f"s3://{BUCKET_NAME}/raw/ine/zones/nombres_municipios.csv"
        
        con.execute("CREATE TABLE IF NOT EXISTS bronze_ine_zones (id VARCHAR, name VARCHAR, zone_type VARCHAR);")

        for url, z_type in [(url_distritos, 'district'), (url_munis, 'municipality')]:
            try:
                con.execute(f"""
                    MERGE INTO bronze_ine_zones AS target
                    USING (SELECT ID, name, '{z_type}' as zone_type FROM read_csv('{url}', delim='|', header=True)) AS source
                    ON target.ID = source.ID AND target.zone_type = source.zone_type
                    WHEN MATCHED THEN UPDATE SET name = source.name
                    WHEN NOT MATCHED THEN INSERT BY NAME;
                """)
            except Exception as e: print(f" Error in {z_type}: {e}")
        con.close()

    @task()
    def ingest_ine_relations():
        con = get_db_connection()
        url_rel = f"s3://{BUCKET_NAME}/raw/ine/zones/relacion_ine_zonificacionMitma.csv"
        try:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS bronze_ine_relacion AS SELECT * FROM read_csv('{url_rel}', delim='|', header=True) LIMIT 0;
                MERGE INTO bronze_ine_relacion AS target
                USING (SELECT * FROM read_csv('{url_rel}', delim='|', header=True)) AS source
                ON target.seccion_ine = source.seccion_ine
                WHEN MATCHED THEN UPDATE SET distrito_mitma = source.distrito_mitma
                WHEN NOT MATCHED THEN INSERT BY NAME;
            """)
            con.execute("""
                MERGE INTO bronze_ine_zones AS target
                USING (SELECT DISTINCT gau_mitma as id, gau_mitma as name, 'gau' as zone_type FROM bronze_ine_relacion WHERE gau_mitma IS NOT NULL) AS source
                ON target.id = source.id AND target.zone_type = 'gau'
                WHEN NOT MATCHED THEN INSERT BY NAME;
            """)
        except Exception as e: print(f" Error in Relations: {e}")
        con.close()

    @task()
    def ingest_demographics_economics():
        con = get_db_connection()
        try:
            url_demo = f"s3://{BUCKET_NAME}/raw/ine/demographics/demografico.csv"
            con.execute(f"CREATE OR REPLACE TABLE bronze_ine_demographics AS SELECT * FROM read_csv('{url_demo}', delim=';', header=True, auto_detect=True)")
        except Exception as e: print(f" Error in Demographics: {e}")

        try:
            url_econ = f"s3://{BUCKET_NAME}/raw/ine/economic/economica.csv"
            con.execute(f"CREATE OR REPLACE TABLE bronze_ine_economic AS SELECT * FROM read_csv('{url_econ}', delim=';', header=True, auto_detect=True)")
        except Exception as e: print(f" Error in Economic: {e}")
        con.close()

    @task()
    def ingest_calendar():
        con = get_db_connection()
        url_cal = f"s3://{BUCKET_NAME}/raw/calendario/calendario_laboral.csv"
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS bronze_calendario_laboral (fecha VARCHAR, zona_provincia VARCHAR, tipo_dia VARCHAR, dia_semana VARCHAR, es_festivo_nacional INTEGER, es_festivo_autonomico INTEGER, nombre_festivo VARCHAR, mes INTEGER, anio INTEGER);
            """)
            con.execute(f"""
                MERGE INTO bronze_calendario_laboral AS target
                USING (SELECT * FROM read_csv('{url_cal}', delim='|', header=True)) AS source
                ON target.fecha = source.fecha AND target.zona_provincia = source.zona_provincia
                WHEN MATCHED THEN UPDATE SET tipo_dia = source.tipo_dia
                WHEN NOT MATCHED THEN INSERT BY NAME;
            """)
        except Exception as e: print(f" Error in Calendar: {e}")
        con.close()

    @task()
    def ingest_shapefiles():
        con = get_db_connection()
        url_shp = f"s3://{BUCKET_NAME}/raw/mitma/zonificacion/zonificacion_matriz_tiempos.shp"
        try:
            con.execute(f"CREATE OR REPLACE TABLE bronze_zonificacion AS SELECT * FROM ST_Read('{url_shp}')")
            print(" Shapefile loaded.")
        except Exception as e:
            print(f" Error loading Shapefile: {e}")
        con.close()

    # --- FLOW ---
    t1 = ingest_viajes_daily()
    t2 = ingest_ine_zones()
    t3 = ingest_ine_relations()
    t4 = ingest_demographics_economics()
    t5 = ingest_calendar()
    t6 = ingest_shapefiles()

    t1 >> t2 >> t3 >> t4 >> t5 >> t6


mobility_dag()