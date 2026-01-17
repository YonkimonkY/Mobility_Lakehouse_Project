import os
import duckdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.decorators import dag, task

# --- CONFIG ---
BUCKET_NAME = 'bigdatabucket-ducklake'
MY_ACCESS_KEY = '' 
MY_SECRET_KEY = ''

os.environ.setdefault('USUARIO_POSTGRES', '')
os.environ.setdefault('CONTR_POSTGRES', '')

def get_ducklake_connection():
    """
    Conexion optimizada con spill a disco local del contenedor (/usr/local/airflow/tmp_duckdb)
    """
    
    temp_dir = '/usr/local/airflow/tmp_duckdb'
    os.makedirs(temp_dir, exist_ok=True)
    
    con = duckdb.connect(config={
        'allow_unsigned_extensions': 'true',
        'memory_limit': '3GB', 
        'temp_directory': temp_dir,
        'preserve_insertion_order': 'false',
        'max_memory': '4GB'
    })
    
    con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs; INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
    
    con.execute("SET s3_region='eu-central-1'; SET http_timeout=600000; SET threads=2;")

    con.execute(f"CREATE OR REPLACE SECRET secret_s3 (TYPE S3, KEY_ID '{MY_ACCESS_KEY}', SECRET '{MY_SECRET_KEY}', REGION 'eu-central-1');")
    con.execute(f"CREATE OR REPLACE SECRET secreto_postgres (TYPE postgres, HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech', PORT 5432, DATABASE neondb, USER '{os.environ['USUARIO_POSTGRES']}', PASSWORD '{os.environ['CONTR_POSTGRES']}');")
    con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
    
    con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lakehouse (DATA_PATH 's3://{BUCKET_NAME}/ducklake/')")
    con.execute("USE lakehouse")
    return con

@dag(
    dag_id='mobility_gold_chunked',
    default_args={'owner': 'grupo_movilidad', 'retries': 2, 'retry_delay': timedelta(minutes=2)},
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['gold', 'chunked', 'stable'],
    max_active_runs=1,
    max_active_tasks=1,
)
def gold_layer_chunked():

    @task()
    def task_gold_od_matrix():
        con = get_ducklake_connection()
        print("Generando gold_od_matrix_top (Estrategia Chunked)...")
        
        con.execute("""
            CREATE OR REPLACE TEMP TABLE local_od_agg (
                origen_zone_id VARCHAR,
                destino_zone_id VARCHAR,
                viajes DOUBLE,
                viajes_km DOUBLE
            );
        """)
        
        start_date = datetime(2022, 1, 1)
        for i in range(12):
            current_month = start_date + relativedelta(months=i)
            next_month = current_month + relativedelta(months=1)
            f_start = current_month.strftime('%Y-%m-%d')
            f_end = next_month.strftime('%Y-%m-%d')
            
            print(f"  Procesando mes {f_start}...")
            con.execute(f"""
                INSERT INTO local_od_agg
                SELECT 
                    origen_zone_id,
                    destino_zone_id,
                    SUM(viajes),
                    SUM(viajes_km)
                FROM silver_fact_viajes
                WHERE fecha >= '{f_start}' AND fecha < '{f_end}'
                GROUP BY origen_zone_id, destino_zone_id
            """)
            print(f"  > Mes {f_start} completado.")
        
        print("Agregando resultados finales y escribiendo a Gold...")
        con.execute("""
            CREATE OR REPLACE TABLE gold_od_matrix_top AS
            WITH final_agg AS (
                SELECT 
                    origen_zone_id,
                    destino_zone_id,
                    SUM(viajes) as total_trips,
                    SUM(viajes_km) as total_km
                FROM local_od_agg
                GROUP BY origen_zone_id, destino_zone_id
                HAVING SUM(viajes) > 100
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY f.total_trips DESC) as od_id,
                f.origen_zone_id,
                zo.zone_name as origen_zone_name,
                f.destino_zone_id,
                zd.zone_name as destino_zone_name,
                f.total_trips,
                f.total_km
            FROM final_agg f
            LEFT JOIN silver_dim_zonas zo ON f.origen_zone_id = zo.zone_id
            LEFT JOIN silver_dim_zonas zd ON f.destino_zone_id = zd.zone_id
            ORDER BY f.total_trips DESC
            LIMIT 10000;
        """)
        
        count = con.execute("SELECT COUNT(*) FROM gold_od_matrix_top").fetchone()[0]
        print(f"OK - {count} flujos OD generados")
        con.close()

    @task()
    def task_gold_hourly_patterns():
        con = get_ducklake_connection()
        print("Generando gold_hourly_patterns (Chunked)...")
        
        con.execute("CREATE OR REPLACE TEMP TABLE local_hourly_agg (hora INTEGER, viajes DOUBLE, viajes_km DOUBLE)")
        
        start_date = datetime(2022, 1, 1)
        for i in range(12):
            current_month = start_date + relativedelta(months=i)
            next_month = current_month + relativedelta(months=1)
            f_start = current_month.strftime('%Y-%m-%d')
            f_end = next_month.strftime('%Y-%m-%d')
            
            print(f"  Procesando mes {f_start}...")
            con.execute(f"""
                INSERT INTO local_hourly_agg
                SELECT hora, SUM(viajes), SUM(viajes_km)
                FROM silver_fact_viajes
                WHERE fecha >= '{f_start}' AND fecha < '{f_end}'
                GROUP BY hora
            """)
            
        print("Escribiendo Gold Hourly...")
        con.execute("""
            CREATE OR REPLACE TABLE gold_hourly_patterns AS
            SELECT 
                hora as hour,
                SUM(viajes) as total_trips,
                SUM(viajes_km) / NULLIF(SUM(viajes), 0) as avg_trip_km,
                (SUM(viajes) * 100.0 / SUM(SUM(viajes)) OVER ()) as pct_of_daily_trips
            FROM local_hourly_agg
            GROUP BY hora
            ORDER BY hora;
        """)
        con.close()

    @task()
    def task_gold_top_zones():
        con = get_ducklake_connection()
        print("Generando gold_top_zones (Chunked)...")
        
        con.execute("CREATE OR REPLACE TEMP TABLE local_zone_agg (zone_id VARCHAR, generated DOUBLE, attracted DOUBLE)")
        
        start_date = datetime(2022, 1, 1)
        for i in range(12):
            current_month = start_date + relativedelta(months=i)
            next_month = current_month + relativedelta(months=1)
            f_start = current_month.strftime('%Y-%m-%d')
            f_end = next_month.strftime('%Y-%m-%d')
            
            print(f"  Procesando mes {f_start}...")
            con.execute(f"""
                INSERT INTO local_zone_agg
                SELECT origen_zone_id, SUM(viajes), 0 FROM silver_fact_viajes WHERE fecha >= '{f_start}' AND fecha < '{f_end}' GROUP BY origen_zone_id
                UNION ALL
                SELECT destino_zone_id, 0, SUM(viajes) FROM silver_fact_viajes WHERE fecha >= '{f_start}' AND fecha < '{f_end}' GROUP BY destino_zone_id
            """)
            
        print("Escribiendo Gold Zones...")
        
        con.execute("""
            CREATE OR REPLACE TABLE gold_top_zones AS
            WITH final_agg AS (
                SELECT 
                    zone_id, 
                    SUM(generated) as trips_generated, 
                    SUM(attracted) as trips_attracted
                FROM local_zone_agg
                GROUP BY zone_id
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY (f.trips_generated + f.trips_attracted) DESC) as rank,
                f.zone_id,
                z.zone_name,
                'Unknown' as zone_level, -- CORREGIDO: Evita el error de columna inexistente
                f.trips_generated,
                f.trips_attracted,
                (f.trips_generated + f.trips_attracted) as total_trips
            FROM final_agg f
            LEFT JOIN silver_dim_zonas z ON f.zone_id = z.zone_id
            ORDER BY total_trips DESC
            LIMIT 200;
        """)
        con.close()

    @task()
    def task_gold_mobility_by_daytype():
        con = get_ducklake_connection()
        print("Generando gold_mobility_by_daytype (Chunked)...")
        
        con.execute("""
            CREATE OR REPLACE TEMP TABLE local_daytype_agg (
                es_fin_de_semana BOOLEAN,
                sum_viajes DOUBLE,
                count_records BIGINT,
                distinct_dates_count BIGINT
            )
        """)
        
        start_date = datetime(2022, 1, 1)
        for i in range(12):
            current_month = start_date + relativedelta(months=i)
            next_month = current_month + relativedelta(months=1)
            f_start = current_month.strftime('%Y-%m-%d')
            f_end = next_month.strftime('%Y-%m-%d')
            
            print(f"  Procesando mes {f_start}...")
            con.execute(f"""
                INSERT INTO local_daytype_agg
                SELECT 
                    c.es_fin_de_semana,
                    SUM(v.viajes),
                    COUNT(*),
                    COUNT(DISTINCT v.fecha)
                FROM silver_fact_viajes v
                JOIN silver_dim_calendario c ON v.fecha = c.fecha
                WHERE v.fecha >= '{f_start}' AND v.fecha < '{f_end}'
                GROUP BY c.es_fin_de_semana
            """)

        print("Escribiendo Gold Daytype...")
        con.execute("""
            CREATE OR REPLACE TABLE gold_mobility_by_daytype AS
            SELECT 
                es_fin_de_semana,
                CASE WHEN es_fin_de_semana THEN 'Weekend' ELSE 'Weekday' END as day_type,
                SUM(distinct_dates_count) as days_count,
                SUM(sum_viajes) as total_trips,
                SUM(sum_viajes) / NULLIF(SUM(count_records), 0) as avg_trips_per_record
            FROM local_daytype_agg
            GROUP BY es_fin_de_semana;
        """)
        con.close()

    @task()
    def task_generate_report():
        con = get_ducklake_connection()
        print("\n" + "="*70)
        print("INFORME GOLD - FINAL")
        print("="*70)
        
        try:
            total = con.execute("SELECT SUM(total_trips) FROM gold_hourly_patterns").fetchone()[0]
            print(f"\nTotal viajes procesados: {total:,.0f}")
            

            top_od = con.execute("""
                SELECT 
                    COALESCE(origen_zone_name, origen_zone_id), 
                    COALESCE(destino_zone_name, destino_zone_id), 
                    total_trips 
                FROM gold_od_matrix_top 
                LIMIT 3
            """).fetchall()
            
            print("\nTop 3 Flujos:")
            for o, d, t in top_od:
                print(f"  - {o} -> {d}: {t:,.0f}")
        except:
            print("No se pudo generar reporte completo.")
            
        con.close()

    od = task_gold_od_matrix()
    hr = task_gold_hourly_patterns()
    tz = task_gold_top_zones()
    dt = task_gold_mobility_by_daytype()
    rep = task_generate_report()

    [od, hr, tz, dt] >> rep

dag_run = gold_layer_chunked()
