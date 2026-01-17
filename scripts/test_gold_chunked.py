import os
import duckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta

# CONFIGURACION
BUCKET_NAME = 'bigdatabucket-ducklake'
MY_ACCESS_KEY = '' 
MY_SECRET_KEY = ''

os.environ.setdefault('USUARIO_POSTGRES', '')
os.environ.setdefault('CONTR_POSTGRES', '')

def get_ducklake_connection():
    os.makedirs('/opt/airflow/data/tmp_duckdb', exist_ok=True)
    con = duckdb.connect(config={
        'allow_unsigned_extensions': 'true',
        'memory_limit': '8GB',
        'temp_directory': '/opt/airflow/data/tmp_duckdb',
        'preserve_insertion_order': 'false'
    })
    con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs; INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
    con.execute("SET s3_region='eu-central-1'; SET http_timeout=600000; SET threads=4;")
    con.execute(f"CREATE OR REPLACE SECRET secret_s3 (TYPE S3, KEY_ID '{MY_ACCESS_KEY}', SECRET '{MY_SECRET_KEY}', REGION 'eu-central-1');")
    con.execute(f"CREATE OR REPLACE SECRET secreto_postgres (TYPE postgres, HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech', PORT 5432, DATABASE neondb, USER '{os.environ['USUARIO_POSTGRES']}', PASSWORD '{os.environ['CONTR_POSTGRES']}');")
    con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
    con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lakehouse (DATA_PATH 's3://{BUCKET_NAME}/ducklake/')")
    con.execute("USE lakehouse")
    return con

def test_gold_od():
    con = get_ducklake_connection()
    print("Generando gold_od_matrix_top (Chunked Test)...")
    
    con.execute("""
        CREATE OR REPLACE TEMP TABLE local_od_agg (
            origen_zone_id VARCHAR,
            destino_zone_id VARCHAR,
            viajes DOUBLE,
            viajes_km DOUBLE
        );
    """)
    
    start_date = datetime(2022, 1, 1)

    for i in range(1): 
        current_month = start_date + relativedelta(months=i)
        next_month = current_month + relativedelta(months=1)
        f_start = current_month.strftime('%Y-%m-%d')
        f_end = next_month.strftime('%Y-%m-%d')
        
        print(f"  Procesando mes {f_start}...")
        try:
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
        except Exception as e:
            print(f"ERROR en mes {f_start}: {e}")
            raise e
    
    print("Test completado.")
    con.close()

if __name__ == "__main__":
    test_gold_od()
