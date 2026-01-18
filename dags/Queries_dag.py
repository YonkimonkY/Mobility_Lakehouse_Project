import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

# CONFIGURACION
BUCKET_NAME = 'bigdatabucket-ducklake'
MY_ACCESS_KEY = '' 
MY_SECRET_KEY = ''

os.environ.setdefault('USUARIO_POSTGRES', '')
os.environ.setdefault('CONTR_POSTGRES', '')

def get_ducklake_connection():
    """
    Conexion optimizada con spill a disco local del contenedor (/usr/local/airflow/tmp_duckdb)
    """
    # Asegurar que el directorio temporal existe y es escribible
    temp_dir = '/usr/local/airflow/tmp_duckdb'
    os.makedirs(temp_dir, exist_ok=True)
    
    con = duckdb.connect(config={
        'allow_unsigned_extensions': 'true',
        'memory_limit': '3GB', # Reducido drasticamente para evitar OOM del contenedor
        'temp_directory': temp_dir,
        'preserve_insertion_order': 'false',
        'max_memory': '4GB'
    })
    
    con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs; INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
    
    # Timeout alto, pocos threads para ahorrar memoria
    con.execute("SET s3_region='eu-central-1'; SET http_timeout=600000; SET threads=2;")

    con.execute(f"CREATE OR REPLACE SECRET secret_s3 (TYPE S3, KEY_ID '{MY_ACCESS_KEY}', SECRET '{MY_SECRET_KEY}', REGION 'eu-central-1');")
    con.execute(f"CREATE OR REPLACE SECRET secreto_postgres (TYPE postgres, HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech', PORT 5432, DATABASE neondb, USER '{os.environ['USUARIO_POSTGRES']}', PASSWORD '{os.environ['CONTR_POSTGRES']}');")
    con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
    
    con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lakehouse (DATA_PATH 's3://{BUCKET_NAME}/ducklake/')")
    con.execute("USE lakehouse")
    return con

DEFAULT_QUERY = "SELECT * FROM gold_od_matrix_top LIMIT 10"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="execute_layer_query",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ad-hoc", "query", "bronze", "silver", "gold"],
    params={
        "query": Param(DEFAULT_QUERY, type="string")
    },
)
def execute_layer_query_dag():
    @task()
    def run_query():
        context = get_current_context()
        dag_run = context.get("dag_run")
        params = context.get("params") or {}
        conf = (dag_run.conf or {}) if dag_run else {}
        
        query = conf.get("query") or params.get("query") or DEFAULT_QUERY

        print(f"Query:\n{query}")

        con = get_ducklake_connection()
        try:
            cursor = con.execute(query)
            try:
                df = cursor.fetch_df()
                if not df.empty:
                    with pd.option_context(
                        "display.max_rows", None,
                        "display.max_columns", None,
                        "display.width", None,
                        "display.max_colwidth", None,
                    ):
                        print(df.to_string(index=False))
            except Exception:
                print("Command executed.")
        finally:
            con.close()

    run_query()


execute_layer_query_dag()
