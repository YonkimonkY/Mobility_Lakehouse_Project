import duckdb
import os

os.environ['USUARIO_POSTGRES'] = ''
os.environ['CONTR_POSTGRES'] = ''

con = duckdb.connect()
con.execute('INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake; INSTALL aws; LOAD aws;')

MY_ACCESS_KEY = '' 
MY_SECRET_KEY = ''

con.execute(f"CREATE OR REPLACE SECRET secret_s3 (TYPE S3, KEY_ID '{MY_ACCESS_KEY}', SECRET '{MY_SECRET_KEY}', REGION 'eu-central-1');")
con.execute("CREATE OR REPLACE SECRET secreto_postgres (TYPE postgres, HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech', PORT 5432, DATABASE neondb, USER 'neondb_owner', PASSWORD 'npg_xzltM9Ign1XO');")
con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
con.execute("ATTACH 'ducklake:secreto_ducklake' AS lakehouse (DATA_PATH 's3://bigdatabucket-ducklake/ducklake/')")
con.execute('USE lakehouse')

try:
    result = con.execute('SELECT COUNT(*) FROM silver_fact_viajes').fetchone()
    print(f'Registros en silver_fact_viajes: {result[0]:,}')
    
    if result[0] == 0:
        print('\nERROR: No hay datos en Silver. El DAG de Gold no puede ejecutarse.')
        print('Necesitas ejecutar primero el DAG de Bronze y Silver.')
    else:
        print('\nOK - Hay datos en Silver.')
        zonas = con.execute('SELECT COUNT(*) FROM silver_dim_zonas').fetchone()
        print(f'Zonas en silver_dim_zonas: {zonas[0]:,}')
except Exception as e:
    print(f'ERROR al consultar Silver: {e}')
