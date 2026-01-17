import duckdb
import boto3

def inspect_csv():
    con = duckdb.connect()
    con.execute("INSTALL aws; LOAD aws;")
    
    # Credenciales explícitas
    MY_ACCESS_KEY = '' 
    MY_SECRET_KEY = ''
    
    con.execute(f"""
        CREATE OR REPLACE SECRET secret_s3 (
            TYPE S3,
            KEY_ID '{MY_ACCESS_KEY}',
            SECRET '{MY_SECRET_KEY}',
            REGION 'eu-central-1'
        );
    """)
    
    try:
        df = con.execute("SELECT * FROM read_csv('s3://bigdatabucket-ducklake/raw/mitma/20220101_Viajes_distritos.csv.gz', delim='|', header=True) LIMIT 0").df()
        print("COLUMNAS EN CSV:")
        for col in df.columns:
            print(f"- {col}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_csv()
