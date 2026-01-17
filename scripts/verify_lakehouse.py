"""
Script RÁPIDO de verificación (sin queries pesadas)
"""
import duckdb
import os
import boto3

os.environ.setdefault('USUARIO_POSTGRES', '')
os.environ.setdefault('CONTR_POSTGRES', '')

def get_db_connection():
    con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
    
    for ext in ['ducklake', 'postgres', 'aws']:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    
    session = boto3.Session()
    creds = session.get_credentials()
    if creds:
        frozen = creds.get_frozen_credentials()
        region = session.region_name or 'eu-central-1'
        con.execute(f"""
            CREATE OR REPLACE SECRET secret_s3 (
                TYPE S3,
                KEY_ID '{frozen.access_key}',
                SECRET '{frozen.secret_key}',
                REGION '{region}'
            )
        """)
    
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE postgres,
            HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech',
            PORT 5432,
            DATABASE neondb,
            USER '{os.environ["USUARIO_POSTGRES"]}',
            PASSWORD '{os.environ["CONTR_POSTGRES"]}'
        )
    """)
    
    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
        )
    """)
    
    con.execute("ATTACH 'ducklake:secreto_ducklake' AS bdet_upv (DATA_PATH 's3://bigdatabucket-ducklake/ducklake/')")
    con.execute("USE bdet_upv")
    
    return con

def main():
    print("=" * 70)
    print("VERIFICACIÓN RÁPIDA - LAKEHOUSE S3")
    print("=" * 70)
    
    con = get_db_connection()
    
    print("\n SILVER - DIMENSIONES:")
    dims = {
        'Zonas': 'silver_dim_zonas',
        'Calendario': 'silver_dim_calendario',
        'Jerarquía': 'silver_dim_zona_jerarquia',
        'Atributos': 'silver_dim_zona_atributos'
    }
    
    for nombre, tabla in dims.items():
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
            status = " " if count > 0 else " "
            print(f"   {status} {nombre:12}: {count:,} registros")
        except:
            print(f"   {nombre:12}: Error")
    
    print("\n SILVER - HECHOS:")

    try:
        viajes_count = con.execute("SELECT COUNT(*) FROM silver_fact_viajes").fetchone()[0]
        viajes_sum = con.execute("SELECT SUM(viajes) FROM silver_fact_viajes").fetchone()[0] if viajes_count > 0 else 0
        
        print(f"   Viajes (filas): {viajes_count:,}")
        print(f"   Viajes (total): {viajes_sum:,.0f}" if viajes_sum else "   Viajes (total): 0")
        
        if viajes_count > 0:
            rango = con.execute("""
                SELECT MIN(fecha), MAX(fecha)
                FROM silver_fact_viajes
            """).fetchone()
            print(f"   Rango fechas: {rango[0]} a {rango[1]}")
            
            dias = con.execute("SELECT COUNT(DISTINCT fecha) FROM silver_fact_viajes").fetchone()[0]
            print(f"   Días únicos: {dias}")
            
            if dias == 365:
                print(" ¡PERFECTO! 365 díasde 2022 procesados")
            else:
                print(f"  Solo {dias} días procesados de 365")
        else:
            print("  VACÍO - Ejecutar process_silver_s3.py")
            
    except Exception as e:
        print(f" Error: {str(e)[:80]}")
    
    
    if viajes_count > 0:
        print("\n Top 5 rutas:")
        try:
            top = con.execute("""
                SELECT origen_zone_id, destino_zone_id, SUM(viajes) as total
                FROM silver_fact_viajes
                GROUP BY origen_zone_id, destino_zone_id
                ORDER BY total DESC
                LIMIT 5
            """).fetchall()
            
            for i, (o, d, t) in enumerate(top, 1):
                print(f"   {i}. {o} → {d}: {t:,.0f} viajes")
        except Exception as e:
            print(f" Error: {str(e)[:80]}")
    
    print("\n" + "=" * 70)
    
    if viajes_count > 0 and dias == 365:
        print(" ¡TODO PERFECTO! Silver completo con 365 días")
    elif viajes_count > 0:
        print(f" Silver tiene datos pero solo {dias}/365 días")
    else:
        print(" Silver vacío - ejecutar: python scripts/process_silver_s3.py")
    
    print("=" * 70)
    con.close()

if __name__ == "__main__":
    main()
