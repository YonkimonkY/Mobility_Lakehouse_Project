"""
Script de verificación completa de Bronze y Silver en DuckLake S3
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
    print("VERIFICACIÓN COMPLETA - BRONZE & SILVER (DuckLake S3)")
    print("=" * 70)
    
    con = get_db_connection()
    

    # BRONZE LAYER
    print("\n" + "=" * 70)
    print("CAPA BRONZE")
    print("=" * 70)
    
    try:
        total_bronze = con.execute("SELECT COUNT(*) FROM bronze_mitma_viajes").fetchone()[0]
        print(f"\nbronze_mitma_viajes:")
        print(f"   Total registros: {total_bronze:,}")
        
        # Fechas únicas
        fechas_bronze = con.execute("""
            SELECT 
                COUNT(DISTINCT fecha) as fechas_unicas,
                MIN(fecha) as primera,
                MAX(fecha) as ultima
            FROM bronze_mitma_viajes
        """).fetchone()
        
        print(f"   Fechas únicas: {fechas_bronze[0]}")
        print(f"   Rango: {fechas_bronze[1]} - {fechas_bronze[2]}")
        
        if fechas_bronze[0] == 365:
            print(" Perfecto 365 días de 2022")
        else:
            print(f" Faltan {365 - fechas_bronze[0]} días")
            
    except Exception as e:
        print(f"   Error: {e}")
    
    # SILVER LAYER - DIMENSIONES
    print("\n" + "=" * 70)
    print("CAPA SILVER - DIMENSIONES")
    print("=" * 70)
    
    dims = [
        ('silver_dim_zonas', 'Zonas'),
        ('silver_dim_calendario', 'Calendario'),
        ('silver_dim_zona_jerarquia', 'Jerarquía'),
        ('silver_dim_zona_atributos', 'Atributos')
    ]
    
    for tabla, nombre in dims:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
            print(f"   {nombre:15} ({tabla}): {count:,} registros")
        except Exception as e:
            print(f"   {nombre:15}: Error - {str(e)[:50]}")
    
    # SILVER LAYER 
    print("\n" + "=" * 70)
    print("CAPA SILVER - HECHOS (FACTS)")
    print("=" * 70)
    
    # 
    try:
        viajes_count = con.execute("SELECT COUNT(*) FROM silver_fact_viajes").fetchone()[0]
        viajes_sum = con.execute("SELECT SUM(viajes) FROM silver_fact_viajes").fetchone()[0]
        
        print(f"\nsilver_fact_viajes:")
        print(f"   Filas: {viajes_count:,}")
        print(f"   Total viajes: {viajes_sum:,.0f}" if viajes_sum else "   Total viajes: 0")
        
        if viajes_count > 0:
            # Fechas en Silver
            fechas_silver = con.execute("""
                SELECT 
                    COUNT(DISTINCT fecha) as fechas_unicas,
                    MIN(fecha) as primera,
                    MAX(fecha) as ultima
                FROM silver_fact_viajes
            """).fetchone()
            
            print(f"   Fechas únicas: {fechas_silver[0]}")
            print(f"   Rango: {fechas_silver[1]} - {fechas_silver[2]}")
            
            if fechas_silver[0] == 365:
                print(" Perfecto 365 días procesados")
            else:
                print(f" Solo {fechas_silver[0]} días procesados")
        else:
            print(" Tabla vacía - ejecutar process_silver_s3.py")
            
    except Exception as e:
        print(f" Error: {e}")
    
    # Personas
    try:
        personas_count = con.execute("SELECT COUNT(*) FROM silver_fact_personas").fetchone()[0]
        print(f"\nsilver_fact_personas:")
        print(f"   Filas: {personas_count:,}")
        
        if personas_count == 0:
            print(" Tabla vacía (normal si no se procesó)")
    except Exception as e:
        print(f" Error: {e}")
    
    # CALIDAD DE DATOS
    print("\n" + "=" * 70)
    print("CALIDAD DE DATOS")
    print("=" * 70)
    
    if viajes_count > 0:
        # Top 5 rutas
        print("\nTop 5 rutas con más viajes:")
        top_rutas = con.execute("""
            SELECT 
                origen_zone_id,
                destino_zone_id,
                SUM(viajes) as total_viajes
            FROM silver_fact_viajes
            GROUP BY origen_zone_id, destino_zone_id
            ORDER BY total_viajes DESC
            LIMIT 5
        """).fetchall()
        
        for i, (origen, destino, total) in enumerate(top_rutas, 1):
            print(f"   {i}. {origen} → {destino}: {total:,.0f} viajes")
        
        print("\n Verificación de integridad:")
        nulls = con.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE fecha IS NULL) as null_fecha,
                COUNT(*) FILTER (WHERE origen_zone_id IS NULL) as null_origen,
                COUNT(*) FILTER (WHERE destino_zone_id IS NULL) as null_destino,
                COUNT(*) FILTER (WHERE viajes IS NULL) as null_viajes
            FROM silver_fact_viajes
        """).fetchone()
        
        if sum(nulls) == 0:
            print(" Sin valores NULL en columnas críticas")
        else:
            print(f" NULLs encontrados: fecha={nulls[0]}, origen={nulls[1]}, destino={nulls[2]}, viajes={nulls[3]}")
    
    # RESUMEN FINAL
    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    
    status = []
    
    if fechas_bronze[0] == 365:
        status.append("Bronze: 365 días completos")
    else:
        status.append(f"Bronze: Solo {fechas_bronze[0]} días")
    
    if viajes_count > 0 and fechas_silver[0] == 365:
        status.append("Silver: 365 días procesados")
    elif viajes_count > 0:
        status.append(f"Silver: Solo {fechas_silver[0]} días")
    else:
        status.append("Silver: Vacío (ejecutar process_silver_s3.py)")
    
    for s in status:
        print(f"   {s}")
    
    if len([s for s in status if 'Perfecto' in s]) == 2:
        print("\n¡TODO PERFECTO! El lakehouse está completo")
    else:
        print("\n  Hay tareas pendientes")
    
    print("=" * 70)
    con.close()

if __name__ == "__main__":
    main()
