import duckdb
import time

SILVER_DB = 'data/processed/silver.duckdb'
BRONZE_DB = 'data/processed/bronze.duckdb'
SQL_FILE = 'src/sql/silver.sql'

def process_silver():
    print("=" * 70)
    print("PROCESANDO SILVER LAYER - Modelo Dimensional")
    print("=" * 70)
    
    start_time = time.time()
    
    # Conectar a Silver
    con = duckdb.connect(SILVER_DB)
    
    # Attach Bronze
    print(f"\n1. Conectando a Bronze: {BRONZE_DB}")
    con.execute(f"ATTACH '{BRONZE_DB}' AS bronze_db (READ_ONLY)")
    print("    Bronze attached")
    
    # Cargar extensión espacial (necesaria para centroides)
    print("\n2. Cargando extensión espacial...")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    print("    Extensión espacial cargada")
    
    # Leer y ejecutar SQL
    print(f"\n3. Ejecutando transformaciones: {SQL_FILE}")
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    print("   Esto puede tardar varios minutos...")
    con.execute(sql)
    print("    Transformaciones completadas")
    
    # Calcular centroides 
    print("\n4. Calculando centroides ...")
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver_dim_zona_atributos (
                zone_id VARCHAR PRIMARY KEY,
                centroid_lat DOUBLE,
                centroid_lon DOUBLE
            )
        """)
        
        con.execute("""
            INSERT INTO silver_dim_zona_atributos
            SELECT 
                z.zone_id,
                ST_Y(ST_Centroid(zg.geom)) as centroid_lat,
                ST_X(ST_Centroid(zg.geom)) as centroid_lon
            FROM silver_dim_zonas z
            LEFT JOIN bronze_db.bronze_zonificacion zg ON z.zone_id = zg.ID
        """)
        print("    Centroides calculados")
    except Exception as e:
        print(f"     Centroides no calculados: {str(e)[:50]}")
    
    # Estadisticas
    print("\n" + "=" * 70)
    print("SILVER LAYER - ESTADISTICAS")
    print("=" * 70)
    
    print("\nDIMENSIONES:")
    zonas = con.execute("SELECT COUNT(*) FROM silver_dim_zonas").fetchone()[0]
    print(f"   dim_zonas: {zonas:,} zonas")
    
    fechas = con.execute("SELECT COUNT(*) FROM silver_dim_calendario").fetchone()[0]
    print(f"   dim_calendario: {fechas:,} fechas")
    
    jerarquia = con.execute("SELECT COUNT(*) FROM silver_dim_zona_jerarquia").fetchone()[0]
    print(f"   dim_zona_jerarquia: {jerarquia:,} relaciones")
    
    try:
        atributos = con.execute("SELECT COUNT(*) FROM silver_dim_zona_atributos").fetchone()[0]
        con_geo = con.execute("SELECT COUNT(*) FROM silver_dim_zona_atributos WHERE centroid_lat IS NOT NULL").fetchone()[0]
        print(f"   dim_zona_atributos: {atributos:,} zonas ({con_geo:,} con centroides)")
    except:
        print(f"    dim_zona_atributos: No creada")
    
    print("\nHECHOS:")
    viajes_count = con.execute("SELECT COUNT(*) FROM silver_fact_viajes").fetchone()[0]
    viajes_total = con.execute("SELECT SUM(viajes) FROM silver_fact_viajes").fetchone()[0]
    print(f"   fact_viajes: {viajes_count:,} registros ({viajes_total:,.0f} viajes)")
    
    personas_count = con.execute("SELECT COUNT(*) FROM silver_fact_personas").fetchone()[0]
    personas_total = con.execute("SELECT SUM(personas) FROM silver_fact_personas").fetchone()[0]
    print(f"   fact_personas: {personas_count:,} registros ({personas_total:,.0f} personas)")
    
    # Validacion de integridad
    print("\n" + "=" * 70)
    print("VALIDACION DE INTEGRIDAD REFERENCIAL")
    print("=" * 70)
    
    # Verificar viajes huerfanos
    orphan_viajes = con.execute("""
        SELECT COUNT(*) FROM silver_fact_viajes v
        WHERE NOT EXISTS (SELECT 1 FROM silver_dim_zonas z WHERE z.zone_id = v.origen_zone_id)
           OR NOT EXISTS (SELECT 1 FROM silver_dim_zonas z WHERE z.zone_id = v.destino_zone_id)
    """).fetchone()[0]
    
    if orphan_viajes == 0:
        print("   Viajes huerfanos: 0 (integridad OK)")
    else:
        print(f"    Viajes huerfanos: {orphan_viajes:,}")
    
    # Top zonas
    print("\n" + "=" * 70)
    print("TOP 5 ZONAS POR VIAJES")
    print("=" * 70)
    
    top_zonas = con.execute("""
        SELECT 
            z.zone_name,
            z.zone_level,
            SUM(v.viajes) as total_viajes
        FROM silver_fact_viajes v
        JOIN silver_dim_zonas z ON v.origen_zone_id = z.zone_id
        GROUP BY z.zone_name, z.zone_level
        ORDER BY total_viajes DESC
        LIMIT 5
    """).fetchall()
    
    for i, (nombre, nivel, total) in enumerate(top_zonas, 1):
        print(f"  {i}. {nombre} ({nivel}): {total:,.0f} viajes")
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f" Silver procesado en {elapsed:.1f} segundos")
    print("=" * 70)
    
    con.close()

if __name__ == "__main__":
    process_silver()
