import duckdb
import time

GOLD_DB = 'data/processed/gold.duckdb'
SILVER_DB = 'data/processed/silver.duckdb'
SQL_FILE = 'src/sql/gold.sql'

def process_gold():
    print("=" * 70)
    print("PROCESANDO GOLD LAYER - Tablas Analiticas")
    print("=" * 70)
    
    start_time = time.time()
    
    # Conectar a Gold
    con = duckdb.connect(GOLD_DB)
    
    # Attach Silver
    print(f"\n1. Conectando a Silver: {SILVER_DB}")
    con.execute(f"ATTACH '{SILVER_DB}' AS silver_db (READ_ONLY)")
    print("    Silver attached")
    
    # Leer y ejecutar SQL
    print(f"\n2. Ejecutando agregaciones: {SQL_FILE}")
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    print("   Esto puede tardar varios minutos...")
    con.execute(sql)
    print("    Agregaciones completadas")
    
    # Estadisticas
    print("\n" + "=" * 70)
    print("GOLD LAYER - ESTADISTICAS")
    print("=" * 70)
    
    print("\nTABLAS ANALITICAS:")
    
    od_matrix = con.execute("SELECT COUNT(*) FROM gold_od_matrix_top").fetchone()[0]
    print(f"   od_matrix_top: {od_matrix:,} flujos OD")
    
    hourly = con.execute("SELECT COUNT(*) FROM gold_hourly_patterns").fetchone()[0]
    print(f"   hourly_patterns: {hourly:,} horas")
    
    day_type = con.execute("SELECT COUNT(*) FROM gold_mobility_by_day_type").fetchone()[0]
    print(f"   mobility_by_day_type: {day_type:,} tipos de dia")
    
    top_zones = con.execute("SELECT COUNT(*) FROM gold_top_zones").fetchone()[0]
    print(f"   top_zones: {top_zones:,} zonas")
    
    gravity = con.execute("SELECT COUNT(*) FROM gold_gravity_model").fetchone()[0]
    with_dist = con.execute("SELECT COUNT(*) FROM gold_gravity_model WHERE distance_km IS NOT NULL").fetchone()[0]
    print(f"   gravity_model: {gravity:,} pares OD ({with_dist:,} con distancia)")
    
    # Insights
    print("\n" + "=" * 70)
    print("INSIGHTS CLAVE")
    print("=" * 70)
    
    # Hora pico
    print("\n PATRONES HORARIOS:")
    hourly_data = con.execute("""
        SELECT hour, total_trips, pct_of_daily_trips
        FROM gold_hourly_patterns
        ORDER BY total_trips DESC
        LIMIT 3
    """).fetchall()
    
    for hora, trips, pct in hourly_data:
        print(f"  {hora:02d}:00 - {trips:,.0f} viajes ({pct:.1f}% del total)")
    
    # Top flujos OD
    print("\n TOP 5 FLUJOS ORIGEN-DESTINO:")
    top_od = con.execute("""
        SELECT origen_zone_name, destino_zone_name, total_trips
        FROM gold_od_matrix_top
        ORDER BY od_id
        LIMIT 5
    """).fetchall()
    
    for i, (origen, destino, trips) in enumerate(top_od, 1):
        print(f"  {i}. {origen} → {destino}: {trips:,.0f} viajes")
    
    # Movilidad por tipo de dia
    print("\n MOVILIDAD POR TIPO DE DIA:")
    day_types = con.execute("""
        SELECT day_type, avg_trips_per_day
        FROM gold_mobility_by_day_type
        ORDER BY avg_trips_per_day DESC
    """).fetchall()
    
    for day_type, avg_trips in day_types:
        print(f"  {day_type}: {avg_trips:,.0f} viajes/dia")
    
    # Top zonas
    print("\n TOP 5 ZONAS POR VIAJES (ORIGEN):")
    top_zones_data = con.execute("""
        SELECT zone_name, zone_level, total_trips
        FROM gold_top_zones
        ORDER BY rank
        LIMIT 5
    """).fetchall()
    
    for nombre, nivel, valor in top_zones_data:
        print(f"  {nombre} ({nivel}): {valor:,.0f} viajes")
    
    # Modelo de gravedad
    print("\n MODELO DE GRAVEDAD :")
    gravity_data = con.execute("""
        SELECT 
            origen_zone_name, 
            destino_zone_name, 
            observed_trips, 
            theoretical_trips,
            ratio_obs_theo,
            distance_km
        FROM gold_gravity_model
        WHERE distance_km IS NOT NULL AND distance_km > 0
        ORDER BY observed_trips DESC
        LIMIT 5
    """).fetchall()
    
    for origen, destino, obs, theo, ratio, dist in gravity_data:
        print(f"  {origen} → {destino}:")
        print(f"     Observado: {obs:,.0f} | Teorico: {theo:,.0f} | Ratio: {ratio:.2f} | Dist: {dist:.1f} km")
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f" Gold procesado en {elapsed:.1f} segundos")
    print("=" * 70)
    
    con.close()

if __name__ == "__main__":
    process_gold()
