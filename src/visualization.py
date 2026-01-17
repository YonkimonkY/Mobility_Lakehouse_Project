import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkt
from keplergl import KeplerGl
import os

# Configuración
GOLD_DB = 'data/processed/gold.duckdb'
SILVER_DB = 'data/processed/silver.duckdb'
BRONZE_DB = 'data/processed/bronze.duckdb'
OUTPUT_FILE = 'reports/mapa_movilidad.html'

def generate_map():
    print(" Generando mapa de movilidad con Kepler.gl...")
    
    # 1. Conectar a DuckDB
    con = duckdb.connect(GOLD_DB)
    
    # Cargar extensión espacial
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    
    # Atachamos Silver y Bronze
    con.execute(f"ATTACH '{SILVER_DB}' AS silver_db (READ_ONLY)")
    con.execute(f"ATTACH '{BRONZE_DB}' AS bronze_db (READ_ONLY)")
    
    # ---------------------------------------------------------
    # CAPA 1: ARCOS (Flujos Origen-Destino con Ratio Modelo de Gravedad)
    # ---------------------------------------------------------
    print("   1. Cargando flujos OD con ratio teórico/observado...")
    df_flows = con.execute("""
        SELECT 
            origen_zone_name,
            destino_zone_name,
            origen_lat,
            origen_lon,
            destino_lat,
            destino_lon,
            observed_trips as viajes,
            theoretical_trips as viajes_teoricos,
            ratio_obs_theo as ratio,
            distance_km as distancia
        FROM gold_gravity_model
        WHERE observed_trips > 100 
          AND theoretical_trips IS NOT NULL
        ORDER BY observed_trips DESC
        LIMIT 10000
    """).df()

    # Reproject Flows (EPSG:25830 -> EPSG:4326)
    print("      Reproyectando flujos a WGS84...")
    # Origen
    gdf_o = gpd.GeoDataFrame(
        df_flows, 
        geometry=gpd.points_from_xy(df_flows.origen_lon, df_flows.origen_lat),
        crs="EPSG:25830"
    ).to_crs("EPSG:4326")
    df_flows['origen_lon'] = gdf_o.geometry.x
    df_flows['origen_lat'] = gdf_o.geometry.y
    
    # Destino
    gdf_d = gpd.GeoDataFrame(
        df_flows, 
        geometry=gpd.points_from_xy(df_flows.destino_lon, df_flows.destino_lat),
        crs="EPSG:25830"
    ).to_crs("EPSG:4326")
    df_flows['destino_lon'] = gdf_d.geometry.x
    df_flows['destino_lat'] = gdf_d.geometry.y

    # Clean flows
    df_flows = df_flows.dropna(subset=['origen_lat', 'origen_lon', 'destino_lat', 'destino_lon'])

    # ---------------------------------------------------------
    # CAPA 2: POLÍGONOS (Distritos con densidad de viajes)
    # ---------------------------------------------------------
    print("   2. Cargando geometrías de zonas con densidad...")
    df_zones = con.execute("""
        SELECT 
            z.zone_id,
            z.zone_name,
            ST_AsText(zg.geom) as geom,
            COALESCE(densidad.total_viajes, 0) as total_viajes,
            COALESCE(densidad.produccion, 0) as produccion,
            COALESCE(densidad.atraccion, 0) as atraccion
        FROM silver_db.silver_dim_zonas z
        JOIN bronze_db.bronze_zonificacion zg ON z.zone_id = zg.ID
        LEFT JOIN (
            SELECT 
                origen_zone_id,
                SUM(observed_trips) as total_viajes,
                SUM(observed_trips) as produccion,
                0 as atraccion
            FROM gold_gravity_model
            GROUP BY origen_zone_id
            
            UNION ALL
            
            SELECT 
                destino_zone_id as origen_zone_id,
                0 as total_viajes,
                0 as produccion,
                SUM(observed_trips) as atraccion
            FROM gold_gravity_model
            GROUP BY destino_zone_id
        ) densidad ON z.zone_id = densidad.origen_zone_id
        WHERE zg.geom IS NOT NULL
    """).df()
    
    # Agregar producción y atracción por zona
    df_zones = df_zones.groupby(['zone_id', 'zone_name', 'geom'], as_index=False).agg({
        'total_viajes': 'sum',
        'produccion': 'sum',
        'atraccion': 'sum'
    })
    
    # Reproject Zones (EPSG:25830 -> EPSG:4326)
    print("      Reproyectando zonas a WGS84...")
    df_zones['geometry'] = df_zones['geom'].apply(wkt.loads)
    gdf_zones = gpd.GeoDataFrame(df_zones, geometry='geometry', crs="EPSG:25830")
    gdf_zones = gdf_zones.to_crs("EPSG:4326")
    
    # Filter invalid or empty geometries
    gdf_zones = gdf_zones[~gdf_zones.is_empty & gdf_zones.is_valid]
    
    # Convert back to WKT for Kepler
    df_zones = pd.DataFrame(gdf_zones.drop(columns='geom'))
    df_zones['geom'] = gdf_zones.geometry.to_wkt()
    df_zones = df_zones.drop(columns=['geometry'])

    # ---------------------------------------------------------
    # CAPA 3: HEATMAP (Puntos de centroides con densidad)
    # ---------------------------------------------------------
    print("   3. Creando heatmap de densidad...")
    df_heatmap = con.execute("""
        SELECT 
            z.zone_name,
            za.centroid_lat as lat,
            za.centroid_lon as lon,
            COALESCE(SUM(gm.observed_trips), 0) as intensidad
        FROM silver_db.silver_dim_zonas z
        JOIN silver_db.silver_dim_zona_atributos za ON z.zone_id = za.zone_id
        LEFT JOIN gold_gravity_model gm ON z.zone_id = gm.origen_zone_id
        WHERE za.centroid_lat IS NOT NULL
        GROUP BY z.zone_name, za.centroid_lat, za.centroid_lon
        HAVING SUM(gm.observed_trips) > 0
    """).df()
    
    con.close()

    # Reproject heatmap
    if not df_heatmap.empty:
        gdf_heat = gpd.GeoDataFrame(
            df_heatmap,
            geometry=gpd.points_from_xy(df_heatmap.lon, df_heatmap.lat),
            crs="EPSG:25830"
        ).to_crs("EPSG:4326")
        df_heatmap['lon'] = gdf_heat.geometry.x
        df_heatmap['lat'] = gdf_heat.geometry.y

    # ---------------------------------------------------------
    # CREAR MAPA KEPLER CON 3 CAPAS
    # ---------------------------------------------------------
    print("   4. Renderizando mapa...")
    
    map_1 = KeplerGl(height=800)
    
    # Añadir capas
    map_1.add_data(data=df_flows, name='Flujos OD con Ratio')
    map_1.add_data(data=df_zones, name='Distritos con Densidad')
    if not df_heatmap.empty:
        map_1.add_data(data=df_heatmap, name='Heatmap Densidad')

    # Guardar HTML
    os.makedirs('reports', exist_ok=True)
    map_1.save_to_html(file_name=OUTPUT_FILE)
    
    print(f" Mapa guardado en: {OUTPUT_FILE}")
    print(f"    {len(df_flows):,} flujos | {len(df_zones):,} zonas | {len(df_heatmap):,} puntos heatmap")
    print("   Abre este archivo en tu navegador para interactuar.")
    
    # Mostrar estadísticas del ratio
    print("\n Estadísticas del Modelo de Gravedad:")
    print(f"   Ratio promedio: {df_flows['ratio'].mean():.2f}")
    print(f"   Ratio máximo: {df_flows['ratio'].max():.2f} (más viajes de lo esperado)")
    print(f"   Ratio mínimo: {df_flows['ratio'].min():.2f}")
    print(f"   Flujos con ratio > 2: {(df_flows['ratio'] > 2).sum():,} ({(df_flows['ratio'] > 2).sum() / len(df_flows) * 100:.1f}%)")

if __name__ == "__main__":
    generate_map()