import duckdb
import os
import glob

# Configuration
DB_PATH = 'data/processed/bronze.duckdb'
RAW_DATA_PATH = 'data/raw'
SQL_PATH = 'src/sql/bronze.sql'

def ingest_bronze():
    con = duckdb.connect(DB_PATH)
    
    
    print("Creating Bronze tables...")
    with open(SQL_PATH, 'r') as f:
        con.execute(f.read())
        
    
    print("Ingesting MITMA Viajes...")
    viajes_files = glob.glob(os.path.join(RAW_DATA_PATH, 'mitma', '*Viajes_distritos.csv.gz'))
    for file in viajes_files:
        print(f"Loading {file}...")
       
        con.execute(f"""
            INSERT INTO bronze_mitma_viajes 
            SELECT fecha, periodo, origen, destino, actividad_origen, actividad_destino, 
                   residencia, edad, sexo, viajes, viajes_km, '{os.path.basename(file)}'
            FROM read_csv('{file}', delim='|', header=True, auto_detect=True)
        """)

    
    print("Ingesting MITMA Personas...")
    personas_files = glob.glob(os.path.join(RAW_DATA_PATH, 'mitma', '*Personas_dia_distritos.csv.gz'))
    for file in personas_files:
        print(f"Loading {file}...")
        con.execute(f"""
            INSERT INTO bronze_mitma_personas
            SELECT fecha, zona_pernoctacion, edad, sexo, numero_viajes, personas, '{os.path.basename(file)}'
            FROM read_csv('{file}', delim='|', header=True, auto_detect=True)
        """)

    
    print("Ingesting INE Zones...")
    
    districts_file = os.path.join(RAW_DATA_PATH, 'ine/zones/nombres_distritos.csv')
    if os.path.exists(districts_file):
        con.execute(f"""
            INSERT INTO bronze_ine_zones
            SELECT ID, name, 'district'
            FROM read_csv('{districts_file}', delim='|', header=True)
        """)
    
    
    munis_file = os.path.join(RAW_DATA_PATH, 'ine/zones/nombres_municipios.csv')
    if os.path.exists(munis_file):
        con.execute(f"""
            INSERT INTO bronze_ine_zones
            SELECT ID, name, 'municipality'
            FROM read_csv('{munis_file}', delim='|', header=True)
        """)

    
    print("Ingesting INE Relation (Jerárquica: distritos -> municipios -> GAU)...")
    rel_file = os.path.join(RAW_DATA_PATH, 'ine/zones/relacion_ine_zonificacionMitma.csv')
    if os.path.exists(rel_file):
        con.execute(f"""
            INSERT INTO bronze_ine_relacion
            SELECT *
            FROM read_csv('{rel_file}', delim='|', header=True)
        """)
    
    
    print("Extracting GAUs from relacion table...")
    con.execute("""
        INSERT INTO bronze_ine_zones
        SELECT DISTINCT gau_mitma as id, gau_mitma as name, 'gau' as zone_type
        FROM bronze_ine_relacion
        WHERE gau_mitma IS NOT NULL AND gau_mitma != ''
    """)

    
    print("Ingesting INE Demographics...")
    demo_file = os.path.join(RAW_DATA_PATH, 'ine/demographics/demografico.csv')
    if os.path.exists(demo_file):
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze_ine_demographics AS
            SELECT *
            FROM read_csv('{demo_file}', delim=';', header=True, auto_detect=True)
        """)
    
   
    print("Ingesting INE Economic...")
    econ_file = os.path.join(RAW_DATA_PATH, 'ine/economic/economica.csv')
    if os.path.exists(econ_file):
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze_ine_economic AS
            SELECT *
            FROM read_csv('{econ_file}', delim=';', header=True, auto_detect=True)
        """)

    
    print("Ingesting Calendario Laboral...")
    cal_file = 'data/raw/calendario/calendario_laboral.csv'
    if os.path.exists(cal_file):
        con.execute(f"""
            INSERT INTO bronze_calendario_laboral
            SELECT *
            FROM read_csv('{cal_file}', delim='|', header=True, auto_detect=True)
        """)
        print(f"  Loaded {con.execute('SELECT COUNT(*) FROM bronze_calendario_laboral').fetchone()[0]:,} days")
    else:
        print(f"   Calendario file not found: {cal_file}")

    
    print("Ingesting Zonificación (Shapefile)...")
    # Cargar extensión espacial de DuckDB
    try:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        
        shp_file = os.path.abspath(os.path.join(RAW_DATA_PATH, 'mitma/zonificacion/zonificacion_matriz_tiempos.shp')).replace('\\', '/')
        print(f"  Reading: {shp_file}")
        
        if os.path.exists(shp_file.replace('/', '\\')): 
            con.execute(f"""
                CREATE OR REPLACE TABLE bronze_zonificacion AS
                SELECT * 
                FROM ST_Read('{shp_file}')
            """)
            count = con.execute("SELECT COUNT(*) FROM bronze_zonificacion").fetchone()[0]
            print(f"  Loaded {count:,} zones with geometry")
        else:
            print(f"   Shapefile not found: {shp_file}")
    except Exception as e:
        print(f"   Error loading shapefile: {e}")

    print("Bronze ingestion complete.")
    print("\nSummary:")
    print(f"  - MITMA Viajes: {con.execute('SELECT COUNT(*) FROM bronze_mitma_viajes').fetchone()[0]:,} records")
    print(f"  - MITMA Personas: {con.execute('SELECT COUNT(*) FROM bronze_mitma_personas').fetchone()[0]:,} records")
    print(f"  - INE Zones: {con.execute('SELECT COUNT(*) FROM bronze_ine_zones').fetchone()[0]:,} records")
    print(f"  - INE Relacion (Jerárquica): {con.execute('SELECT COUNT(*) FROM bronze_ine_relacion').fetchone()[0]:,} records")
    print(f"  - INE Demographics: {con.execute('SELECT COUNT(*) FROM bronze_ine_demographics').fetchone()[0]:,} records")
    print(f"  - INE Economic: {con.execute('SELECT COUNT(*) FROM bronze_ine_economic').fetchone()[0]:,} records")
    
    # New tables
    try:
        print(f"  - Calendario Laboral: {con.execute('SELECT COUNT(*) FROM bronze_calendario_laboral').fetchone()[0]:,} records")
    except:
        print(f"  - Calendario Laboral: 0 records (not loaded)")
    
    try:
        print(f"  - Zonificación: {con.execute('SELECT COUNT(*) FROM bronze_zonificacion').fetchone()[0]:,} records")
    except:
        print(f"  - Zonificación: 0 records (not loaded)")
    
    con.close()

if __name__ == "__main__":
    ingest_bronze()
