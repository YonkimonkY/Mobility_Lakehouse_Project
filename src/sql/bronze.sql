-- ============
-- TABLAS MITMA
-- ============

CREATE TABLE IF NOT EXISTS bronze_mitma_viajes (
    fecha VARCHAR,
    periodo VARCHAR,
    origen VARCHAR,
    destino VARCHAR,
    actividad_origen VARCHAR,
    actividad_destino VARCHAR,
    residencia VARCHAR,
    edad VARCHAR,
    sexo VARCHAR,
    viajes DOUBLE,
    viajes_km DOUBLE,
    ingestion_file VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_mitma_personas (
    fecha VARCHAR,
    zona_pernoctacion VARCHAR,
    edad VARCHAR,
    sexo VARCHAR,
    numero_viajes VARCHAR,
    personas DOUBLE,
    ingestion_file VARCHAR
);

-- ===========
-- TABLAS INE
-- ===========

CREATE TABLE IF NOT EXISTS bronze_ine_zones (
    id VARCHAR,
    name VARCHAR,
    zone_type VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_ine_relacion (
    seccion_ine VARCHAR,
    distrito_ine VARCHAR,
    municipio_ine VARCHAR,
    distrito_mitma VARCHAR,
    municipio_mitma VARCHAR,
    gau_mitma VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_ine_demographics (
    col1 VARCHAR,
    col2 VARCHAR,
    col3 VARCHAR,
    col4 VARCHAR,
    col5 VARCHAR,
    col6 VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_ine_economic (
    col1 VARCHAR,
    col2 VARCHAR,
    col3 VARCHAR,
    col4 VARCHAR,
    col5 VARCHAR,
    col6 VARCHAR
);

-- ====================
-- CALENDARIO LABORAL
-- ====================

CREATE TABLE IF NOT EXISTS bronze_calendario_laboral (
    fecha VARCHAR,
    zona_provincia VARCHAR,
    tipo_dia VARCHAR,
    dia_semana VARCHAR,
    es_festivo_nacional INTEGER,
    es_festivo_autonomico INTEGER,
    nombre_festivo VARCHAR,
    mes INTEGER,
    anio INTEGER
);

-- ==============
-- ZONIFICACIÓN
-- ==============

CREATE TABLE IF NOT EXISTS bronze_zonificacion (
    zone_id VARCHAR,
    zone_name VARCHAR,
    zone_type VARCHAR,
    geometry_wkt VARCHAR,
    centroid_lat DOUBLE,
    centroid_lon DOUBLE
);