-- SILVER LAYER DDL - VERSION RAPIDA CON INNER JOINS

-- DIMENSIONES
CREATE TABLE IF NOT EXISTS silver_dim_zonas (
    zone_id VARCHAR PRIMARY KEY,
    zone_name VARCHAR NOT NULL,
    zone_level VARCHAR NOT NULL,
    provincia_code VARCHAR
);

CREATE TABLE IF NOT EXISTS silver_dim_calendario (
    fecha DATE PRIMARY KEY,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_semana VARCHAR NOT NULL,
    es_laborable BOOLEAN NOT NULL,
    es_festivo_nacional BOOLEAN NOT NULL,
    es_fin_de_semana BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS silver_dim_zona_jerarquia (
    jerarquia_id INTEGER PRIMARY KEY,
    distrito_id VARCHAR NOT NULL,
    municipio_id VARCHAR NOT NULL,
    gau_id VARCHAR NOT NULL
);

-- HECHOS
CREATE TABLE IF NOT EXISTS silver_fact_viajes (
    viaje_id BIGINT PRIMARY KEY,
    fecha DATE NOT NULL,
    hora INTEGER NOT NULL,
    origen_zone_id VARCHAR NOT NULL,
    destino_zone_id VARCHAR NOT NULL,
    edad VARCHAR,
    sexo VARCHAR,
    viajes DOUBLE NOT NULL,
    viajes_km DOUBLE
);

CREATE TABLE IF NOT EXISTS silver_fact_personas (
    persona_id BIGINT PRIMARY KEY,
    fecha DATE NOT NULL,
    zona_pernoctacion_id VARCHAR NOT NULL,
    edad VARCHAR,
    sexo VARCHAR,
    personas DOUBLE NOT NULL
);

-- POBLAR DIMENSIONES
INSERT INTO
    silver_dim_zonas
SELECT
    z.id,
    MAX(z.name),
    MAX(z.zone_type),
    CASE
        WHEN LENGTH(z.id) >= 2 THEN LEFT(z.id, 2)
        ELSE NULL
    END
FROM bronze_db.bronze_ine_zones z
WHERE
    z.id IS NOT NULL
    AND z.id != ''
GROUP BY
    z.id;

INSERT INTO silver_dim_calendario
SELECT DISTINCT 
    strptime(fecha, '%Y%m%d')::DATE, 
    CAST(LEFT(fecha, 4) AS INTEGER), 
    CAST(SUBSTRING(fecha, 5, 2) AS INTEGER), 
    CAST(RIGHT(fecha, 2) AS INTEGER), 
    dia_semana, 
    CASE WHEN tipo_dia = 'laborable' THEN TRUE ELSE FALSE END, 
    CASE WHEN es_festivo_nacional = 1 THEN TRUE ELSE FALSE END, 
    CASE WHEN tipo_dia = 'fin_de_semana' THEN TRUE ELSE FALSE END 
FROM bronze_db.bronze_calendario_laboral 
WHERE zona_provincia IS NULL;

INSERT INTO
    silver_dim_zona_jerarquia
SELECT ROW_NUMBER() OVER (), r.distrito_mitma, r.municipio_mitma, r.gau_mitma
FROM
    bronze_db.bronze_ine_relacion r
    INNER JOIN silver_dim_zonas d ON r.distrito_mitma = d.zone_id
    INNER JOIN silver_dim_zonas m ON r.municipio_mitma = m.zone_id
    INNER JOIN silver_dim_zonas g ON r.gau_mitma = g.zone_id;

-- POBLAR HECHOS (INNER JOINS - MAS RAPIDO)
INSERT INTO silver_fact_viajes
SELECT 
    ROW_NUMBER() OVER (), 
    strptime(v.fecha, '%Y%m%d')::DATE, 
    CAST(RIGHT(v.periodo, 2) AS INTEGER), 
    v.origen, 
    v.destino, 
    v.edad, 
    v.sexo, 
    v.viajes, 
    v.viajes_km 
FROM bronze_db.bronze_mitma_viajes v 
INNER JOIN silver_dim_zonas zo ON v.origen = zo.zone_id 
INNER JOIN silver_dim_zonas zd ON v.destino = zd.zone_id 
INNER JOIN silver_dim_calendario c ON strptime(v.fecha, '%Y%m%d')::DATE = c.fecha 
WHERE v.viajes > 0;

INSERT INTO silver_fact_personas
SELECT 
    ROW_NUMBER() OVER (), 
    strptime(p.fecha, '%Y%m%d')::DATE, 
    p.zona_pernoctacion, 
    p.edad, 
    p.sexo, 
    p.personas 
FROM bronze_db.bronze_mitma_personas p 
INNER JOIN silver_dim_zonas z ON p.zona_pernoctacion = z.zone_id 
INNER JOIN silver_dim_calendario c ON strptime(p.fecha, '%Y%m%d')::DATE = c.fecha 
WHERE p.personas > 0;

-- INDICES
CREATE INDEX IF NOT EXISTS idx_dim_zonas_level ON silver_dim_zonas (zone_level);

CREATE INDEX IF NOT EXISTS idx_fact_viajes_fecha ON silver_fact_viajes (fecha);

CREATE INDEX IF NOT EXISTS idx_fact_viajes_origen ON silver_fact_viajes (origen_zone_id);

CREATE INDEX IF NOT EXISTS idx_fact_viajes_destino ON silver_fact_viajes (destino_zone_id);

CREATE INDEX IF NOT EXISTS idx_fact_personas_fecha ON silver_fact_personas (fecha);

CREATE INDEX IF NOT EXISTS idx_fact_personas_zona ON silver_fact_personas (zona_pernoctacion_id);