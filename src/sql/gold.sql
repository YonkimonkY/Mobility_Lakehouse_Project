-- GOLD LAYER DDL - VERSION SIMPLIFICADA Y RAPIDA

-- Tabla 1: Top 1000 Flujos OD
CREATE
OR
REPLACE
TABLE gold_od_matrix_top (
    od_id INTEGER PRIMARY KEY,
    origen_zone_id VARCHAR NOT NULL,
    origen_zone_name VARCHAR NOT NULL,
    destino_zone_id VARCHAR NOT NULL,
    destino_zone_name VARCHAR NOT NULL,
    total_trips DOUBLE NOT NULL,
    total_km DOUBLE,
    avg_trips_per_day DOUBLE
);

-- Tabla 2: Patrones Horarios
CREATE
OR
REPLACE
TABLE gold_hourly_patterns (
    hour INTEGER PRIMARY KEY,
    total_trips DOUBLE NOT NULL,
    avg_trip_km DOUBLE,
    pct_of_daily_trips DOUBLE
);

-- Tabla 3: Movilidad por Tipo de Dia
CREATE
OR
REPLACE
TABLE gold_mobility_by_day_type (
    day_type VARCHAR PRIMARY KEY,
    total_trips DOUBLE NOT NULL,
    avg_trips_per_day DOUBLE
);

-- Tabla 4: Top 20 Zonas
CREATE
OR
REPLACE
TABLE gold_top_zones (
    rank INTEGER PRIMARY KEY,
    zone_id VARCHAR,
    zone_name VARCHAR,
    zone_level VARCHAR,
    total_trips DOUBLE
);

-- Tabla 5: Modelo de Gravedad (Top 5000 flujos con distancia)
CREATE
OR
REPLACE
TABLE gold_gravity_model (
    od_pair_id INTEGER PRIMARY KEY,
    origen_zone_id VARCHAR NOT NULL,
    origen_zone_name VARCHAR NOT NULL,
    destino_zone_id VARCHAR NOT NULL,
    destino_zone_name VARCHAR NOT NULL,
    observed_trips DOUBLE NOT NULL,
    theoretical_trips DOUBLE, -- Viajes teoricos calculados con formula Tij = k * Pi * Ej / d^2
    ratio_obs_theo DOUBLE, -- Ratio Observado/Teorico (>1 = mas flujo del esperado)
    distance_km DOUBLE,
    origen_lat DOUBLE,
    origen_lon DOUBLE,
    destino_lat DOUBLE,
    destino_lon DOUBLE
);

-- POBLAR TABLAS (queries optimizadas)

-- 1. Top 1000 Flujos OD
INSERT INTO
    gold_od_matrix_top
SELECT ROW_NUMBER() OVER (
        ORDER BY SUM(v.viajes) DESC
    ), v.origen_zone_id, zo.zone_name, v.destino_zone_id, zd.zone_name, SUM(v.viajes), SUM(v.viajes_km), AVG(v.viajes)
FROM silver_db.silver_fact_viajes v
    JOIN silver_db.silver_dim_zonas zo ON v.origen_zone_id = zo.zone_id
    JOIN silver_db.silver_dim_zonas zd ON v.destino_zone_id = zd.zone_id
GROUP BY
    v.origen_zone_id,
    zo.zone_name,
    v.destino_zone_id,
    zd.zone_name
ORDER BY SUM(v.viajes) DESC
LIMIT 1000;

-- 2. Patrones Horarios
INSERT INTO
    gold_hourly_patterns
SELECT v.hora, SUM(v.viajes), AVG(
        v.viajes_km / NULLIF(v.viajes, 0)
    ), 100.0 * SUM(v.viajes) / (
        SELECT SUM(viajes)
        FROM silver_db.silver_fact_viajes
    )
FROM silver_db.silver_fact_viajes v
GROUP BY
    v.hora
ORDER BY v.hora;

-- 3. Movilidad por Tipo de Dia
INSERT INTO
    gold_mobility_by_day_type
SELECT
    CASE
        WHEN c.es_fin_de_semana THEN 'fin_de_semana'
        WHEN c.es_festivo_nacional THEN 'festivo'
        ELSE 'laborable'
    END,
    SUM(v.viajes),
    AVG(v.viajes)
FROM silver_db.silver_fact_viajes v
    JOIN silver_db.silver_dim_calendario c ON v.fecha = c.fecha
GROUP BY
    CASE
        WHEN c.es_fin_de_semana THEN 'fin_de_semana'
        WHEN c.es_festivo_nacional THEN 'festivo'
        ELSE 'laborable'
    END;

-- 4. Top 20 Zonas
INSERT INTO
    gold_top_zones
SELECT ROW_NUMBER() OVER (
        ORDER BY SUM(v.viajes) DESC
    ), z.zone_id, z.zone_name, z.zone_level, SUM(v.viajes)
FROM silver_db.silver_fact_viajes v
    JOIN silver_db.silver_dim_zonas z ON v.origen_zone_id = z.zone_id
GROUP BY
    z.zone_id,
    z.zone_name,
    z.zone_level
ORDER BY SUM(v.viajes) DESC
LIMIT 20;

-- 5. Modelo de Gravedad (Tij = k * Pi * Ej / d^2)
INSERT INTO
    gold_gravity_model
WITH
    -- A. Calcular flujos base y distancias
    base_flows AS (
        SELECT
            v.origen_zone_id,
            zo.zone_name as origen_name,
            v.destino_zone_id,
            zd.zone_name as destino_name,
            SUM(v.viajes) as observed_trips,
            -- Distancia haversine (minimo 0.5km para evitar division por cero)
            GREATEST(
                0.5,
                CASE
                    WHEN ao.centroid_lat IS NOT NULL
                    AND ad.centroid_lat IS NOT NULL THEN 111.32 * SQRT(
                        POW(
                            ao.centroid_lat - ad.centroid_lat,
                            2
                        ) + POW(
                            (
                                ao.centroid_lon - ad.centroid_lon
                            ) * COS(
                                RADIANS(
                                    (
                                        ao.centroid_lat + ad.centroid_lat
                                    ) / 2
                                )
                            ),
                            2
                        )
                    )
                    ELSE NULL
                END
            ) as distance_km,
            ao.centroid_lat as o_lat,
            ao.centroid_lon as o_lon,
            ad.centroid_lat as d_lat,
            ad.centroid_lon as d_lon
        FROM
            silver_db.silver_fact_viajes v
            JOIN silver_db.silver_dim_zonas zo ON v.origen_zone_id = zo.zone_id
            JOIN silver_db.silver_dim_zonas zd ON v.destino_zone_id = zd.zone_id
            LEFT JOIN silver_db.silver_dim_zona_atributos ao ON v.origen_zone_id = ao.zone_id
            LEFT JOIN silver_db.silver_dim_zona_atributos ad ON v.destino_zone_id = ad.zone_id
        GROUP BY
            v.origen_zone_id,
            zo.zone_name,
            v.destino_zone_id,
            zd.zone_name,
            ao.centroid_lat,
            ao.centroid_lon,
            ad.centroid_lat,
            ad.centroid_lon
        HAVING
            SUM(v.viajes) > 100
    ),
    -- B. Calcular Produccion (Pi) por zona
    zone_prod AS (
        SELECT origen_zone_id as zone_id, SUM(observed_trips) as Pi
        FROM base_flows
        GROUP BY
            origen_zone_id
    ),
    -- C. Calcular Atraccion (Ej) por zona
    zone_attr AS (
        SELECT destino_zone_id as zone_id, SUM(observed_trips) as Ej
        FROM base_flows
        GROUP BY
            destino_zone_id
    ),
    -- D. Calcular factor de gravedad sin escalar (Pi * Ej / d^2)
    gravity_term AS (
        SELECT b.*, p.Pi, a.Ej, (p.Pi * a.Ej) / POW(b.distance_km, 2) as gravity_factor
        FROM
            base_flows b
            JOIN zone_prod p ON b.origen_zone_id = p.zone_id
            JOIN zone_attr a ON b.destino_zone_id = a.zone_id
        WHERE
            b.distance_km IS NOT NULL
    ),
    -- E. Calcular k (factor de escala global para calibrar)
    global_k AS (
        SELECT SUM(observed_trips) / SUM(gravity_factor) as k_factor
        FROM gravity_term
    )
    -- F. Insertar con viajes teoricos
SELECT
    ROW_NUMBER() OVER (
        ORDER BY g.observed_trips DESC
    ),
    g.origen_zone_id,
    g.origen_name,
    g.destino_zone_id,
    g.destino_name,
    g.observed_trips,
    (g.gravity_factor * k.k_factor) as theoretical_trips,
    g.observed_trips / NULLIF(
        (g.gravity_factor * k.k_factor),
        0
    ) as ratio_obs_theo,
    g.distance_km,
    g.o_lat,
    g.o_lon,
    g.d_lat,
    g.d_lon
FROM gravity_term g, global_k k
ORDER BY g.observed_trips DESC
LIMIT 5000;

-- INDICES
CREATE INDEX IF NOT EXISTS idx_gold_od_origen ON gold_od_matrix_top (origen_zone_id);

CREATE INDEX IF NOT EXISTS idx_gold_od_destino ON gold_od_matrix_top (destino_zone_id);

CREATE INDEX IF NOT EXISTS idx_gold_gravity_origen ON gold_gravity_model (origen_zone_id);

CREATE INDEX IF NOT EXISTS idx_gold_gravity_destino ON gold_gravity_model (destino_zone_id);

CREATE INDEX IF NOT EXISTS idx_gold_gravity_distance ON gold_gravity_model (distance_km);