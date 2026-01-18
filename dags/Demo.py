import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import boto3
import duckdb
import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param
from keplergl import KeplerGl
from shapely import wkt as shapely_wkt
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform

try:
    # Shapely 2.x
    from shapely.validation import make_valid as _make_valid  # type: ignore
except Exception:  # pragma: no cover
    _make_valid = None

# --- CONFIGURACIÓN ---
# Change credentials
BUCKET_NAME = "bigdatabucket-ducklake"
S3_REGION = "eu-central-1"

# AWS (S3)
MY_ACCESS_KEY = ""
MY_SECRET_KEY = ""

# Postgres (Ducklake metadata)
POSTGRES_USER = ""
POSTGRES_PASSWORD = ""

PARAMS = {
    "wkt_polygon": Param(
        "POLYGON((-3.71 40.42, -3.69 40.42, -3.69 40.41, -3.71 40.41, -3.71 40.42))", 
        type="string", 
        description="Pega aquí el WKT Polygon"
    ),
    
    # - wkt_epsg: CRS WKT  ( EPSG:4326)
    "wkt_epsg": Param(4326, type="integer", description="EPSG del WKT recibido (default 4326)"),
    "wkt_axis": Param("auto", type="string", description="Orden de ejes del WKT: 'auto'|'lonlat'|'latlon'"),
    # Default : EPSG:25830 (ETRS89 / UTM 30N).
    "zonas_epsg": Param(25830, type="integer", description="EPSG de geometry_wkt (default 25830)"),
    "filtro_zona": Param(
        "origin",
        type="string",
        description="Filtro espacial: 'origin'|'destination'|'origin and destination'|'origin or destination'",
        enum=["origin", "destination", "origin and destination", "origin or destination"],
    ),
    "fecha_inicio": Param("2022-02-01", type="string", format="date"),
    "fecha_fin": Param("2022-02-05", type="string", format="date")
}

@dataclass(frozen=True)
class PolygonRequest:
    wkt_lonlat: str
    fecha_inicio: str
    fecha_fin: str
    wkt_epsg: int
    zonas_epsg: int


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _maybe_swap_xy(geom: BaseGeometry) -> BaseGeometry:
    # Swap X/Y for all coordinates.
    return shapely_transform(lambda x, y, z=None: (y, x) if z is None else (y, x, z), geom)


def _normalize_wkt_axis(wkt_text: str, axis: str) -> str:
    """
    Returns WKT in correct order lon/lat (x=lon, y=lat).
    - axis='lonlat': correct
    - axis='latlon': swap xy
    - axis='auto': detects axis inverted
    """
    geom = shapely_wkt.loads(wkt_text)
    if not geom.is_valid:
        # make_valid (Shapely 2) o fallback clásico buffer(0)
        geom = _make_valid(geom) if _make_valid else geom.buffer(0)

    axis = (axis or "auto").lower().strip()
    if axis == "lonlat":
        return geom.wkt
    if axis == "latlon":
        return _maybe_swap_xy(geom).wkt


    c = geom.centroid

    if abs(c.x) > 20 and abs(c.y) < 20:
        return _maybe_swap_xy(geom).wkt

    return geom.wkt


def _normalize_filter_mode(value: str) -> str:
    mode = (value or "origin").lower().strip()

    mode = mode.replace(" or ", "_or_").replace("&", "and").replace("-", "_").replace(" ", "_")
    while "__" in mode:
        mode = mode.replace("__", "_")
    if mode in {"origin_destination", "origin_and_destination", "originanddestination"}:
        return "origin_and_destination"
    if mode in {"origin_or_destination", "originor_destination", "origin_ordestination"}:
        return "origin_or_destination"
    if mode in {"origin", "destination"}:
        return mode
    return "origin"


def _normalize_wkt_to_crs84_lonlat(wkt_text: str, axis: str, epsg: int) -> str:
    geom = shapely_wkt.loads(wkt_text)
    if not geom.is_valid:
        geom = _make_valid(geom) if _make_valid else geom.buffer(0)

    axis = (axis or "auto").lower().strip()
    if axis == "latlon":
        geom = _maybe_swap_xy(geom)
    elif axis == "auto":
        c = geom.centroid
        if abs(c.x) > 20 and abs(c.y) < 20:
            geom = _maybe_swap_xy(geom)

    epsg = int(epsg)
    if epsg != 4326:
        from pyproj import Transformer

        transformer = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
        geom = shapely_transform(transformer.transform, geom)

    return geom.wkt


def get_demo_connection():
    con = duckdb.connect()
    con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")
    
    con.execute(f"SET s3_region='{S3_REGION}';")
    con.execute(f"SET s3_access_key_id='{_escape_sql_literal(MY_ACCESS_KEY)}';")
    con.execute(f"SET s3_secret_access_key='{_escape_sql_literal(MY_SECRET_KEY)}';")
    
    con.execute("INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
    pg_user = POSTGRES_USER
    pg_pass = POSTGRES_PASSWORD
    con.execute(
        "CREATE OR REPLACE SECRET secreto_postgres ("
        "TYPE postgres, "
        "HOST 'ep-silent-art-agv6w15r-pooler.c-2.eu-central-1.aws.neon.tech', "
        "PORT 5432, "
        "DATABASE neondb, "
        f"USER '{_escape_sql_literal(pg_user)}', "
        f"PASSWORD '{_escape_sql_literal(pg_pass)}'"
        ");"
    )
    con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
    con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lakehouse (DATA_PATH 's3://{BUCKET_NAME}/ducklake/')")
    return con


def _s3_upload(local_path: Path, s3_uri: str) -> None:
    # s3_uri: s3://bucket/key
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"s3_uri inválido: {s3_uri}")
    _, _, rest = s3_uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"s3_uri inválido: {s3_uri}")

    s3 = boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=MY_ACCESS_KEY,
        aws_secret_access_key=MY_SECRET_KEY,
    )
    s3.upload_file(str(local_path), bucket, key)


def _auto_fix_lonlat_columns(df: pd.DataFrame, lon_col: str, lat_col: str) -> pd.DataFrame:
    """
    Fast heurístic to swap lon/lat.
    (Ej: lon ~ 40 y lat ~ -3 en España).
    """
    if lon_col not in df.columns or lat_col not in df.columns or df.empty:
        return df

    lon_med = float(pd.to_numeric(df[lon_col], errors="coerce").abs().median())
    lat_med = float(pd.to_numeric(df[lat_col], errors="coerce").abs().median())
    # Si "lon" parece latitud (grande) y "lat" parece longitud (pequeña), swap.
    if lon_med > 20 and lat_med < 20:
        df = df.copy()
        df[[lon_col, lat_col]] = df[[lat_col, lon_col]]
    return df


def _filter_df_for_arcs(
    df: pd.DataFrame,
    origin_cols: tuple[str, str] = ("lon_origen", "lat_origen"),
    dest_cols: tuple[str, str] = ("lon_destino", "lat_destino"),
) -> pd.DataFrame:
    if df.empty:
        return df
    lon_o = pd.to_numeric(df[origin_cols[0]], errors="coerce")
    lat_o = pd.to_numeric(df[origin_cols[1]], errors="coerce")
    lon_d = pd.to_numeric(df[dest_cols[0]], errors="coerce")
    lat_d = pd.to_numeric(df[dest_cols[1]], errors="coerce")
    mask = lon_o.notna() & lat_o.notna() & lon_d.notna() & lat_d.notna()
    return df[mask].copy()


def _points_in_polygon(
    df: pd.DataFrame,
    lon_col: str,
    lat_col: str,
    polygon: BaseGeometry,
) -> pd.Series:
    if df.empty or lon_col not in df.columns or lat_col not in df.columns:
        return pd.Series(False, index=df.index)

    # Import local para evitar depender del import global en algunos entornos
    from shapely.geometry import Point
    from shapely.prepared import prep

    lon = pd.to_numeric(df[lon_col], errors="coerce")
    lat = pd.to_numeric(df[lat_col], errors="coerce")
    valid = lon.notna() & lat.notna()

    inside = pd.Series(False, index=df.index)
    if valid.any():
        polygon_prep = prep(polygon)
        inside.loc[valid] = [
            polygon_prep.contains(Point(xy)) for xy in zip(lon[valid].astype(float), lat[valid].astype(float))
        ]
    return inside


def _fetch_zone_name_map(
    con: duckdb.DuckDBPyConnection, zone_ids: list[str], max_ids: int = 200
) -> dict[str, str]:
    """
    Returns zone_id -> zone_name (if exists).
    """
    ids = [str(z) for z in zone_ids if z is not None]
  
    ids = list(dict.fromkeys(ids))[: max_ids]
    if not ids:
        return {}


    base_ids = []
    for zid in ids:

        if "_" in zid:
            base_part = zid.split("_")[0]
            if base_part and base_part not in ids:
                base_ids.append(base_part)
       
        try:
            num_id = str(int(zid.split("_")[0] if "_" in zid else zid))
            if num_id not in ids and num_id not in base_ids:
                base_ids.append(num_id)
        except (ValueError, TypeError):
            pass

    all_query_ids = ids + base_ids
    all_query_ids = list(dict.fromkeys(all_query_ids))[: max_ids * 2]

    try:
       
        in_list = ", ".join(f"'{_escape_sql_literal(z)}'" for z in all_query_ids)
        rows = con.execute(
            f"""
            SELECT DISTINCT
                CAST(z.zone_id AS VARCHAR) AS zone_id,
                COALESCE(
                    NULLIF(bine.name, ''),
                    NULLIF(bine.name, CAST(z.zone_id AS VARCHAR)),
                    NULLIF(zn.zone_name, ''),
                    NULLIF(zn.zone_name, CAST(z.zone_id AS VARCHAR)),
                    'Zona ' || CAST(z.zone_id AS VARCHAR)
                ) AS zone_name
            FROM silver_dim_zona_atributos z
            LEFT JOIN silver_dim_zonas zn ON z.zone_id = zn.zone_id
            LEFT JOIN bronze_ine_zones bine ON z.zone_id = bine.id
            WHERE CAST(z.zone_id AS VARCHAR) IN ({in_list})
            """
        ).fetchall()
        
        print(f" _fetch_zone_name_map: Query devolvió {len(rows)} filas de {len(all_query_ids)} IDs consultados")
        
       
        base_map: dict[str, str] = {}
        base_map_int: dict[int, str] = {}
        filtered_count = 0
        sample_names = []
        for zid, zname in rows:
            zid_s = str(zid)
            name_s = (str(zname).strip() if zname is not None else "").strip()
            if len(sample_names) < 5:
                sample_names.append(f"{zid_s} -> '{name_s}'")
            # We preserve TODOS los nombres, even if "Zona X" (mejor que solo el ID)

            if name_s:
                zid_s_norm = str(zid_s).strip()
                name_s_norm = str(name_s).strip()
                
                if name_s_norm != zid_s_norm:
                    base_map[zid_s] = name_s
                    try:
                        zid_int = int(zid_s)
                        base_map_int[zid_int] = name_s
                    except (ValueError, TypeError):
                        pass
                else:
                    filtered_count += 1
            else:
                filtered_count += 1
        
        if filtered_count > 0:
            print(f"   (Filtrados {filtered_count} filas donde name==id o name vacío)")
        print(f"   Muestra de nombres devueltos por query: {sample_names}")
        print(f"   base_map tiene {len(base_map)} entradas válidas: {list(base_map.items())[:5] if base_map else []}")
        
        # Map originals IDs 
        out: dict[str, str] = {}
        for orig_id in ids:
            found_name = None
            # 1. Exact
            if orig_id in base_map:
                found_name = base_map[orig_id]
            else:
                # 2. Base part (sin sufijo como "_AM")
                base_part = orig_id.split("_")[0] if "_" in orig_id else orig_id
                # 3. Try exact (as string)
                if base_part in base_map:
                    found_name = base_map[base_part]
                else:
                    # 4. As numeric
                    try:
                        base_part_int = int(base_part)
                        if base_part_int in base_map_int:
                            found_name = base_map_int[base_part_int]
                    except (ValueError, TypeError):
                        pass
            
            if found_name:
                out[orig_id] = found_name
        
    
        if len(out) < len(ids) and base_map:
            print(f" Debug mapeo: {len(ids)} IDs originales, {len(base_map)} nombres en BD")
            unmatched = [zid for zid in ids if zid not in out][:5]
            if unmatched:
                sample_unmatched = unmatched[0]
                base_part_sample = sample_unmatched.split("_")[0] if "_" in sample_unmatched else sample_unmatched
                print(f"   Ejemplo no mapeado: '{sample_unmatched}' (base: '{base_part_sample}')")
                print(f"   ¿Existe base_part en base_map? {base_part_sample in base_map}")
                try:
                    base_int = int(base_part_sample)
                    print(f"   ¿Existe base_int en base_map_int? {base_int in base_map_int}")
                    if base_int in base_map_int:
                        print(f"   → Encontrado! Debería mapearse a: {base_map_int[base_int]}")
                except (ValueError, TypeError):
                    pass
        
        print(f" Mapeo final: {len(out)} IDs mapeados de {len(ids)} IDs originales")
        return out
    except Exception as e:
        import traceback
        print(f" Error en _fetch_zone_name_map: {e}")
        traceback.print_exc()
        return {}


def _original_geometry_is_projected_utm(con: duckdb.DuckDBPyConnection) -> bool:
    """
    Detecta si `silver_dim_zona_atributos.geometry_wkt` parece proyectada (UTM/metros).
    """
    row = con.execute(
        "SELECT geometry_wkt "
        "FROM silver_dim_zona_atributos "
        "WHERE geometry_wkt IS NOT NULL "
        "LIMIT 1"
    ).fetchone()
    if not row or not row[0]:
        return False
    g = shapely_wkt.loads(row[0])
    xmin, ymin, xmax, ymax = g.bounds
    return max(abs(xmin), abs(ymin), abs(xmax), abs(ymax)) > 1000


def _pick_best_utm_epsg_for_polygon(
    con: duckdb.DuckDBPyConnection, polygon_lonlat_wkt: str, epsg_hint: int
) -> tuple[int, int]:
    """
    Cuando `geometry_wkt` está en UTM/proyectado, puede variar por zona (28/29/30/31).
    Para evitar CSV vacío por EPSG incorrecto, probamos varios EPSG y elegimos el primero con intersecciones.
    """
    candidates = [int(epsg_hint), 25828, 25829, 25830, 25831]
   
    seen: set[int] = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    wkt_sql = _escape_sql_literal(polygon_lonlat_wkt)
    best_epsg = candidates[0]
    best_count = 0

    for epsg in candidates:
       
        poly_utm = f"ST_Transform(ST_GeomFromText('{wkt_sql}'), 'OGC:CRS84', 'EPSG:{epsg}')"
        q = (
            "SELECT COUNT(*) "
            "FROM silver_dim_zona_atributos ao "
            "WHERE ao.geometry_wkt IS NOT NULL "
            f"AND ST_Intersects(ST_GeomFromText(ao.geometry_wkt), {poly_utm})"
        )
        cnt = int(con.execute(q).fetchone()[0])
        if cnt > best_count:
            best_count = cnt
            best_epsg = epsg
        if cnt > 0:
            return epsg, cnt

    return best_epsg, best_count

@dag(
    dag_id='demo_day_interactive_tool_goat',
    default_args={'owner': 'grupo_movilidad'},
    schedule=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params=PARAMS, 
    tags=['demo_day', 'spatial', 'interactive']
)
def demo_pipeline():

    @task()
    def process_polygon_request(**context):
        params = context["params"]
        raw_wkt = params["wkt_polygon"]
        f_ini = params["fecha_inicio"]
        f_fin = params["fecha_fin"]
        wkt_epsg = int(params.get("wkt_epsg", 4326))
        zonas_epsg = int(params.get("zonas_epsg", 4326))
        wkt_axis = params.get("wkt_axis", "auto")
        filtro_zona = _normalize_filter_mode(params.get("filtro_zona", "origin"))

        # Normalization WKT: axis + CRS → CRS84 lon/lat.
        wkt_lonlat = _normalize_wkt_to_crs84_lonlat(raw_wkt, wkt_axis, wkt_epsg)
        req = PolygonRequest(
            wkt_lonlat=wkt_lonlat,
            fecha_inicio=f_ini,
            fecha_fin=f_fin,
            wkt_epsg=wkt_epsg,
            zonas_epsg=zonas_epsg,
        )
        
        run_id = context.get("run_id", datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
        safe_run_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in run_id)
        out_dir = Path("/tmp") / "demo_day_outputs" / safe_run_id
        out_dir.mkdir(parents=True, exist_ok=True)

        print(" OBJETIVO: Analizar zona WKT con CRS/axis robusto + artefactos (CSV/Kepler/PDF) en S3")
        
        con = get_demo_connection()
        con.execute("USE lakehouse") 

        # ORIGINAL (geometry_wkt).
        geometry_is_projected = _original_geometry_is_projected_utm(con)
        wkt_sql = _escape_sql_literal(req.wkt_lonlat)
        if geometry_is_projected:
            picked_epsg = zonas_epsg
            print(f"EPSG usado para geometry_wkt: {picked_epsg}")
            poly_expr = f"ST_Transform(ST_GeomFromText('{wkt_sql}'), 'OGC:CRS84', 'EPSG:{picked_epsg}')"
        else:
            poly_expr = f"ST_GeomFromText('{wkt_sql}')"
            picked_epsg = 4326
        
       
        local_csv_raw = out_dir / f"viajes_filtrados_{req.fecha_inicio}_{req.fecha_fin}.raw.csv"
        local_csv = out_dir / f"viajes_filtrados_{req.fecha_inicio}_{req.fecha_fin}.csv"
        s3_prefix = f"demo_day_outputs/{safe_run_id}"
        s3_csv_uri = f"s3://{BUCKET_NAME}/{s3_prefix}/viajes_filtrados.csv"
        s3_parquet_uri = f"s3://{BUCKET_NAME}/{s3_prefix}/viajes_filtrados.parquet"

       
        if geometry_is_projected:
            dim_point_crs84 = (
                f"ST_Transform(ST_Centroid(ST_GeomFromText(geometry_wkt)), 'EPSG:{picked_epsg}', 'OGC:CRS84')"
            )
            zonas_intersect_geom = "ST_GeomFromText(geometry_wkt)"
        else:
            dim_point_crs84 = "ST_Centroid(ST_GeomFromText(geometry_wkt))"
            zonas_intersect_geom = "ST_GeomFromText(geometry_wkt)"
        
        # We build JOIN based on filtro_zona
        if filtro_zona == "destination":
            zonas_join = (
                "JOIN zonas_dentro zd_dest "
                "ON CAST(v.destino_zone_id AS VARCHAR) LIKE (zd_dest.zone_norm || '%')"
            )
            zonas_where_extra = ""
        elif filtro_zona == "origin_and_destination":
            zonas_join = (
                "JOIN zonas_dentro zd_orig "
                "ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd_orig.zone_norm || '%') "
                "JOIN zonas_dentro zd_dest "
                "ON CAST(v.destino_zone_id AS VARCHAR) LIKE (zd_dest.zone_norm || '%')"
            )
            zonas_where_extra = ""
        elif filtro_zona == "origin_or_destination":
            # LEFT JOINs 
            zonas_join = (
                "LEFT JOIN zonas_dentro zd_orig "
                "ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd_orig.zone_norm || '%') "
                "LEFT JOIN zonas_dentro zd_dest "
                "ON CAST(v.destino_zone_id AS VARCHAR) LIKE (zd_dest.zone_norm || '%')"
            )
            # Condición: one inside the polygon
            zonas_where_extra = "AND (zd_orig.zone_norm IS NOT NULL OR zd_dest.zone_norm IS NOT NULL)"
        else:  # origin (default)
            zonas_join = (
                "JOIN zonas_dentro zd "
                "ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd.zone_norm || '%')"
            )
            zonas_where_extra = ""
        
        print(f" Filtro espacial: {filtro_zona}")

        query_local_csv = f"""
            COPY (
                WITH dim AS (
                    SELECT
                        zone_id,
                        geometry_wkt,
                        CASE
                            WHEN TRY_CAST(zone_id AS INTEGER) IS NULL THEN CAST(zone_id AS VARCHAR)
                            WHEN TRY_CAST(zone_id AS INTEGER) < 100000 THEN printf('%05d', TRY_CAST(zone_id AS INTEGER))
                            ELSE printf('%07d', TRY_CAST(zone_id AS INTEGER))
                        END AS zone_norm,
                        substr(printf('%07d', TRY_CAST(zone_id AS INTEGER)), 1, 7) AS zone_norm7,
                        substr(printf('%05d', TRY_CAST(zone_id AS INTEGER)), 1, 5) AS zone_norm5,
                        ST_X({dim_point_crs84}) AS lon,
                        ST_Y({dim_point_crs84}) AS lat
                    FROM silver_dim_zona_atributos
                    WHERE geometry_wkt IS NOT NULL
                ),
                zonas_dentro AS (
                    SELECT zone_norm, zone_norm7, zone_norm5, geometry_wkt, lon, lat
                    FROM dim
                    WHERE ST_Intersects(
                        {zonas_intersect_geom},
                        {poly_expr}
                    )
                ),
                zona_filtrada AS (
                    SELECT
                        v.origen_zone_id,
                        v.destino_zone_id,
                        v.fecha,
                        v.hora,
                        v.viajes,

                        ao.lon as lon_origen,
                        ao.lat as lat_origen,
                        ad.lon as lon_destino,
                        ad.lat as lat_destino

                    FROM silver_fact_viajes v

                    -- filtro espacial según parámetro (origen/destino/ambos)
                    {zonas_join}

                    -- coords origen/destino (prefijo, sin exigir que destino tenga geometría)
                    JOIN dim ao
                        ON CAST(v.origen_zone_id AS VARCHAR) LIKE (ao.zone_norm || '%')
                    LEFT JOIN dim ad
                        ON CAST(v.destino_zone_id AS VARCHAR) LIKE (ad.zone_norm || '%')

                    WHERE
                        v.fecha BETWEEN '{_escape_sql_literal(req.fecha_inicio)}' AND '{_escape_sql_literal(req.fecha_fin)}'
                        {zonas_where_extra}
                )
                SELECT * FROM zona_filtrada
                LIMIT 50000
            ) TO '{_escape_sql_literal(str(local_csv_raw))}' (HEADER, DELIMITER ',');
        """

        query_s3_parquet = f"""
            COPY (
                WITH dim AS (
                    SELECT
                        zone_id,
                        geometry_wkt,
                        CASE
                            WHEN TRY_CAST(zone_id AS INTEGER) IS NULL THEN CAST(zone_id AS VARCHAR)
                            WHEN TRY_CAST(zone_id AS INTEGER) < 100000 THEN printf('%05d', TRY_CAST(zone_id AS INTEGER))
                            ELSE printf('%07d', TRY_CAST(zone_id AS INTEGER))
                        END AS zone_norm,
                        substr(printf('%07d', TRY_CAST(zone_id AS INTEGER)), 1, 7) AS zone_norm7,
                        substr(printf('%05d', TRY_CAST(zone_id AS INTEGER)), 1, 5) AS zone_norm5,
                        ST_X({dim_point_crs84}) AS lon,
                        ST_Y({dim_point_crs84}) AS lat
                    FROM silver_dim_zona_atributos
                    WHERE geometry_wkt IS NOT NULL
                ),
                zonas_dentro AS (
                    SELECT zone_norm, zone_norm7, zone_norm5, geometry_wkt, lon, lat
                    FROM dim
                    WHERE ST_Intersects(
                        {zonas_intersect_geom},
                        {poly_expr}
                    )
                ),
                zona_filtrada AS (
                    SELECT
                        v.origen_zone_id,
                        v.destino_zone_id,
                        v.fecha,
                        v.hora,
                        v.viajes,

                        ao.lon as lon_origen,
                        ao.lat as lat_origen,
                        ad.lon as lon_destino,
                        ad.lat as lat_destino

                    FROM silver_fact_viajes v

                    -- filtro espacial según parámetro (origen/destino/ambos)
                    {zonas_join}

                    JOIN dim ao
                        ON CAST(v.origen_zone_id AS VARCHAR) LIKE (ao.zone_norm || '%')
                    LEFT JOIN dim ad
                        ON CAST(v.destino_zone_id AS VARCHAR) LIKE (ad.zone_norm || '%')

                    WHERE
                        v.fecha BETWEEN '{_escape_sql_literal(req.fecha_inicio)}' AND '{_escape_sql_literal(req.fecha_fin)}'
                        {zonas_where_extra}
                )
                SELECT * FROM zona_filtrada
                LIMIT 50000
            ) TO '{_escape_sql_literal(s3_parquet_uri)}' (FORMAT PARQUET);
        """
        
        print(" Ejecutando análisis espacial de alta precisión...")
        try:
            con.execute(query_local_csv)
            con.execute(query_s3_parquet)

         
            df = pd.read_csv(local_csv_raw)
            df = _auto_fix_lonlat_columns(df, "lon_origen", "lat_origen")
            df = _auto_fix_lonlat_columns(df, "lon_destino", "lat_destino")
            
            df_for_arcs = _filter_df_for_arcs(df)
            
            print(f" Datos para arcos: {len(df_for_arcs)} filas con coordenadas completas (origen + destino)")
            if len(df_for_arcs) == 0:
                print(" ADVERTENCIA: No hay datos con coordenadas completas de origen y destino. Los arcos no se mostrarán.")
            
            df.to_csv(local_csv, index=False)

            # --- Artefacto 1: CSV corregido en S3
            _s3_upload(local_csv, s3_csv_uri)

            # --- Artefacto 2: Kepler HTML ---
            from shapely.geometry import mapping

            poly_geom = shapely_wkt.loads(req.wkt_lonlat)
            kepler_config = {
                "version": "v1",
                "config": {
                    "visState": {
                        "filters": [],
                        "layers": [
                            {
                                "id": "puntos_origen",
                                "type": "point",
                                "config": {
                                    "dataId": "viajes",
                                    "label": "Puntos Origen",
                                    "color": [0, 200, 83],
                                    "columns": {"lat": "lat_origen", "lng": "lon_origen"},
                                    "isVisible": True,
                                    "visConfig": {"opacity": 0.8, "radius": 4},
                                },
                                "visualChannels": {
                                    "sizeField": {"name": "viajes", "type": "integer"},
                                    "sizeScale": "sqrt",
                                },
                            },
                            {
                                "id": "puntos_destino",
                                "type": "point",
                                "config": {
                                    "dataId": "viajes",
                                    "label": "Puntos Destino",
                                    "color": [255, 82, 82],
                                    "columns": {"lat": "lat_destino", "lng": "lon_destino"},
                                    "isVisible": True,
                                    "visConfig": {"opacity": 0.8, "radius": 4},
                                },
                                "visualChannels": {
                                    "sizeField": {"name": "viajes", "type": "integer"},
                                    "sizeScale": "sqrt",
                                },
                            },
                            {
                                "id": "arcos_od",
                                "type": "arc",
                                "config": {
                                    "dataId": "viajes",
                                    "label": "Arcos O-D",
                                    # Color de origen (source) - morado
                                    "color": [170, 0, 255],
                                    "columns": {
                                        "lat0": "lat_origen",
                                        "lng0": "lon_origen",
                                        "lat1": "lat_destino",
                                        "lng1": "lon_destino",
                                    },
                                    "isVisible": True,
                                    "visConfig": {
                                        "opacity": 0.8,
                                        "thickness": 2,
                                        # Color de destino (target) - amarillo
                                        "targetColor": [255, 215, 0],
                                        # (Algunas versiones usan sourceColor explícito)
                                        "sourceColor": [170, 0, 255],
                                    },
                                },
                                "visualChannels": {
                                    "sizeField": {"name": "viajes", "type": "integer"},
                                    "sizeScale": "sqrt",
                                },
                            },
                            {
                                "id": "poligono",
                                "type": "geojson",
                                "config": {
                                    "dataId": "polygon",
                                    "label": "Poligono",
                                    "color": [255, 153, 31],
                                    "isVisible": True,
                                    "visConfig": {
                                        "opacity": 0.15,
                                        "strokeOpacity": 0.9,
                                        "thickness": 1,
                                        "strokeColor": [255, 153, 31],
                                    },
                                },
                            },
                        ],
                        "interactionConfig": {"tooltip": {"enabled": True}},
                    },
                    "mapState": {
                        "latitude": float(poly_geom.centroid.y),
                        "longitude": float(poly_geom.centroid.x),
                        "zoom": 9,
                        "bearing": 0,
                        "pitch": 0,
                    },
                },
            }

           
            mapbox_key = os.getenv("MAPBOX_API_KEY")
            if mapbox_key:
                kepler_config["config"]["mapboxApiAccessToken"] = mapbox_key

            kepler = KeplerGl(height=700, config=kepler_config)
            kepler.add_data(data=df_for_arcs, name="viajes")
            poly_geojson = {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": "polygon"},
                        "geometry": mapping(poly_geom),
                    }
                ],
            }
            kepler.add_data(data=poly_geojson, name="polygon")
            kepler_html = out_dir / "kepler.html"
            kepler.save_to_html(file_name=str(kepler_html), read_only=True)
            s3_kepler_uri = f"s3://{BUCKET_NAME}/{s3_prefix}/kepler.html"
            _s3_upload(kepler_html, s3_kepler_uri)

            # --- PDF ---
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_pdf import PdfPages

            pdf_path = out_dir / "report.pdf"
            with PdfPages(str(pdf_path)) as pdf:
                # Páge 1: summary
                fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
                fig.suptitle("Demo Day - Resumen de análisis espacial", fontsize=16)
                total_viajes = float(df["viajes"].sum()) if "viajes" in df.columns and len(df) else 0.0
                txt = (
                    f"Rango fechas: {req.fecha_inicio} → {req.fecha_fin}\n"
                    f"Registros: {len(df):,}\n"
                    f"Total viajes: {total_viajes:,.0f}\n"
                    f"Filtro zona: {filtro_zona}\n"
                    f"WKT (lon/lat): {req.wkt_lonlat[:120]}{'...' if len(req.wkt_lonlat) > 120 else ''}\n"
                )
                fig.text(0.05, 0.85, txt, fontsize=12, va="top")
                pdf.savefig(fig)
                plt.close(fig)

                # 
                df_rep = df.copy()
                if "viajes" in df_rep.columns:
                    df_rep["viajes_num"] = pd.to_numeric(df_rep["viajes"], errors="coerce").fillna(0.0)
                else:
                    df_rep["viajes_num"] = 0.0

                if "fecha" in df_rep.columns:
                    df_rep["fecha_dt"] = pd.to_datetime(df_rep["fecha"], errors="coerce")
                else:
                    df_rep["fecha_dt"] = pd.NaT

                # Página 2: 
                if "fecha" in df_rep.columns and "viajes_num" in df_rep.columns and len(df_rep):
                    df_ts = (
                        df_rep.dropna(subset=["fecha_dt"])
                        .groupby("fecha", as_index=False)["viajes_num"]
                        .sum()
                        .sort_values("fecha")
                    )
                    fig, ax = plt.subplots(figsize=(11.69, 8.27))
                    ax.plot(df_ts["fecha"], df_ts["viajes_num"], marker="o")
                    ax.set_title("Viajes por día")
                    ax.set_xlabel("fecha")
                    ax.set_ylabel("viajes")
                    ax.tick_params(axis="x", rotation=45)
                    ax.grid(True, alpha=0.3)
                    fig.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)

                # Página 3: Weekday vs Weekend (media diaria)
                if df_rep["fecha_dt"].notna().any() and len(df_rep):
                    df_rep_valid = df_rep[df_rep["fecha_dt"].notna()].copy()
                    df_rep_valid["is_weekend"] = df_rep_valid["fecha_dt"].dt.weekday >= 5

                    # 1) Sum by day
                    daily = (
                        df_rep_valid.groupby(["fecha", "is_weekend"], as_index=False)["viajes_num"]
                        .sum()
                    )
                    # 2) media de los días weekday vs weekend
                    wk_mean = daily.groupby("is_weekend", as_index=False)["viajes_num"].mean()
                    wk_map = {False: 0.0, True: 0.0}
                    for _, r in wk_mean.iterrows():
                        wk_map[bool(r["is_weekend"])] = float(r["viajes_num"])

                    fig, ax = plt.subplots(figsize=(11.69, 8.27))
                    ax.bar(["Entre semana", "Fin de semana"], [wk_map[False], wk_map[True]])
                    ax.set_title("Comparativa (media diaria): Entre semana vs Fin de semana")
                    ax.set_ylabel("media de viajes/día")
                    ax.grid(True, axis="y", alpha=0.3)
                    fig.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)

                    # Página 4: By day
                    dow_order = [
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                        "Saturday",
                        "Sunday",
                    ]
                    df_rep_valid["dow"] = df_rep_valid["fecha_dt"].dt.day_name()
                    dow_sum = (
                        df_rep_valid.groupby("dow", as_index=False)["viajes_num"]
                        .sum()
                        .set_index("dow")
                        .reindex(dow_order)
                        .fillna(0.0)
                    )
                    fig, ax = plt.subplots(figsize=(11.69, 8.27))
                    ax.bar(dow_sum.index.tolist(), dow_sum["viajes_num"].tolist())
                    ax.set_title("Viajes por día de la semana")
                    ax.set_ylabel("viajes")
                    ax.tick_params(axis="x", rotation=25)
                    ax.grid(True, axis="y", alpha=0.3)
                    fig.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)

                # Página 5: Patrón horario
                if "hora" in df_rep.columns and len(df_rep):
                    hora = pd.to_numeric(df_rep["hora"], errors="coerce")
                    df_hr = df_rep[hora.notna()].copy()
                    df_hr["hora_int"] = hora[hora.notna()].astype(int)
                    df_hr = df_hr[df_hr["hora_int"].between(0, 23)]
                    if len(df_hr):
                        hr_sum = (
                            df_hr.groupby("hora_int", as_index=False)["viajes_num"]
                            .sum()
                            .set_index("hora_int")
                            .reindex(list(range(24)))
                            .fillna(0.0)
                        )
                        fig, ax = plt.subplots(figsize=(11.69, 8.27))
                        ax.plot(hr_sum.index.tolist(), hr_sum["viajes_num"].tolist(), marker="o")
                        ax.set_title("Patrón horario (viajes por hora)")
                        ax.set_xlabel("hora")
                        ax.set_ylabel("viajes")
                        ax.set_xticks(list(range(0, 24, 1)))
                        ax.grid(True, alpha=0.3)
                        fig.tight_layout()
                        pdf.savefig(fig)
                        plt.close(fig)

                # Nombres de zonas (si existen) para enriquecer etiquetas del report
                zone_name_map: dict[str, str] = {}
                try:
                    top_ids: list[str] = []
                    if "origen_zone_id" in df_rep.columns:
                        top_ids += (
                            df_rep.groupby("origen_zone_id", as_index=False)["viajes_num"]
                            .sum()
                            .sort_values("viajes_num", ascending=False)
                            .head(20)["origen_zone_id"]
                            .astype(str)
                            .tolist()
                        )
                    if "destino_zone_id" in df_rep.columns:
                        top_ids += (
                            df_rep.groupby("destino_zone_id", as_index=False)["viajes_num"]
                            .sum()
                            .sort_values("viajes_num", ascending=False)
                            .head(20)["destino_zone_id"]
                            .astype(str)
                            .tolist()
                        )
                    zone_name_map = _fetch_zone_name_map(con, top_ids)
                    print(f" Zone name map: {len(zone_name_map)} entries encontrados")
                    if zone_name_map:
                        sample = list(zone_name_map.items())[:5]
                        print(f" Ejemplos: {sample}")
                except Exception as e:
                    import traceback
                    print(f" Error obteniendo nombres de zonas: {e}")
                    traceback.print_exc()
                    zone_name_map = {}

                def _label_zone(zid: str) -> str:
                    zid_s = str(zid)
                    name = zone_name_map.get(zid_s, "")
                    if not name or name == zid_s:
                        return zid_s
                    label = f"{zid_s} - {name}"
                    return (label[:45] + "…") if len(label) > 46 else label

                # Página 6: Top zones origen + destino
                if "origen_zone_id" in df_rep.columns and "destino_zone_id" in df_rep.columns and len(df_rep):
                    top_o = (
                        df_rep.groupby("origen_zone_id", as_index=False)["viajes_num"]
                        .sum()
                        .sort_values("viajes_num", ascending=False)
                        .head(12)
                    )
                    top_d = (
                        df_rep.groupby("destino_zone_id", as_index=False)["viajes_num"]
                        .sum()
                        .sort_values("viajes_num", ascending=False)
                        .head(12)
                    )

                    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
                    axes[0].bar([_label_zone(z) for z in top_o["origen_zone_id"]], top_o["viajes_num"])
                    axes[0].set_title("Top 12 zonas origen")
                    axes[0].tick_params(axis="x", rotation=70)
                    axes[0].grid(True, axis="y", alpha=0.3)

                    axes[1].bar([_label_zone(z) for z in top_d["destino_zone_id"]], top_d["viajes_num"], color="#ff5252")
                    axes[1].set_title("Top 12 zonas destino")
                    axes[1].tick_params(axis="x", rotation=70)
                    axes[1].grid(True, axis="y", alpha=0.3)

                    fig.suptitle("Ranking de zonas (por viajes)")
                    fig.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)

                
                if filtro_zona == "origin_or_destination" and len(df_rep):
                    try:
                        report_metrics_sql = f"""
                            WITH dim AS (
                                SELECT
                                    zone_id,
                                    geometry_wkt,
                                    CASE
                                        WHEN TRY_CAST(zone_id AS INTEGER) IS NULL THEN CAST(zone_id AS VARCHAR)
                                        WHEN TRY_CAST(zone_id AS INTEGER) < 100000 THEN printf('%05d', TRY_CAST(zone_id AS INTEGER))
                                        ELSE printf('%07d', TRY_CAST(zone_id AS INTEGER))
                                    END AS zone_norm
                                FROM silver_dim_zona_atributos
                                WHERE geometry_wkt IS NOT NULL
                            ),
                            zonas_dentro AS (
                                SELECT zone_norm
                                FROM dim
                                WHERE ST_Intersects(
                                    ST_GeomFromText(dim.geometry_wkt),
                                    {poly_expr}
                                )
                            ),
                            fact AS (
                                SELECT
                                    CAST(v.origen_zone_id AS VARCHAR) AS origen_zone_id,
                                    CAST(v.destino_zone_id AS VARCHAR) AS destino_zone_id,
                                    v.viajes AS viajes,
                                    (zd_o.zone_norm IS NOT NULL) AS o_in,
                                    (zd_d.zone_norm IS NOT NULL) AS d_in
                                FROM silver_fact_viajes v
                                LEFT JOIN zonas_dentro zd_o
                                    ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd_o.zone_norm || '%')
                                LEFT JOIN zonas_dentro zd_d
                                    ON CAST(v.destino_zone_id AS VARCHAR) LIKE (zd_d.zone_norm || '%')
                                WHERE v.fecha BETWEEN '{_escape_sql_literal(req.fecha_inicio)}' AND '{_escape_sql_literal(req.fecha_fin)}'
                                  AND (zd_o.zone_norm IS NOT NULL OR zd_d.zone_norm IS NOT NULL)
                            )
                            SELECT
                                SUM(CASE WHEN o_in AND NOT d_in THEN viajes ELSE 0 END) AS only_origin,
                                SUM(CASE WHEN d_in AND NOT o_in THEN viajes ELSE 0 END) AS only_dest,
                                SUM(CASE WHEN o_in AND d_in THEN viajes ELSE 0 END) AS both,
                                SUM(CASE WHEN o_in AND d_in AND origen_zone_id = destino_zone_id THEN viajes ELSE 0 END) AS intra
                            FROM fact
                        """
                        m = con.execute(report_metrics_sql).fetchone()
                        only_origin_v = float(m[0] or 0)
                        only_dest_v = float(m[1] or 0)
                        both_v = float(m[2] or 0)
                        intra_v = float(m[3] or 0)

                        metrics = {
                            "Solo origen": only_origin_v,
                            "Solo destino": only_dest_v,
                            "Origen y destino": both_v,
                            "Intra-zona": intra_v,
                        }

                        fig, ax = plt.subplots(figsize=(11.69, 8.27))
                        ax.bar(list(metrics.keys()), list(metrics.values()), color=["#00c853", "#ff5252", "#aa00ff", "#607d8b"])
                        ax.set_title("Desglose (polígono como origen/destino)")
                        ax.set_ylabel("viajes")
                        ax.grid(True, axis="y", alpha=0.3)
                        fig.tight_layout()
                        pdf.savefig(fig)
                        plt.close(fig)

                        # Top zonas “partner” (SQL, consistente con el filtro)
                        partner_sql = f"""
                            WITH dim AS (
                                SELECT
                                    zone_id,
                                    geometry_wkt,
                                    CASE
                                        WHEN TRY_CAST(zone_id AS INTEGER) IS NULL THEN CAST(zone_id AS VARCHAR)
                                        WHEN TRY_CAST(zone_id AS INTEGER) < 100000 THEN printf('%05d', TRY_CAST(zone_id AS INTEGER))
                                        ELSE printf('%07d', TRY_CAST(zone_id AS INTEGER))
                                    END AS zone_norm
                                FROM silver_dim_zona_atributos
                                WHERE geometry_wkt IS NOT NULL
                            ),
                            zonas_dentro AS (
                                SELECT zone_norm
                                FROM dim
                                WHERE ST_Intersects(
                                    ST_GeomFromText(dim.geometry_wkt),
                                    {poly_expr}
                                )
                            ),
                            fact AS (
                                SELECT
                                    CAST(v.origen_zone_id AS VARCHAR) AS origen_zone_id,
                                    CAST(v.destino_zone_id AS VARCHAR) AS destino_zone_id,
                                    v.viajes AS viajes,
                                    (zd_o.zone_norm IS NOT NULL) AS o_in,
                                    (zd_d.zone_norm IS NOT NULL) AS d_in
                                FROM silver_fact_viajes v
                                LEFT JOIN zonas_dentro zd_o
                                    ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd_o.zone_norm || '%')
                                LEFT JOIN zonas_dentro zd_d
                                    ON CAST(v.destino_zone_id AS VARCHAR) LIKE (zd_d.zone_norm || '%')
                                WHERE v.fecha BETWEEN '{_escape_sql_literal(req.fecha_inicio)}' AND '{_escape_sql_literal(req.fecha_fin)}'
                                  AND (zd_o.zone_norm IS NOT NULL OR zd_d.zone_norm IS NOT NULL)
                            )
                            SELECT
                                partner_zone_id,
                                SUM(viajes) AS viajes
                            FROM (
                                SELECT
                                    CASE
                                        WHEN o_in AND NOT d_in THEN destino_zone_id
                                        WHEN d_in AND NOT o_in THEN origen_zone_id
                                        WHEN o_in AND d_in THEN destino_zone_id
                                        ELSE NULL
                                    END AS partner_zone_id,
                                    viajes
                                FROM fact
                            ) t
                            WHERE partner_zone_id IS NOT NULL
                            GROUP BY partner_zone_id
                            ORDER BY viajes DESC
                            LIMIT 12
                        """
                        top_partner = con.execute(partner_sql).fetchdf()
                        top_partner = top_partner.rename(columns={"viajes": "viajes_num"})

                        partner_name_map = _fetch_zone_name_map(con, top_partner["partner_zone_id"].astype(str).tolist())
                        labels = []
                        for zid in top_partner["partner_zone_id"].astype(str).tolist():
                            name = partner_name_map.get(zid, "")
                            if not name or name == zid:
                                labels.append(zid)
                            else:
                                lab = f"{zid} - {name}"
                                labels.append((lab[:45] + "…") if len(lab) > 46 else lab)

                        fig, ax = plt.subplots(figsize=(11.69, 8.27))
                        ax.bar(labels, top_partner["viajes_num"].astype(float), color="#ffd700")
                        ax.set_title("Top 12 zonas con más viajes conectados al polígono (partner zones)")
                        ax.set_ylabel("viajes")
                        ax.tick_params(axis="x", rotation=70)
                        ax.grid(True, axis="y", alpha=0.3)
                        fig.tight_layout()
                        pdf.savefig(fig)
                        plt.close(fig)
                    except Exception as _e:
                        import traceback
                        print(f" No se pudo generar el bloque extra de PDF (origin_or_destination): {_e}")
                        traceback.print_exc()

            s3_pdf_uri = f"s3://{BUCKET_NAME}/{s3_prefix}/report.pdf"
            _s3_upload(pdf_path, s3_pdf_uri)

            # --- Artefacto 4: JSON de salida con punteros S3 (para la demo) ---
            out_json = {
                "inputs": {
                    "fecha_inicio": req.fecha_inicio,
                    "fecha_fin": req.fecha_fin,
                    "wkt_epsg": req.wkt_epsg,
                    "zonas_epsg": req.zonas_epsg,
                    "filtro_zona": filtro_zona,
                    "wkt_axis_normalized_to": "lonlat",
                },
                "outputs": {
                    "viajes_csv": s3_csv_uri,
                    "viajes_parquet": s3_parquet_uri,
                    "kepler_html": s3_kepler_uri,
                    "report_pdf": s3_pdf_uri,
                },
            }
            local_json = out_dir / "outputs.json"
            local_json.write_text(json.dumps(out_json, ensure_ascii=False, indent=2), encoding="utf-8")
            s3_json_uri = f"s3://{BUCKET_NAME}/{s3_prefix}/outputs.json"
            _s3_upload(local_json, s3_json_uri)

            print(" ÉXITO TOTAL! Artefactos generados y subidos a S3:")
            print(f"  - Datos (CSV): {s3_csv_uri}")
            print(f"  - Datos (Parquet): {s3_parquet_uri}")
            print(f"  - Kepler (HTML): {s3_kepler_uri}")
            print(f"  - Report (PDF): {s3_pdf_uri}")
            print(f"  - Índice (JSON): {s3_json_uri}")
        except Exception as e:
            print(" ERROR DURANTE EL ANÁLISIS:")
            print(e)
            raise e
        finally:
            con.close()

    process_polygon_request()

dag = demo_pipeline()
