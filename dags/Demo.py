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

# --- CONFIG ---
BUCKET_NAME = "bigdatabucket-ducklake"
S3_REGION = "eu-central-1"


MY_ACCESS_KEY = ""
MY_SECRET_KEY = ""


POSTGRES_USER = ""
POSTGRES_PASSWORD = ""

PARAMS = {
    "wkt_polygon": Param(
        "POLYGON((-3.71 40.42, -3.69 40.42, -3.69 40.41, -3.71 40.41, -3.71 40.42))", 
        type="string", 
        description="Pega aquí el WKT Polygon"
    ),
    "wkt_epsg": Param(4326, type="integer", description="EPSG del WKT recibido (default 4326)"),
    "wkt_axis": Param("auto", type="string", description="Orden de ejes del WKT: 'auto'|'lonlat'|'latlon'"),

    "zonas_epsg": Param(25830, type="integer", description="EPSG de geometry_wkt (default 25830)"),
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
    return shapely_transform(lambda x, y, z=None: (y, x) if z is None else (y, x, z), geom)


def _normalize_wkt_axis(wkt_text: str, axis: str) -> str:
    """
    Devuelve WKT en orden lon/lat (x=lon, y=lat).
    - axis='lonlat': asume correcto
    - axis='latlon': hace swap xy
    - axis='auto': heurística por centroid para detectar ejes invertidos
    """
    geom = shapely_wkt.loads(wkt_text)
    if not geom.is_valid:
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


def _normalize_wkt_to_crs84_lonlat(wkt_text: str, axis: str, epsg: int) -> str:
    """
    Normaliza:
    - orden de ejes → lon/lat (x=lon, y=lat)
    - CRS → OGC:CRS84 (equivalente práctico a EPSG:4326 lon/lat)

    Si `epsg != 4326`, reproyecta a 4326 usando pyproj (always_xy=True).
    """
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
    Heurística rápida para corregir columnas lon/lat si vienen intercambiadas.
    (Ej: lon ~ 40 y lat ~ -3 en España).
    """
    if lon_col not in df.columns or lat_col not in df.columns or df.empty:
        return df

    lon_med = float(pd.to_numeric(df[lon_col], errors="coerce").abs().median())
    lat_med = float(pd.to_numeric(df[lat_col], errors="coerce").abs().median())
    if lon_med > 20 and lat_med < 20:
        df = df.copy()
        df[[lon_col, lat_col]] = df[[lat_col, lon_col]]
    return df


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
    dag_id='demo_day_interactive_tool',
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

        print("OBJETIVO: Analizar zona WKT con CRS/axis robusto + artefactos (CSV/Kepler/PDF) en S3")
        
        con = get_demo_connection()
        con.execute("USE lakehouse") 


        geometry_is_projected = _original_geometry_is_projected_utm(con)
        wkt_sql = _escape_sql_literal(req.wkt_lonlat)
        if geometry_is_projected:
            picked_epsg = zonas_epsg
            print(f"EPSG usado para geometry_wkt: {picked_epsg}")
            poly_expr = f"ST_Transform(ST_GeomFromText('{wkt_sql}'), 'OGC:CRS84', 'EPSG:{picked_epsg}')"
        else:
            poly_expr = f"ST_GeomFromText('{wkt_sql}')"
            picked_epsg = 4326
        
        # --- MASTER QUERY ---
        
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

                    -- origen dentro del polígono (prefijo)
                    JOIN zonas_dentro zd
                        ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd.zone_norm || '%')

                    -- coords origen/destino (prefijo, sin exigir que destino tenga geometría)
                    JOIN dim ao
                        ON CAST(v.origen_zone_id AS VARCHAR) LIKE (ao.zone_norm || '%')
                    LEFT JOIN dim ad
                        ON CAST(v.destino_zone_id AS VARCHAR) LIKE (ad.zone_norm || '%')

                    WHERE
                        v.fecha BETWEEN '{_escape_sql_literal(req.fecha_inicio)}' AND '{_escape_sql_literal(req.fecha_fin)}'
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

                    JOIN zonas_dentro zd
                        ON CAST(v.origen_zone_id AS VARCHAR) LIKE (zd.zone_norm || '%')

                    JOIN dim ao
                        ON CAST(v.origen_zone_id AS VARCHAR) LIKE (ao.zone_norm || '%')
                    LEFT JOIN dim ad
                        ON CAST(v.destino_zone_id AS VARCHAR) LIKE (ad.zone_norm || '%')

                    WHERE
                        v.fecha BETWEEN '{_escape_sql_literal(req.fecha_inicio)}' AND '{_escape_sql_literal(req.fecha_fin)}'
                )
                SELECT * FROM zona_filtrada
                LIMIT 50000
            ) TO '{_escape_sql_literal(s3_parquet_uri)}' (FORMAT PARQUET);
        """
        
        print("Ejecutando análisis espacial de alta precisión...")
        try:
            con.execute(query_local_csv)
            con.execute(query_s3_parquet)

           
            df = pd.read_csv(local_csv_raw)
            df = _auto_fix_lonlat_columns(df, "lon_origen", "lat_origen")
            df = _auto_fix_lonlat_columns(df, "lon_destino", "lat_destino")
            df.to_csv(local_csv, index=False)

           
            _s3_upload(local_csv, s3_csv_uri)

           
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
                                        "targetColor": [255, 215, 0],
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
            kepler.add_data(data=df, name="viajes")
            
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

            # --- ARTEFACT 3---
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_pdf import PdfPages

            pdf_path = out_dir / "report.pdf"
            with PdfPages(str(pdf_path)) as pdf:
                fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
                fig.suptitle("Demo Day - Resumen de análisis espacial", fontsize=16)
                total_viajes = float(df["viajes"].sum()) if "viajes" in df.columns and len(df) else 0.0
                txt = (
                    f"Rango fechas: {req.fecha_inicio} → {req.fecha_fin}\n"
                    f"Registros: {len(df):,}\n"
                    f"Total viajes: {total_viajes:,.0f}\n"
                    f"WKT (lon/lat): {req.wkt_lonlat[:120]}{'...' if len(req.wkt_lonlat) > 120 else ''}\n"
                )
                fig.text(0.05, 0.85, txt, fontsize=12, va="top")
                pdf.savefig(fig)
                plt.close(fig)

                
                if "fecha" in df.columns and "viajes" in df.columns and len(df):
                    df_ts = df.groupby("fecha", as_index=False)["viajes"].sum()
                    fig, ax = plt.subplots(figsize=(11.69, 8.27))
                    ax.plot(df_ts["fecha"], df_ts["viajes"], marker="o")
                    ax.set_title("Viajes por día")
                    ax.set_xlabel("fecha")
                    ax.set_ylabel("viajes")
                    ax.tick_params(axis="x", rotation=45)
                    ax.grid(True, alpha=0.3)
                    fig.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)

                
                if "origen_zone_id" in df.columns and "viajes" in df.columns and len(df):
                    top = (
                        df.groupby("origen_zone_id", as_index=False)["viajes"]
                        .sum()
                        .sort_values("viajes", ascending=False)
                        .head(15)
                    )
                    fig, ax = plt.subplots(figsize=(11.69, 8.27))
                    ax.bar(top["origen_zone_id"].astype(str), top["viajes"])
                    ax.set_title("Top 15 zonas origen (por viajes)")
                    ax.set_xlabel("origen_zone_id")
                    ax.set_ylabel("viajes")
                    ax.tick_params(axis="x", rotation=45)
                    fig.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)

            s3_pdf_uri = f"s3://{BUCKET_NAME}/{s3_prefix}/report.pdf"
            _s3_upload(pdf_path, s3_pdf_uri)

            # --- Artefacto 4---
            out_json = {
                "inputs": {
                    "fecha_inicio": req.fecha_inicio,
                    "fecha_fin": req.fecha_fin,
                    "wkt_epsg": req.wkt_epsg,
                    "zonas_epsg": req.zonas_epsg,
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

            print("ÉXITO TOTAL! Artefactos generados y subidos a S3:")
            print(f"  - Datos (CSV): {s3_csv_uri}")
            print(f"  - Datos (Parquet): {s3_parquet_uri}")
            print(f"  - Kepler (HTML): {s3_kepler_uri}")
            print(f"  - Report (PDF): {s3_pdf_uri}")
            print(f"  - Índice (JSON): {s3_json_uri}")
        except Exception as e:
            print("ERROR DURANTE EL ANÁLISIS:")
            print(e)
            raise e
        finally:
            con.close()

    process_polygon_request()

dag = demo_pipeline()
