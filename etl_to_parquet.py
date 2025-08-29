#!/usr/bin/env python3
"""
ETL to Parquet → GCS / S3 / Azure (single file per source)

- Multiple sources (table or custom query)
- Optional last N days on a timestamp column
- Upper bound control: < or <=
- Single Parquet per source per run (no daily partitioning)
- Uploads to GCS, S3, or Azure
- Skips upload when empty (configurable via io.skip_empty_uploads)

CLI:
  python etl_to_parquet.py --config config.yml [--chunksize 100000] [--dry-run]
  python etl_to_parquet.py --config config.yml --only-source orders --only-source events
"""

import argparse
import datetime as dt
import os
import sys
import tempfile
import uuid
import logging
from typing import Optional, Dict, Any, List, Tuple

import yaml
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, select
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

import pyarrow as pa
import pyarrow.parquet as pq

# Optional cloud SDKs
try:
    import boto3  # S3
except Exception:
    boto3 = None

try:
    from google.cloud import storage as gcs_storage  # GCS
except Exception:
    gcs_storage = None

try:
    from azure.storage.blob import BlobServiceClient  # Azure
except Exception:
    BlobServiceClient = None


# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_to_parquet")


# -----------------------
# YAML / URL helpers
# -----------------------
def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def deduce_sqlalchemy_url(dbconf: Dict[str, Any]) -> str:
    if dbconf.get("url"):
        return dbconf["url"]

    db_type = (dbconf.get("type") or "").lower()
    user = dbconf.get("user")
    password = dbconf.get("password")
    host = dbconf.get("host", "localhost")
    port = dbconf.get("port")
    database = dbconf.get("database")

    if db_type == "postgresql":
        port = port or 5432
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    if db_type == "mysql":
        port = port or 3306
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    if db_type == "oracle":
        port = port or 1521
        service_name = dbconf.get("service_name")
        sid = dbconf.get("sid")
        if service_name:
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
        elif sid:
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/{sid}"
        raise ValueError("Oracle config requires 'service_name' or 'sid'.")

    if db_type in ("sql server", "mssql", "sqlserver"):
        driver = dbconf.get("odbc_driver", "ODBC Driver 17 for SQL Server")
        port_str = f",{port}" if port else ""
        odbc_str = f"DRIVER={{{{{{driver}}}}}};SERVER={host}{port_str};DATABASE={database};UID={user};PWD={password}"
        return str(URL.create("mssql+pyodbc", query={"odbc_connect": odbc_str}))

    raise ValueError(f"Unsupported or missing db.type: {db_type}")

def _guess_epoch_unit(int_series: pd.Series) -> str:
    """
    Guess epoch unit from magnitude; returns one of 's','ms','us','ns'.
    Uses first non-null value as a heuristic.
    """
    s = int_series.dropna()
    if s.empty:
        return "s"
    v = float(s.iloc[0])
    if v < 1e11:    # < 100,000,000,000  -> seconds
        return "s"
    if v < 1e14:    # < 100,000,000,000,000 -> milliseconds
        return "ms"
    if v < 1e17:    # < 100,000,000,000,000,000 -> microseconds
        return "us"
    return "ns"     # otherwise treat as nanoseconds

def create_db_engine(dbconf: Dict[str, Any]) -> Engine:
    sqlalchemy_url = deduce_sqlalchemy_url(dbconf)
    engine_args = dbconf.get("engine_args", {}) or {}
    return create_engine(
        sqlalchemy_url,
        pool_size=engine_args.get("pool_size", 5),
        max_overflow=engine_args.get("max_overflow", 10),
        future=True,
    )


# -----------------------
# Naming / templating
# -----------------------
def render_template(template: str, ctx: Dict[str, str]) -> str:
    out = template
    for k, v in ctx.items():
        out = out.replace(f"{{{{{k}}}}}", v)
    return out


def make_names(
    src: Dict[str, Any],
    run_ts: dt.datetime,
    from_ts: Optional[dt.datetime],
    to_ts: Optional[dt.datetime],
) -> Tuple[str, Optional[str]]:
    """
    Return (local_filename, object_name) with templating.
    Placeholders: {{schema}}, {{table}}, {{run_ts}}, {{from_ts}}, {{to_ts}}
    """
    schema = src.get("schema") or ""
    table = src.get("table") or (src.get("name") or f"query_{uuid.uuid4().hex[:8]}")
    ctx = {
        "schema": schema,
        "table": table,
        "run_ts": run_ts.strftime("%Y%m%d_%H%M%S"),
        "from_ts": (from_ts.strftime("%Y%m%d") if from_ts else ""),
        "to_ts": (to_ts.strftime("%Y%m%d") if to_ts else ""),
    }

    base = src.get("output_filename") or "{{schema}}_{{table}}_{{from_ts}}_to_{{to_ts}}_{{run_ts}}.parquet"
    local_name = render_template(base, ctx)
    if not local_name.lower().endswith(".parquet"):
        local_name += ".parquet"

    object_name = src.get("object_name")
    if object_name:
        object_name = render_template(object_name, ctx)

    return local_name, object_name


def maybe_render_global_object_name(
    global_obj_name: Optional[str],
    src: Dict[str, Any],
    run_ts: dt.datetime,
    from_ts: Optional[dt.datetime],
    to_ts: Optional[dt.datetime],
) -> Optional[str]:
    if not global_obj_name:
        return None
    schema = src.get("schema") or ""
    table = src.get("table") or (src.get("name") or "query")
    ctx = {
        "schema": schema,
        "table": table,
        "run_ts": run_ts.strftime("%Y%m%d_%H%M%S"),
        "from_ts": (from_ts.strftime("%Y%m%d") if from_ts else ""),
        "to_ts": (to_ts.strftime("%Y%m%d") if to_ts else ""),
    }
    return render_template(global_obj_name, ctx)


# -----------------------
# Statement builder
# -----------------------
def build_statement(
    engine: Engine,
    src: Dict[str, Any],
    from_utc: Optional[dt.datetime],
    to_utc: Optional[dt.datetime],
    upper_inclusive: bool,
):
    """
    Table mode:
      - Adds WHERE ts >= :from_utc AND ts <(=) :to_utc if timestamp_column provided.

    Query mode:
      - If query already references :from_utc / :to_utc, pass params through.
      - Else, if timestamp_column provided AND from/to given, wrap the query and inject a WHERE on that column.
        SELECT * FROM ( <original query> ) sub WHERE sub.<timestamp_column> >= :from_utc AND sub.<op_to> :to_utc
    Returns (statement, params).
    """
    query = src.get("query")
    ts_col_name = src.get("timestamp_column")
    op_to = "<=" if upper_inclusive else "<"

    if query:
        params = {}
        if (from_utc is not None) and (to_utc is not None):
            # If caller wrote the predicates explicitly, just bind
            if (":from_utc" in query) or (":to_utc" in query):
                params["from_utc"] = from_utc
                params["to_utc"] = to_utc
                return text(query), params

            # Otherwise, inject using timestamp_column if provided
            if ts_col_name:
                wrapped = f"""
                SELECT * FROM (
                    {query}
                ) sub
                WHERE sub.{ts_col_name} >= :from_utc
                  AND sub.{ts_col_name} {op_to} :to_utc
                """
                params["from_utc"] = from_utc
                params["to_utc"] = to_utc
                return text(wrapped), params
            # No way to inject safely without knowing the column
        return text(query), params

    # ---- Table mode ----
    schema = src.get("schema")
    table = src.get("table")
    if not (schema and table):
        raise ValueError("Each source must provide either 'query' or both 'schema' and 'table'.")

    md = MetaData()
    tbl = Table(table, md, schema=schema, autoload_with=engine)
    stmt = select(tbl)

    params = {}
    last_n_days = src.get("last_n_days")

    if ts_col_name and (last_n_days or (from_utc is not None and to_utc is not None)):
        if ts_col_name not in tbl.c:
            raise ValueError(f"timestamp_column '{ts_col_name}' not found in {schema}.{table}")
        col_qualified = f"{tbl.c[ts_col_name].compile(dialect=engine.dialect)}"
        if from_utc is not None and to_utc is not None:
            stmt = stmt.where(text(f"{col_qualified} >= :from_utc"))
            stmt = stmt.where(text(f"{col_qualified} {op_to} :to_utc"))
            params["from_utc"] = from_utc
            params["to_utc"] = to_utc

    where_extra = src.get("where_extra")
    if where_extra:
        stmt = stmt.where(text(where_extra))

    return stmt, params


# -----------------------
# Extraction & upload
# -----------------------
def extract_to_parquet(
    engine: Engine,
    stmt,
    params: Dict[str, Any],
    out_path: str,
    chunksize: int,
    parquet_compression: str,
    ts_coerce: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Stream query → single Parquet.
    - ts_coerce: {"columns": [..], "utc": True/False, "naive": True/False}
    """
    total_rows = 0
    writer = None
    target_arrow_schema = None

    ts_cols = (ts_coerce or {}).get("columns", []) or []
    coerce_utc = bool((ts_coerce or {}).get("utc", True))
    make_naive = bool((ts_coerce or {}).get("naive", True))

    try:
        for i, chunk in enumerate(pd.read_sql_query(stmt, engine, params=params, chunksize=chunksize)):
            if chunk is None or chunk.empty:
                if i == 0:
                    logger.info("    (no rows)")
                break

            # ---- Timestamp coercion to ensure Parquet logical timestamps ----
            for col in ts_cols:
                if col not in chunk.columns:
                    continue

                # Already datetime?
                if pd.api.types.is_datetime64_any_dtype(chunk[col]):
                    dtser = chunk[col]
                    # normalize tz → UTC‑naive if requested
                    if getattr(dtser.dtype, "tz", None) is not None:
                        if coerce_utc:
                            dtser = dtser.dt.tz_convert("UTC")
                        # drop tz to store as naive UTC
                        if make_naive:
                            dtser = dtser.dt.tz_localize(None)
                    chunk[col] = dtser.astype("datetime64[ns]")
                else:
                    # Convert from integers/strings → datetime
                    if pd.api.types.is_integer_dtype(chunk[col]):
                        unit = _guess_epoch_unit(chunk[col])
                        dtser = pd.to_datetime(chunk[col], unit=unit, utc=coerce_utc, errors="coerce")
                    else:
                        dtser = pd.to_datetime(chunk[col], utc=coerce_utc, errors="coerce")
                    # Ensure UTC‑naive if requested
                    if getattr(dtser.dtype, "tz", None) is not None:
                        if coerce_utc:
                            dtser = dtser.dt.tz_convert("UTC")
                        if make_naive:
                            dtser = dtser.dt.tz_localize(None)
                    chunk[col] = dtser.astype("datetime64[ns]")

            # Build Arrow table
            table = pa.Table.from_pandas(chunk, preserve_index=False)

            # On first non-empty chunk, fix schema to force timestamp logical types
            if writer is None:
                os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

                if ts_cols:
                    fields = []
                    schema_names = table.schema.names
                    # map desired types: chosen 'timestamp("us")' for wide compatibility
                    for name in schema_names:
                        if name in ts_cols:
                            fields.append(pa.field(name, pa.timestamp("us")))
                        else:
                            fields.append(pa.field(name, table.schema.field(name).type))
                    target_arrow_schema = pa.schema(fields)
                    # cast current chunk to target schema
                    table = table.cast(target_arrow_schema)
                    writer = pq.ParquetWriter(out_path, target_arrow_schema, compression=parquet_compression)
                else:
                    writer = pq.ParquetWriter(out_path, table.schema, compression=parquet_compression)

            else:
                # Keep schema stable across chunks
                if target_arrow_schema is not None:
                    table = table.cast(target_arrow_schema)

            writer.write_table(table)
            total_rows += len(chunk)
            logger.info("    wrote chunk %d (%d rows)", i + 1, len(chunk))

    finally:
        if writer is not None:
            writer.close()

    return total_rows


def upload_to_gcs(local_path: str, bucket: str, object_name: str, sa_key_file: str) -> str:
    if gcs_storage is None:
        raise RuntimeError("google-cloud-storage is not installed.")
    if not sa_key_file or not os.path.exists(sa_key_file):
        raise FileNotFoundError(f"GCS service account JSON not found: {sa_key_file}")
    client = gcs_storage.Client.from_service_account_json(sa_key_file)
    blob = client.bucket(bucket).blob(object_name)
    blob.upload_from_filename(local_path)
    uri = f"gs://{bucket}/{object_name}"
    logger.info("    uploaded → %s", uri)
    return uri


def upload_to_s3(local_path: str, bucket: str, object_name: str, s3_conf: Dict[str, Any]) -> str:
    if boto3 is None:
        raise RuntimeError("boto3 is not installed.")
    client = boto3.client(
        "s3",
        region_name=s3_conf.get("region"),
        aws_access_key_id=s3_conf.get("aws_access_key_id"),
        aws_secret_access_key=s3_conf.get("aws_secret_access_key"),
        aws_session_token=s3_conf.get("aws_session_token"),
    )
    client.upload_file(local_path, bucket, object_name)
    uri = f"s3://{bucket}/{object_name}"
    logger.info("    uploaded → %s", uri)
    return uri


def upload_to_azure_blob(local_path: str, az_conf: Dict[str, Any], object_name: str) -> str:
    if BlobServiceClient is None:
        raise RuntimeError("azure-storage-blob is not installed.")
    container = az_conf["container"]

    conn_str = az_conf.get("connection_string")
    if conn_str:
        svc = BlobServiceClient.from_connection_string(conn_str)
    else:
        account_name = az_conf.get("account_name")
        account_key = az_conf.get("account_key")
        if not (account_name and account_key):
            raise ValueError("Azure config must include 'connection_string' or 'account_name' + 'account_key'.")
        account_url = f"https://{account_name}.blob.core.windows.net"
        svc = BlobServiceClient(account_url=account_url, credential=account_key)

    blob_client = svc.get_blob_client(container=container, blob=object_name)
    with open(local_path, "rb") as fh:
        blob_client.upload_blob(fh, overwrite=True)
    url = blob_client.url
    logger.info("    uploaded → %s", url)
    return url


# -----------------------
# Main
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Extract multiple sources to a single Parquet each and upload.")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--chunksize", type=int, default=None, help="Chunk size for extraction")
    parser.add_argument("--dry-run", action="store_true", help="Run extraction but skip upload")
    parser.add_argument("--only-source", action="append", default=[], help="Run only sources whose 'name' matches")
    args = parser.parse_args()

    cfg = load_yaml(args.config)

    db_conf = cfg.get("source_database") or {}
    target_conf = cfg.get("target") or {}
    io_conf = cfg.get("io", {}) or {}
    sources: List[Dict[str, Any]] = cfg.get("sources") or []

    if not db_conf:
        raise ValueError("Missing 'source_database'.")
    if not sources:
        raise ValueError("Missing 'sources' list.")
    if not target_conf:
        raise ValueError("Missing 'target' section.")

    now_utc = dt.datetime.utcnow().replace(tzinfo=None)
    run_ts = now_utc

    engine = create_db_engine(db_conf)

    parquet_dir = io_conf.get("local_temp_dir") or tempfile.gettempdir()
    os.makedirs(parquet_dir, exist_ok=True)
    parquet_compression = io_conf.get("parquet_compression", "snappy")
    chunksize = args.chunksize or io_conf.get("chunksize", 100_000)
    skip_empty_uploads = io_conf.get("skip_empty_uploads", True)

    target_type = (target_conf.get("type") or "").lower()
    gcs_conf = target_conf.get("gcs") or {}
    s3_conf = target_conf.get("s3") or {}
    az_conf = target_conf.get("azure") or {}

    for src in sources:
        name = src.get("name") or f"{src.get('schema','')}.{src.get('table','')}".strip(".") or "query"
        if args.only_source and name not in set(args.only_source):
            continue

        logger.info("Source: %s", name)

        # Window
        last_n_days = src.get("last_n_days")
        if last_n_days:
            n = int(last_n_days)
            # Start of "today" in UTC (00:00)
            today_start_utc = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            to_utc = today_start_utc
            from_utc = today_start_utc - dt.timedelta(days=n)
        else:
            from_utc = None
            to_utc = now_utc  # if no last_n_days, default to "up to now"
        upper_inclusive = bool(src.get("upper_bound_inclusive", False))

        # Statement
        stmt, params = build_statement(engine, src, from_utc, to_utc, upper_inclusive)

        # Names
        local_filename, object_name_override = make_names(src, run_ts, from_utc, to_utc)
        local_path = os.path.join(parquet_dir, local_filename)

        # Format Timestamps
        ts_cols = src.get("timestamp_columns")
        if not ts_cols:
            # default to the primary timestamp_column if present
            ts_cols = [src["timestamp_column"]] if src.get("timestamp_column") else []
        ts_cfg = {
            "columns": ts_cols,
            "utc": bool(src.get("timestamp_utc", True)),
            "naive": bool(src.get("timestamp_naive", True)),
        }

        # Extract
        logger.info("  Extracting → %s", local_path)
        rows = extract_to_parquet(engine, stmt, params, local_path, chunksize, parquet_compression, ts_coerce=ts_cfg)
        logger.info("  Rows: %d", rows)

        # Skip upload if empty
        if skip_empty_uploads and (rows == 0 or not os.path.exists(local_path)):
            logger.info("  Skipping upload: empty extract or file missing.")
            print(f"{name}: rows=0 -> {local_path} (skipped upload)")
            continue

        # Upload
        uploaded_uri = None
        if not args.dry_run and target_type in ("gcs", "s3", "azure"):
            obj_name = object_name_override or maybe_render_global_object_name(
                target_conf.get("object_name"), src, run_ts, from_utc, to_utc
            ) or os.path.basename(local_path)

            if target_type == "gcs":
                uploaded_uri = upload_to_gcs(local_path, gcs_conf["bucket"], obj_name,
                                             gcs_conf.get("service_account_key_file"))
            elif target_type == "s3":
                uploaded_uri = upload_to_s3(local_path, s3_conf["bucket"], obj_name, s3_conf)
            else:  # azure
                uploaded_uri = upload_to_azure_blob(local_path, az_conf, obj_name)

        print(f"{name}: rows={rows} -> {uploaded_uri or local_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(1)
