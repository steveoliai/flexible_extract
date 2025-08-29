#!/usr/bin/env python3
"""
ETL to Parquet → GCS / S3 / Azure (single file per source)

OPTIMIZED VERSION with:
- Parallel source processing
- Optimized timestamp coercion logic
- Pre-computed Arrow schemas
- Better memory management
- Yesterday's date (YYYY-MM-DD) in file naming

- Multiple sources (table or custom query)
- Optional last N days on a timestamp column
- Upper bound control: < or <=
- Single Parquet per source per run
- Uploads to GCS, S3, or Azure
- Skips upload when empty (configurable via io.skip_empty_uploads)

Template placeholders available:
- {{schema}}, {{table}}, {{run_ts}} - source info and run timestamp
- {{from_ts}}, {{to_ts}} - date range in YYYYMMDD format
- {{yesterday}} - previous day in YYYY-MM-DD format

CLI:
  python etl_to_parquet.py --config config.yml [--chunksize 100000] [--dry-run] [--max-workers 4]
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import threading

import yaml
import pandas as pd
import numpy as np
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

# Thread-local storage for database connections
_thread_local = threading.local()


# -----------------------
# YAML / URL helpers
# -----------------------
def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=128)
def deduce_sqlalchemy_url(
    url: Optional[str], 
    db_type: Optional[str], 
    user: Optional[str], 
    password: Optional[str], 
    host: str, 
    port: Optional[int], 
    database: Optional[str],
    service_name: Optional[str] = None,
    sid: Optional[str] = None,
    odbc_driver: str = "ODBC Driver 17 for SQL Server"
) -> str:
    """Cached URL construction to avoid repeated string operations."""
    if url:
        return url

    db_type = (db_type or "").lower()

    if db_type == "postgresql":
        port = port or 5432
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    if db_type == "mysql":
        port = port or 3306
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    if db_type == "oracle":
        port = port or 1521
        if service_name:
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
        elif sid:
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/{sid}"
        raise ValueError("Oracle config requires 'service_name' or 'sid'.")

    if db_type in ("sql server", "mssql", "sqlserver"):
        port_str = f",{port}" if port else ""
        odbc_str = f"DRIVER={{{{{odbc_driver}}}}};SERVER={host}{port_str};DATABASE={database};UID={user};PWD={password}"
        return str(URL.create("mssql+pyodbc", query={"odbc_connect": odbc_str}))

    raise ValueError(f"Unsupported or missing db.type: {db_type}")


def get_thread_engine(db_conf: Dict[str, Any]) -> Engine:
    """Get or create a database engine for the current thread."""
    if not hasattr(_thread_local, 'engine'):
        # Cache URL construction parameters for efficiency
        sqlalchemy_url = deduce_sqlalchemy_url(
            db_conf.get("url"),
            db_conf.get("type"),
            db_conf.get("user"),
            db_conf.get("password"),
            db_conf.get("host", "localhost"),
            db_conf.get("port"),
            db_conf.get("database"),
            db_conf.get("service_name"),
            db_conf.get("sid"),
            db_conf.get("odbc_driver", "ODBC Driver 17 for SQL Server")
        )
        
        engine_args = db_conf.get("engine_args", {}) or {}
        _thread_local.engine = create_engine(
            sqlalchemy_url,
            pool_size=engine_args.get("pool_size", 3),  # Smaller pools for parallel processing
            max_overflow=engine_args.get("max_overflow", 5),
            future=True,
        )
    return _thread_local.engine


def _guess_epoch_unit_vectorized(int_series: pd.Series) -> str:
    """
    Improved epoch unit guessing using statistical sampling.
    Uses multiple samples and median to avoid outlier bias.
    """
    s = int_series.dropna()
    if s.empty:
        return "s"
    
    # Sample up to 10 values to get a better estimate
    sample_size = min(10, len(s))
    sample_values = s.iloc[:sample_size] if len(s) <= sample_size else s.sample(sample_size)
    median_val = float(sample_values.median())
    
    if median_val < 1e11:    # < 100,000,000,000  -> seconds
        return "s"
    if median_val < 1e14:    # < 100,000,000,000,000 -> milliseconds
        return "ms"
    if median_val < 1e17:    # < 100,000,000,000,000,000 -> microseconds
        return "us"
    return "ns"              # otherwise treat as nanoseconds


def optimize_timestamp_coercion(
    chunk: pd.DataFrame, 
    ts_cols: List[str], 
    coerce_utc: bool = True, 
    make_naive: bool = True
) -> pd.DataFrame:
    """
    Optimized timestamp coercion that processes all timestamp columns efficiently.
    Uses vectorized operations and minimizes memory allocations.
    """
    if not ts_cols:
        return chunk
    
    # Create a copy only if we need to modify timestamp columns
    existing_ts_cols = [col for col in ts_cols if col in chunk.columns]
    if not existing_ts_cols:
        return chunk
    
    # Work on a view first, only copy if we need to modify
    result = chunk
    modifications_needed = False
    
    for col in existing_ts_cols:
        series = chunk[col]
        
        # Skip if already properly formatted datetime64[ns]
        if (pd.api.types.is_datetime64_any_dtype(series) and 
            series.dtype == 'datetime64[ns]' and 
            getattr(series.dtype, "tz", None) is None):
            continue
            
        # We need modifications, so copy the dataframe if we haven't already
        if not modifications_needed:
            result = chunk.copy()
            modifications_needed = True
        
        # Process based on current type
        if pd.api.types.is_datetime64_any_dtype(series):
            # Already datetime, just handle timezone
            if getattr(series.dtype, "tz", None) is not None:
                if coerce_utc:
                    series = series.dt.tz_convert("UTC")
                if make_naive:
                    series = series.dt.tz_localize(None)
            result[col] = series.astype("datetime64[ns]")
            
        elif pd.api.types.is_integer_dtype(series):
            # Integer timestamps - guess unit and convert
            unit = _guess_epoch_unit_vectorized(series)
            result[col] = pd.to_datetime(series, unit=unit, utc=coerce_utc, errors="coerce")
            if make_naive and getattr(result[col].dtype, "tz", None) is not None:
                result[col] = result[col].dt.tz_localize(None)
            result[col] = result[col].astype("datetime64[ns]")
            
        else:
            # String or other format
            converted = pd.to_datetime(series, utc=coerce_utc, errors="coerce")
            if make_naive and getattr(converted.dtype, "tz", None) is not None:
                converted = converted.dt.tz_localize(None)
            result[col] = converted.astype("datetime64[ns]")
    
    return result


def determine_target_schema(
    engine: Engine,
    stmt,
    params: Dict[str, Any],
    ts_cols: List[str],
    sample_size: int = 1000
) -> Optional[pa.Schema]:
    """
    Pre-determine the target Arrow schema by sampling a small amount of data.
    This avoids schema casting on every chunk.
    """
    if not ts_cols:
        return None
    
    try:
        # Get a small sample to determine schema
        sample_df = pd.read_sql_query(stmt, engine, params=params, chunksize=sample_size).__next__()
        if sample_df is None or sample_df.empty:
            return None
        
        # Apply timestamp coercion to sample
        sample_df = optimize_timestamp_coercion(sample_df, ts_cols)
        sample_table = pa.Table.from_pandas(sample_df, preserve_index=False)
        
        # Build target schema with explicit timestamp types
        fields = []
        for name in sample_table.schema.names:
            if name in ts_cols:
                fields.append(pa.field(name, pa.timestamp("us")))
            else:
                fields.append(pa.field(name, sample_table.schema.field(name).type))
        
        return pa.schema(fields)
    
    except Exception as e:
        logger.warning("Could not pre-determine schema, falling back to dynamic: %s", e)
        return None


# -----------------------
# Naming / templating (optimized)
# -----------------------
def render_template(template: str, ctx: Dict[str, str]) -> str:
    """Optimized template rendering using str.format()."""
    try:
        # Convert {{key}} to {key} for str.format()
        format_template = template
        for key in ctx.keys():
            format_template = format_template.replace(f"{{{{{key}}}}}", f"{{{key}}}")
        return format_template.format(**ctx)
    except (KeyError, ValueError):
        # Fallback to original method if format() fails
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
    Return (local_filename, object_name) with optimized templating.
    Uses yesterday's date in YYYY-MM-DD format.
    """
    schema = src.get("schema") or ""
    table = src.get("table") or (src.get("name") or f"query_{uuid.uuid4().hex[:8]}")
    
    # Calculate yesterday's date
    yesterday = (run_ts - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    
    ctx = {
        "schema": schema,
        "table": table,
        "run_ts": run_ts.strftime("%Y%m%d_%H%M%S"),
        "from_ts": (from_ts.strftime("%Y%m%d") if from_ts else ""),
        "to_ts": (to_ts.strftime("%Y%m%d") if to_ts else ""),
        "yesterday": yesterday,
    }

    base = src.get("output_filename") or "{schema}_{table}_{yesterday}_{run_ts}.parquet"
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
    
    # Calculate yesterday's date
    yesterday = (run_ts - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    
    ctx = {
        "schema": schema,
        "table": table,
        "run_ts": run_ts.strftime("%Y%m%d_%H%M%S"),
        "from_ts": (from_ts.strftime("%Y%m%d") if from_ts else ""),
        "to_ts": (to_ts.strftime("%Y%m%d") if to_ts else ""),
        "yesterday": yesterday,
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
    """Build SQL statement and parameters for extraction."""
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
# Optimized extraction
# -----------------------
def extract_to_parquet(
    db_conf: Dict[str, Any],
    stmt,
    params: Dict[str, Any],
    out_path: str,
    chunksize: int,
    parquet_compression: str,
    ts_coerce: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Optimized streaming extraction with pre-computed schema and efficient timestamp handling.
    """
    engine = get_thread_engine(db_conf)
    total_rows = 0
    writer = None
    target_arrow_schema = None

    ts_cols = (ts_coerce or {}).get("columns", []) or []
    coerce_utc = bool((ts_coerce or {}).get("utc", True))
    make_naive = bool((ts_coerce or {}).get("naive", True))

    # Pre-determine target schema if we have timestamp columns
    if ts_cols:
        target_arrow_schema = determine_target_schema(engine, stmt, params, ts_cols)

    try:
        chunk_iter = pd.read_sql_query(stmt, engine, params=params, chunksize=chunksize)
        
        for i, chunk in enumerate(chunk_iter):
            if chunk is None or chunk.empty:
                if i == 0:
                    logger.info("    (no rows)")
                break

            # Optimized timestamp coercion
            if ts_cols:
                chunk = optimize_timestamp_coercion(chunk, ts_cols, coerce_utc, make_naive)

            # Convert to Arrow with pre-determined schema
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            
            # Initialize writer on first chunk
            if writer is None:
                os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
                
                if target_arrow_schema is not None:
                    # Use pre-computed schema
                    table = table.cast(target_arrow_schema)
                    writer = pq.ParquetWriter(out_path, target_arrow_schema, compression=parquet_compression)
                else:
                    writer = pq.ParquetWriter(out_path, table.schema, compression=parquet_compression)
            else:
                # Cast to target schema if needed
                if target_arrow_schema is not None:
                    table = table.cast(target_arrow_schema)

            writer.write_table(table)
            total_rows += len(chunk)
            logger.info("    wrote chunk %d (%d rows)", i + 1, len(chunk))

    finally:
        if writer is not None:
            writer.close()

    return total_rows


# -----------------------
# Cloud upload functions (unchanged but with better error handling)
# -----------------------
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
# Single source processor
# -----------------------
def process_source(
    src: Dict[str, Any],
    db_conf: Dict[str, Any],
    target_conf: Dict[str, Any],
    io_conf: Dict[str, Any],
    run_ts: dt.datetime,
    chunksize: int,
    parquet_compression: str,
    dry_run: bool,
) -> Tuple[str, int, Optional[str], bool]:
    """
    Process a single source. Returns (name, rows, uri, success).
    This function runs in a separate thread.
    """
    name = src.get("name") or f"{src.get('schema','')}.{src.get('table','')}".strip(".") or "query"
    
    try:
        logger.info("Processing source: %s [Thread: %s]", name, threading.current_thread().name)

        # Window calculation
        last_n_days = src.get("last_n_days")
        if last_n_days:
            n = int(last_n_days)
            today_start_utc = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            to_utc = today_start_utc
            from_utc = today_start_utc - dt.timedelta(days=n)
        else:
            from_utc = None
            to_utc = dt.datetime.utcnow().replace(tzinfo=None)
        
        upper_inclusive = bool(src.get("upper_bound_inclusive", False))

        # Build statement
        engine = get_thread_engine(db_conf)
        stmt, stmt_params = build_statement(engine, src, from_utc, to_utc, upper_inclusive)

        # File paths
        local_filename, object_name_override = make_names(src, run_ts, from_utc, to_utc)
        parquet_dir = io_conf.get("local_temp_dir") or tempfile.gettempdir()
        local_path = os.path.join(parquet_dir, local_filename)

        # Timestamp configuration
        ts_cols = src.get("timestamp_columns")
        if not ts_cols:
            ts_cols = [src["timestamp_column"]] if src.get("timestamp_column") else []
        
        ts_cfg = {
            "columns": ts_cols,
            "utc": bool(src.get("timestamp_utc", True)),
            "naive": bool(src.get("timestamp_naive", True)),
        }

        # Extract
        logger.info("  [%s] Extracting → %s", name, local_path)
        rows = extract_to_parquet(
            db_conf, stmt, stmt_params, local_path, chunksize, parquet_compression, ts_coerce=ts_cfg
        )
        logger.info("  [%s] Extracted %d rows", name, rows)

        # Skip upload if empty
        skip_empty_uploads = io_conf.get("skip_empty_uploads", True)
        if skip_empty_uploads and (rows == 0 or not os.path.exists(local_path)):
            logger.info("  [%s] Skipping upload: empty extract", name)
            return name, 0, local_path, True

        # Upload
        uploaded_uri = local_path
        if not dry_run:
            target_type = (target_conf.get("type") or "").lower()
            
            if target_type in ("gcs", "s3", "azure"):
                obj_name = object_name_override or maybe_render_global_object_name(
                    target_conf.get("object_name"), src, run_ts, from_utc, to_utc
                ) or os.path.basename(local_path)

                if target_type == "gcs":
                    gcs_conf = target_conf.get("gcs") or {}
                    uploaded_uri = upload_to_gcs(
                        local_path, gcs_conf["bucket"], obj_name,
                        gcs_conf.get("service_account_key_file")
                    )
                elif target_type == "s3":
                    s3_conf = target_conf.get("s3") or {}
                    uploaded_uri = upload_to_s3(local_path, s3_conf["bucket"], obj_name, s3_conf)
                else:  # azure
                    az_conf = target_conf.get("azure") or {}
                    uploaded_uri = upload_to_azure_blob(local_path, az_conf, obj_name)

        return name, rows, uploaded_uri, True

    except Exception as e:
        logger.exception("Failed to process source %s: %s", name, e)
        return name, 0, None, False


# -----------------------
# Main (parallelized)
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Extract multiple sources to Parquet and upload (parallelized).")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--chunksize", type=int, default=None, help="Chunk size for extraction")
    parser.add_argument("--dry-run", action="store_true", help="Run extraction but skip upload")
    parser.add_argument("--only-source", action="append", default=[], help="Run only sources whose 'name' matches")
    parser.add_argument("--max-workers", type=int, default=4, help="Maximum number of parallel workers")
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

    # Filter sources if requested
    if args.only_source:
        only_set = set(args.only_source)
        sources = [
            src for src in sources 
            if (src.get("name") or f"{src.get('schema','')}.{src.get('table','')}".strip(".") or "query") in only_set
        ]

    if not sources:
        logger.warning("No sources to process after filtering.")
        return

    # Configuration
    now_utc = dt.datetime.utcnow().replace(tzinfo=None)
    run_ts = now_utc
    chunksize = args.chunksize or io_conf.get("chunksize", 100_000)
    parquet_compression = io_conf.get("parquet_compression", "snappy")

    # Ensure temp directory exists
    parquet_dir = io_conf.get("local_temp_dir") or tempfile.gettempdir()
    os.makedirs(parquet_dir, exist_ok=True)

    logger.info("Starting parallel ETL with %d workers for %d sources", args.max_workers, len(sources))

    # Process sources in parallel
    results = []
    successful_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Submit all jobs
        future_to_source = {
            executor.submit(
                process_source,
                src,
                db_conf,
                target_conf,
                io_conf,
                run_ts,
                chunksize,
                parquet_compression,
                args.dry_run,
            ): src
            for src in sources
        }

        # Collect results as they complete
        for future in as_completed(future_to_source):
            src = future_to_source[future]
            try:
                name, rows, uri, success = future.result()
                results.append((name, rows, uri, success))
                if success:
                    successful_count += 1
                    print(f"{name}: rows={rows} -> {uri or 'N/A'}")
                else:
                    failed_count += 1
                    print(f"{name}: FAILED")
            except Exception as e:
                src_name = src.get("name", "unknown")
                logger.exception("Unexpected error processing source %s: %s", src_name, e)
                failed_count += 1
                print(f"{src_name}: FAILED - {e}")

    # Summary
    logger.info("ETL completed: %d successful, %d failed", successful_count, failed_count)
    
    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(1)