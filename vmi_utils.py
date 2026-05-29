# =============================================================================
# FILE:     vmi_utils.py
# PURPOSE:  Shared utility functions for VMI Analytics platform notebooks.
#           Import this module at the top of every source, core and output
#           notebook instead of defining functions inline.
# AUTHOR:   Conner Ruth
# CREATED:  2026-05-15
# MODIFIED: 2026-05-15
# USAGE:
#   import sys
#   sys.path.insert(0, "/lakehouse/default/Files/utils/")
#   import vmi_utils
#   from vmi_utils import *
# =============================================================================

import pandas as pd
import numpy as np
import math
import os
import traceback
import uuid
import getpass
import json
import pytz
from datetime import datetime, date


def get_current_user(mssparkutils=None):
    """
    Resolves the identity of the user or service account executing the notebook.
    Pass mssparkutils explicitly from the notebook for Fabric-native identity resolution.

    Args:
        mssparkutils: Fabric mssparkutils object. Pass from notebook namespace.

    Returns:
        str: The executing user's display name or identity string.

    Usage:
        executed_by = get_current_user(mssparkutils)
    """
    try:
        if mssparkutils:
            return mssparkutils.runtime.context.get("userName")
        raise Exception("mssparkutils not provided")
    except Exception:
        try:
            from pyspark.sql import SparkSession
            active_spark = SparkSession.getActiveSession()
            if active_spark:
                return active_spark.sql("SELECT current_user()").collect()[0][0]
            raise Exception("No active Spark session")
        except Exception:
            return getpass.getuser()

def get_run_info():
    """
    Returns a dictionary summarizing the current run's metadata.

    Returns:
        dict: Contains run_id, load_date, load_timestamp, source_name,
              target tables and executing user.

    Usage:
        info = get_run_info()
        print(info)
    """
    return {
        "run_id":         RUN_ID,
        "load_date":      LOAD_DATE,
        "load_timestamp": LOAD_TIMESTAMP,
        "source_name":    SOURCE_NAME,
        "raw_table":      RAW_TABLE,
        "stg_table":      STG_TABLE,
        "executed_by":    get_current_user()
    }


def log_message(step, message, level="INFO"):
    """
    Prints a timestamped log message to the notebook output.

    Args:
        step    (str): The notebook section or step name.
        message (str): The message to display.
        level   (str): Severity level — INFO, WARN or ERROR. Defaults to INFO.

    Usage:
        log_message("Extract", "Source file loaded successfully.")
        log_message("Validate Raw", "Missing columns detected.", level="WARN")
    """
    timestamp = datetime.now(pytz.timezone("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [{step}] {message}")


def check_freshness(file_path, freshness_warn_hours, freshness_fail_hours, source_owner):
    """
    Validates that a source file was modified within acceptable thresholds.
    Logs a warning if older than freshness_warn_hours.
    Raises an error if older than freshness_fail_hours.

    Args:
        file_path             (str): Full path to the source file.
        freshness_warn_hours  (int): Hours before warning is logged.
        freshness_fail_hours  (int): Hours before pipeline fails.
        source_owner          (str): Contact if freshness validation fails.

    Raises:
        FileNotFoundError : If the file does not exist at the given path.
        ValueError        : If the file is older than freshness_fail_hours.

    Usage:
        check_freshness(SRC_PATH, FRESHNESS_WARN_HOURS, FRESHNESS_FAIL_HOURS, SOURCE_OWNER)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")

    modified_time        = datetime.fromtimestamp(os.path.getmtime(file_path))
    hours_since_modified = (datetime.now() - modified_time).total_seconds() / 3600

    if hours_since_modified > freshness_fail_hours:
        raise ValueError(
            f"Source file is {hours_since_modified:.1f} hours old — exceeds failure "
            f"threshold of {freshness_fail_hours}h. Contact: {source_owner}"
        )
    elif hours_since_modified > freshness_warn_hours:
        log_message("Freshness",
            f"Source file is {hours_since_modified:.1f} hours old — exceeds warning "
            f"threshold of {freshness_warn_hours}h. Contact: {source_owner}",
            level="WARN")
    else:
        log_message("Freshness",
            f"Source file is {hours_since_modified:.1f} hours old — OK.")


def check_columns(df, expected_columns, required_columns, step="Validate"):
    """
    Compares DataFrame columns against expected and required column lists.
    Logs unexpected or missing columns. Raises an error if any required
    column is absent.

    Args:
        df               (pd.DataFrame): The DataFrame to validate.
        expected_columns (list)        : Full list of expected column names.
        required_columns (list)        : Subset that must be present.
        step             (str)         : Label for log messages.

    Raises:
        ValueError: If any required column is missing from the DataFrame.

    Usage:
        check_columns(df_raw, EXPECTED_COLUMNS, REQUIRED_COLUMNS)
    """
    actual_columns   = set(df.columns.tolist())
    expected_set     = set(expected_columns)
    required_set     = set(required_columns)

    missing_expected = expected_set - actual_columns
    unexpected       = actual_columns - expected_set
    missing_required = required_set - actual_columns

    if unexpected:
        log_message(step,
            f"Unexpected columns present (will be retained): {unexpected}",
            level="WARN")
    if missing_expected:
        log_message(step,
            f"Expected columns not found in source: {missing_expected}",
            level="WARN")
    if missing_required:
        raise ValueError(
            f"Required columns missing from source: {missing_required}")

    log_message(step,
        f"Column check passed. {len(actual_columns)} columns present.")


def check_row_count(df, min_row_count, max_row_count, step="Validate"):
    """
    Validates that a DataFrame's row count falls within expected thresholds.
    Raises an error if below minimum. Logs a warning if above maximum.

    Args:
        df            (pd.DataFrame): The DataFrame to validate.
        min_row_count (int)         : Minimum acceptable row count.
        max_row_count (int)         : Maximum acceptable row count. None to skip.
        step          (str)         : Label for log messages.

    Raises:
        ValueError: If row count is below min_row_count.

    Usage:
        check_row_count(df_raw, MIN_ROW_COUNT, MAX_ROW_COUNT)
    """
    row_count = len(df)

    if row_count < min_row_count:
        raise ValueError(
            f"Row count {row_count} is below minimum threshold of {min_row_count}.")
    if max_row_count and row_count > max_row_count:
        log_message(step,
            f"Row count {row_count:,} exceeds expected maximum of {max_row_count:,}.",
            level="WARN")

    log_message(step, f"Row count check passed: {row_count:,} rows.")


def check_nulls(df, required_columns, step="Validate"):
    """
    Checks that all required columns contain no null values.
    Raises an error if any required field contains nulls.

    Args:
        df               (pd.DataFrame): The DataFrame to validate.
        required_columns (list)        : Columns that must not contain nulls.
        step             (str)         : Label for log messages.

    Raises:
        ValueError: If any required column contains null values.

    Usage:
        check_nulls(df_raw, REQUIRED_COLUMNS)
    """
    failed_columns = []

    for col in required_columns:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            log_message(step,
                f"Required column '{col}' has {null_count:,} null values.",
                level="WARN")
            failed_columns.append(col)

    if failed_columns:
        raise ValueError(
            f"Null values found in required columns: {failed_columns}")

    log_message(step, "Null check passed on all required columns.")


def add_audit_columns(df, src_file_name, run_id, load_date, load_timestamp):
    """
    Appends standard audit columns to a DataFrame before writing to a Delta table.
    All governed tables must carry these columns per platform standards.

    Args:
        df             (pd.DataFrame): The DataFrame to annotate.
        src_file_name  (str)         : The source file name or path to record.
        run_id         (str)         : Unique pipeline run ID.
        load_date      (date)        : Date of pipeline execution.
        load_timestamp (datetime)    : Timestamp of pipeline execution.

    Returns:
        pd.DataFrame: Original DataFrame with audit columns appended.

    Usage:
        df_raw = add_audit_columns(df_raw, SRC_PATH, RUN_ID, LOAD_DATE, LOAD_TIMESTAMP)
    """
    df = df.copy()
    df["load_date"]       = load_date
    df["load_timestamp"]  = load_timestamp
    df["src_file_name"]   = src_file_name
    df["pipeline_run_id"] = run_id
    return df


def log_run(spark, source_name, notebook_type, run_id, load_date, load_timestamp,
            raw_table, stg_table, status, mssparkutils=None,
            rows_raw=0, rows_stg=0, rows_core=0, rows_out=0, message=""):
    """
    Writes a structured run record to the log.pipeline_runs Delta table.
    Called at the end of every notebook execution regardless of success or failure.

    Args:
        spark         (SparkSession): Active Spark session.
        source_name   (str): Source identifier from config.
        notebook_type (str): Type of notebook — source, core or output.
        run_id        (str): Unique pipeline run ID.
        load_date     (date): Date of pipeline execution.
        load_timestamp(datetime): Timestamp of pipeline execution.
        raw_table     (str): Raw table name — None for core/output notebooks.
        stg_table     (str): Stg table name — None for core/output notebooks.
        status        (str): SUCCESS or FAILURE.
        rows_raw      (int): Rows written to raw. Defaults to 0.
        rows_stg      (int): Rows written to stg. Defaults to 0.
        rows_core     (int): Rows written to core. Defaults to 0.
        rows_out      (int): Rows written to output. Defaults to 0.
        message       (str): Error detail on failure. Defaults to empty string.

    Usage:
    log_run(spark, SOURCE_NAME, NOTEBOOK_TYPE, RUN_ID, LOAD_DATE, LOAD_TIMESTAMP,
            RAW_TABLE, STG_TABLE, "SUCCESS",
            mssparkutils=mssparkutils,
            rows_raw=df_raw.shape[0], rows_stg=df_stg.shape[0])
    """
    executed_by = get_current_user(mssparkutils)

    log_entry = pd.DataFrame([{
        "pipeline_run_id" : run_id,
        "notebook_type"   : notebook_type,
        "load_date"       : load_date,
        "load_timestamp"  : load_timestamp,
        "source_name"     : source_name,
        "raw_table"       : raw_table if notebook_type == "source" else None,
        "stg_table"       : stg_table if notebook_type == "source" else None,
        "core_table"      : None,
        "out_table"       : None,
        "status"          : status,
        "rows_raw"        : rows_raw,
        "rows_stg"        : rows_stg,
        "rows_core"       : rows_core,
        "rows_out"        : rows_out,
        "executed_by"     : executed_by,
        "message"         : message
    }])

    spark_log = spark.createDataFrame(log_entry)
    (spark_log.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("log.pipeline_runs"))

    log_message("Log",
        f"Run logged — status: {status} | "
        f"notebook type: {notebook_type} | "
        f"raw rows: {rows_raw:,} | "
        f"stg rows: {rows_stg:,} | "
        f"executed by: {executed_by}"
    )


def register_source(spark, source_name, domain, notebook_type, src_path,
                    src_format, delivery_method, refresh_cadence,
                    expected_columns, business_key, min_row_count, max_row_count,
                    source_owner, load_timestamp, run_id, rows_raw, rows_stg,
                    mssparkutils=None):
    """
    Upserts a record for this source into cfg.source_registry upon successful
    pipeline completion. Inserts a new record if the source does not exist.
    Updates last run metadata if it does.

    Args:
        spark            (SparkSession): Active Spark session.
        source_name      (str): Source identifier.
        domain           (str): Source domain.
        notebook_type    (str): Notebook type.
        src_path         (str): Source file path.
        src_format       (str): Source file format.
        delivery_method  (str): How the file is delivered.
        refresh_cadence  (str): How often the file refreshes.
        expected_columns (list): Expected column list.
        business_key     (list): Business key column list.
        min_row_count    (int): Minimum expected row count.
        max_row_count    (int): Maximum expected row count.
        source_owner     (str): Contact for source issues.
        load_timestamp   (datetime): Timestamp of this run.
        run_id           (str): Unique run ID.
        rows_raw         (int): Rows written to raw table.
        rows_stg         (int): Rows written to stg table.
        mssparkutils     : Fabric mssparkutils object for user identity resolution.
                          Defaults to None.

    Usage:
        register_source(spark, SOURCE_NAME, DOMAIN, NOTEBOOK_TYPE, SRC_PATH,
                       SRC_FORMAT, DELIVERY_METHOD, REFRESH_CADENCE,
                       EXPECTED_COLUMNS, BUSINESS_KEY, MIN_ROW_COUNT, MAX_ROW_COUNT,
                       SOURCE_OWNER, LOAD_TIMESTAMP, RUN_ID,
                       rows_raw=df_raw.shape[0], rows_stg=df_stg.shape[0],
                       mssparkutils=mssparkutils)
    """
    try:
        registry_record = pd.DataFrame([{
            "source_name"         : source_name,
            "domain"              : domain,
            "notebook_type"       : notebook_type,
            "src_path"            : src_path,
            "src_format"          : src_format,
            "delivery_method"     : delivery_method,
            "refresh_cadence"     : refresh_cadence,
            "expected_columns"    : json.dumps(expected_columns),
            "business_key"        : json.dumps(business_key),
            "min_row_count"       : min_row_count,
            "max_row_count"       : max_row_count if max_row_count else 0,
            "source_owner"        : source_owner,
            "first_loaded"        : load_timestamp,
            "last_successful_run" : load_timestamp,
            "last_run_id"         : run_id,
            "last_raw_row_count"  : rows_raw,
            "last_stg_row_count"  : rows_stg,
            "is_active"           : True
        }])

        spark_registry = spark.createDataFrame(registry_record)

        try:
            existing = spark.sql(
                f"SELECT source_name FROM cfg.source_registry "
                f"WHERE source_name = '{source_name}'"
            ).count()
        except Exception:
            existing = 0

        if existing == 0:
            (spark_registry.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable("cfg.source_registry"))
            log_message("Source Registry",
                f"New source registered: {source_name}")
        else:
            spark.sql(f"""
                UPDATE cfg.source_registry
                SET
                    last_successful_run = '{load_timestamp}',
                    last_run_id         = '{run_id}',
                    last_raw_row_count  = {rows_raw},
                    last_stg_row_count  = {rows_stg},
                    src_path            = '{src_path}'
                WHERE source_name = '{source_name}'
            """)
            log_message("Source Registry",
                f"Source registry updated: {source_name}")

    except Exception as error:
        log_message("Source Registry",
            f"Source registry update failed — pipeline success not affected: {error}",
            level="WARN")

def get_sanitized_column_names(file_path, file_format, src_delimiter=",",
                                sheet_name=None, encoding="utf-8"):
    """
    Reads the header row of a source file and prints sanitized column names
    ready to copy directly into EXPECTED_COLUMNS, REQUIRED_COLUMNS and
    COLUMN_RENAMES in Section 2. Run this before filling in config for a
    new source.

    Args:
        file_path     (str): Full path to the source file.
        file_format   (str): 'csv', 'txt', 'tab' or 'excel'.
        src_delimiter (str): Delimiter for csv/txt files. Defaults to ','.
        sheet_name    (str): Excel sheet name if applicable. Defaults to None.
        encoding      (str): File encoding. Defaults to 'utf-8'.

    Usage:
        get_sanitized_column_names(
            file_path   = "/lakehouse/default/Files/raw/cust_inventory/JohnsonBrothers-VMI852.csv",
            file_format = "csv"
        )
    """
    import re

    if file_format in ("csv", "txt"):
        df_sample = pd.read_csv(
            file_path, encoding=encoding, nrows=0,
            dtype=str, index_col=False, sep=src_delimiter)
    elif file_format == "tab":
        df_sample = pd.read_csv(
            file_path, encoding=encoding, nrows=0,
            dtype=str, index_col=False, sep="\t")
    elif file_format == "excel":
        df_sample = pd.read_excel(
            file_path, sheet_name=sheet_name, nrows=0, dtype=str)
    else:
        raise ValueError(
            f"Unsupported format: '{file_format}'. Expected csv, txt, tab or excel.")

    original_names  = df_sample.columns.tolist()
    sanitized_names = (
        pd.Index(original_names)
        .str.strip()
        .str.lower()
        .str.replace(' ', '_', regex=False)
        .str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        .str.replace(r'_+', '_', regex=True)
        .str.strip('_')
        .tolist()
    )

    print("=" * 60)
    print("SANITIZED COLUMN NAMES — copy into config")
    print("=" * 60)

    print("\nEXPECTED_COLUMNS = [")
    for name in sanitized_names:
        print(f'    "{name}",')
    print("]")

    print("\nCOLUMN_RENAMES = {")
    for original, sanitized in zip(original_names, sanitized_names):
        padding = " " * (35 - len(sanitized))
        print(f'    "{sanitized}"{padding}: "{sanitized}",  # source: {repr(original)}')
    print("}")

    print("\nOriginal → Sanitized mapping:")
    for original, sanitized in zip(original_names, sanitized_names):
        print(f"  {repr(original):<40} → {sanitized}")