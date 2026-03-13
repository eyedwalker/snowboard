"""
Upgraded Snowflake connector for agentic tools.
SSL-safe, supports explicit database/schema, shared config.
"""

import snowflake.connector
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


def get_connection_params(database=None, schema=None):
    """Get Snowflake connection params from st.secrets or env vars."""
    try:
        import streamlit as st
        params = {
            "account": st.secrets.get("SNOWFLAKE_ACCOUNT"),
            "user": st.secrets.get("SNOWFLAKE_USER"),
            "password": st.secrets.get("SNOWFLAKE_PASSWORD"),
            "role": st.secrets.get("SNOWFLAKE_ROLE", "FR_DEV_WINDSURF"),
            "warehouse": st.secrets.get("SNOWFLAKE_WAREHOUSE", "DEV_COMPUTE_WINDSURF"),
            "database": database or st.secrets.get("SNOWFLAKE_DATABASE", "DEV_ANALYTICS"),
            "schema": schema or st.secrets.get("SNOWFLAKE_SCHEMA", "DATAWAREHOUSE"),
        }
        if params["account"]:
            return params
    except Exception:
        pass

    password = os.getenv("SNOWFLAKE_PASSWORD")

    # In Lambda, fetch password from Secrets Manager if not in env
    if not password and os.getenv("SNOWFLAKE_PASSWORD_SECRET_ARN"):
        try:
            import boto3
            sm = boto3.client("secretsmanager")
            secret = sm.get_secret_value(SecretId=os.getenv("SNOWFLAKE_PASSWORD_SECRET_ARN"))
            password = secret["SecretString"]
        except Exception as e:
            logger.error(f"Failed to fetch Snowflake password from Secrets Manager: {e}")

    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "eyefinity-dev"),
        "user": os.getenv("SNOWFLAKE_USER", "DEV_WINDSURF"),
        "password": password,
        "role": os.getenv("SNOWFLAKE_ROLE", "FR_DEV_WINDSURF"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DEV_COMPUTE_WINDSURF"),
        "database": database or os.getenv("SNOWFLAKE_DATABASE", "DEV_ANALYTICS"),
        "schema": schema or os.getenv("SNOWFLAKE_SCHEMA", "DATAWAREHOUSE"),
    }


def get_connection(database=None, schema=None):
    """Create a new Snowflake connection with SSL-safe settings."""
    params = get_connection_params(database, schema)
    return snowflake.connector.connect(
        **params,
        insecure_mode=True,
        ocsp_fail_open=True,
        client_session_keep_alive=True,
    )


def execute_query(query, database=None, schema=None, limit=1000, chunk_size=5000):
    """Execute a read-only query and return a DataFrame.

    Uses fetchmany() chunking to avoid SSL/S3 cert failures on large results.
    Automatically appends LIMIT if not present.
    """
    conn = get_connection(database, schema)
    try:
        cursor = conn.cursor()
        if "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]

        chunks = []
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            chunks.extend(rows)

        cursor.close()
        return pd.DataFrame(chunks, columns=columns)
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
