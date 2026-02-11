import pandas as pd
from sqlalchemy import inspect, text


def get_data_columns(engine, table, ts_col="ts"):
    inspector = inspect(engine)
    cols = inspector.get_columns(table)

    return [
        c["name"]
        for c in cols
        if c["name"].lower() not in ("id", ts_col.lower())
    ]


def fetch_column_data(engine, table, column, start_ts, end_ts, ts_col="ts"):
    try:
        q = text(f"""
            SELECT `{ts_col}` AS ts, `{column}` AS value
            FROM `{table}`
            WHERE `{ts_col}` BETWEEN :start AND :end
            ORDER BY `{ts_col}`
        """)
        return pd.read_sql(q, engine, params={
            "start": start_ts,
            "end": end_ts
        })
    except Exception:
        return pd.DataFrame()


def fetch_latest_ts(engine, table, ts_col="ts"):
    try:
        q = text(f"SELECT MAX(`{ts_col}`) AS ts FROM `{table}`")
        df = pd.read_sql(q, engine)
        return df.iloc[0]["ts"]
    except Exception:
        return None
