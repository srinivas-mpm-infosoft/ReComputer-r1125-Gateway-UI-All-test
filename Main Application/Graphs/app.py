import json
from datetime import datetime, time as dtime
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

from core.config_parser import extract_dashboard_sources
from core.db import get_engine_for_db
from core.query import (
    get_data_columns,
    fetch_column_data,
    fetch_latest_ts
)

st.set_page_config(layout="wide")
st.title("Industrial Live Dashboard")

CARDS_PER_ROW = 3   # 2 for tablets, 3–4 for desktop


# ---------- SIDEBAR ----------
view_mode = st.sidebar.radio(
    "View mode",
    ["Latest available data", "Select date & time range"]
)
refresh = st.sidebar.selectbox("Refresh (sec)", [1, 5, 10,15])

if view_mode == "Select date & time range":
    sd = st.sidebar.date_input("Start date")
    stt = st.sidebar.time_input("Start time", dtime(0, 0))
    ed = st.sidebar.date_input("End date")
    ett = st.sidebar.time_input("End time", dtime(23, 59))

    start_ts = datetime.combine(sd, stt)
    end_ts = datetime.combine(ed, ett)
else:
    start_ts = end_ts = None

st_autorefresh(interval=refresh * 1000, key="refresh")

# ---------- LOAD CONFIG ----------
with open("/home/gateway/GATEWAY-COMPLETE/Demo application/Main Application/config.json") as f:
    cfg = json.load(f)

sources = extract_dashboard_sources(cfg)
db_cfg = cfg["Database"]

tabs = st.tabs([s["name"] for s in sources])

for src_idx, (tab, src) in enumerate(zip(tabs, sources)):
    with tab:
        # ---- DB ENGINE (PER SOURCE) ----
        engine = get_engine_for_db(db_cfg["local"], src["db_name"])

        st.subheader(f"{src['db_name']}.{src['table']}")

        # ---- TIME RANGE RESOLUTION ----
        last_ts = fetch_latest_ts(engine, src["table"])
        if not last_ts:
            st.warning("No data available")
            continue

        if view_mode == "Latest available data":
            start = end = last_ts
        else:
            start, end = start_ts, end_ts

        # ---- DISCOVER DATA COLUMNS ----
        columns = get_data_columns(engine, src["table"])
        if not columns:
            st.warning("No data columns found")
            continue

        # ---- GRID LAYOUT ----
        rows = [
            columns[i:i + CARDS_PER_ROW]
            for i in range(0, len(columns), CARDS_PER_ROW)
        ]

        for row_idx, row_cols in enumerate(rows):
            ui_cols = st.columns(len(row_cols))

            for col_idx, (ui_col, col_name) in enumerate(zip(ui_cols, row_cols)):
                with ui_col:
                    df = fetch_column_data(
                        engine, src["table"], col_name, start, end
                    )

                    st.markdown(f"**{col_name}**")

                    if df.empty:
                        st.caption("No data")
                        continue

                    # ---- VALUE PANEL ----
                    with st.container():
                        st.metric(
                            label="Value",
                            value=df.iloc[-1]["value"],
                            help=str(df.iloc[-1]["ts"])
                        )

                    # ---- MINI GRAPH ----
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df["ts"],
                        y=df["value"],
                        mode="lines",
                        line=dict(width=2)
                    ))

                    fig.update_layout(
                        height=180,
                        margin=dict(l=10, r=10, t=10, b=10),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=False),
                    )

                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        key=f"plot_{src_idx}_{row_idx}_{col_idx}"
                    )

                    # ---- EXPANDABLE FULL GRAPH ----
                    with st.expander("Expand"):
                        big_fig = go.Figure()
                        big_fig.add_trace(go.Scatter(
                            x=df["ts"],
                            y=df["value"],
                            mode="lines+markers"
                        ))
                        big_fig.update_layout(height=400)

                        st.plotly_chart(
                            big_fig,
                            use_container_width=True,
                            key=f"plot_big_{src_idx}_{row_idx}_{col_idx}"
                        )
