#!/usr/bin/env python3

import os
import csv
import json
import time
import subprocess
import logging
from datetime import datetime, date
from io import StringIO
from datetime import timezone, timedelta
import pymysql
from smb.SMBConnection import SMBConnection

# ============================================================
# PATHS
# ============================================================
BASE_DIR = "/home/gateway/Downloads/Munjal-Phase1/Gateway-UI/Gateway-UI/Main Application"
LOG_BASE = "/home/gateway"
CONFIG_FILE = f"{BASE_DIR}/config.json"
UPDATED_FILE = f"{BASE_DIR}/is_file_to_db_updated.json"
LOCAL_MDB = f"{BASE_DIR}/DSM.mdb"
LOG_BASE = f"{LOG_BASE}/logs"

# ============================================================
# TABLE CONFIG
# ============================================================
TABLE_PARAMETER_COLUMN = {
    "RECompactibility": ("Compactibility", "Compactability"),
    "REDCGTemp": ("Temperature", "Temperature"),
    "REDWT": ("Strength", "Strength(WTS)"),
    "REMoisture": ("Moisture", "Moisture"),
    "REPermeability": ("Permeability", "Permeability"),
    "REDSMMaster": ("Strength", "Strength(GCS)"),
}

# ============================================================
# GLOBAL STATE
# ============================================================
FILE_JOBS = []
LAST_RUN = {}
CURRENT_LOG_DATE = None
logger = None


# ============================================================
# LOGGING (DAILY FOLDER ROTATION)
# ============================================================
def setup_logger():
    global logger, CURRENT_LOG_DATE

    today_str = date.today().isoformat()
    if CURRENT_LOG_DATE == today_str:
        return

    CURRENT_LOG_DATE = today_str
    log_dir = f"{LOG_BASE}/{today_str}"
    os.makedirs(log_dir, exist_ok=True)

    log_path = f"{log_dir}/file_to_db.log"

    logger = logging.getLogger("file_to_db")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(log_path)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info("===== LOGGER INITIALIZED =====")


# ============================================================
# UTILS
# ============================================================
def load_json(path, default=None):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ============================================================
# DATE PARSER (REAL DATA SAFE)
# ============================================================
def parse_batch_date(raw):
    if raw is None:
        return None

    raw = str(raw).strip().strip('"').strip("'")
    if not raw:
        return None

    raw = raw.replace("/", "-")

    FORMATS = [
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%Y %H:%M",
        "%m-%d-%y %H:%M:%S",
        "%m-%d-%y %H:%M",
        "%m-%d-%Y",
        "%m-%d-%y",
    ]

    for fmt in FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass

    logger.error(f"[DATE PARSE FAILED] raw='{raw}'")
    return None

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


# ============================================================
# DB HELPERS
# ============================================================
def get_db_conn(cfg):
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        autocommit=False,
        # 🔑 HARD TIME LIMITS
        connect_timeout=3,
        read_timeout=3,
        write_timeout=3,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )


def safe_db_connect(cfg, name):
    try:
        conn = get_db_conn(cfg)
        logger.info(f"[DB] {name} connected")
        return conn
    except Exception as e:
        logger.error(f"[DB] {name} unavailable: {e}")
        return None


def safe_ensure_table(conn, cur, name):
    if not conn or not cur:
        return None, None
    try:
        ensure_sand_lab_table(cur, logger)
        return conn, cur
    except pymysql.err.OperationalError as e:
        logger.error(f"[DB] {name} ensure table failed: {e}")
        try:
            conn.close()
        except:
            pass
        return None, None


def ensure_sand_lab_table(cursor,logger):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sand_lab (
            id INT AUTO_INCREMENT PRIMARY KEY,
            rec_no INT NOT NULL,
            batch_no VARCHAR(100),
            test_id VARCHAR(50),
            test_parameter VARCHAR(100),
            batch_date DATE,
            batch_time VARCHAR(50),
            foundry_line_id VARCHAR(50),
            created_on DATETIME NOT NULL,
            value DOUBLE,
            source_table VARCHAR(100),
            INDEX idx_rec_no (rec_no),
            INDEX idx_source_table (source_table),
            INDEX idx_rec_source (rec_no, source_table)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    logger.info("[DB] Ensured sand_lab table exists")


def fetch_last_rec(cursor, table):
    cursor.execute(
        "SELECT rec_no FROM sand_lab WHERE source_table=%s ORDER BY id DESC LIMIT 1",
        (table,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


# ============================================================
# SMB → MDB
# ============================================================
def fetch_mdb_from_smb(smb_cfg):
    smb_url = smb_cfg["smb_share"]
    parts = smb_url[2:].split("/", 1)
    server = parts[0]
    path = parts[1]
    share, rel_path = path.split("/", 1)

    conn = SMBConnection(
        smb_cfg["share_username"],
        smb_cfg["share_password"],
        "cm4",
        server,
        use_ntlm_v2=True,
        is_direct_tcp=True,
    )

    if not conn.connect(server, 445):
        raise RuntimeError("SMB connection failed")

    with open(LOCAL_MDB, "wb") as f:
        conn.retrieveFile(share, "/" + rel_path, f)

    logger.info(f"[SMB] MDB downloaded → {LOCAL_MDB}")


# ============================================================
# MDB → ROWS
# ============================================================
def read_table_rows(table):
    cmd = ["mdb-export", LOCAL_MDB, table]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    return list(csv.DictReader(StringIO(result.stdout)))


# ============================================================
# LOAD CONFIG
# ============================================================
def load_file_to_db_config():
    global FILE_JOBS, LAST_RUN

    cfg = load_json(CONFIG_FILE, {})
    files = cfg.get("fileToDb", {}).get("files", [])

    FILE_JOBS = []
    LAST_RUN = {}

    for f in files:
        if not f.get("_internal", {}).get("enabled", False):
            continue

        FILE_JOBS.append(
            {
                "smb": f["SMBShare"],
                "freq": f.get("data_freq(in secs)", 60),
                "local_db": f["local_db"],
                "cloud_db": f["cloud_db"],
            }
        )

    logger.info(f"[CONFIG] Loaded {len(FILE_JOBS)} FileToDb jobs")


# ============================================================
# JOB EXECUTION (LOCAL + CLOUD)
# ============================================================
def run_job(job,logger):
    fetch_mdb_from_smb(job["smb"])

    logger.info("[JOB] MDB fetched, processing tables...")
    today = date.today()

    local_conn = safe_db_connect(job["local_db"], "LOCAL")

    cloud_conn = safe_db_connect(job["cloud_db"], "CLOUD")


    local_cur = local_conn.cursor() if local_conn else None
    logger.debug(f"[DB] local_cur={local_cur}")
    cloud_cur = cloud_conn.cursor() if cloud_conn else None
    logger.debug(f"[DB] cloud_cur={cloud_cur}")

    if not local_cur and not cloud_cur:
        logger.error("[JOB] No DB available, skipping job")
        return

    logger.info("[JOB] Processing tables...")
    # 🔑 ENSURE TABLE EXISTS ON BOTH DBS
    local_conn, local_cur = safe_ensure_table(local_conn, local_cur, "LOCAL")
    cloud_conn, cloud_cur = safe_ensure_table(cloud_conn, cloud_cur, "CLOUD")

    if not local_cur and not cloud_cur:
        logger.error("[JOB] No usable DB after table ensure, skipping job")
        return

    logger.info("[JOB] sand_lab table ensured on both DBs")
    last_local = None
    last_cloud = None
    for table, (read_col, store_param) in TABLE_PARAMETER_COLUMN.items():
        rows = read_table_rows(table)
        if local_cur:
                last_local = fetch_last_rec(local_cur, table)
        if cloud_cur:
            last_cloud = fetch_last_rec(cloud_cur, table)

        logger.info(f"[TABLE] {table}: {len(rows)} rows, last_local={last_local}, last_cloud={last_cloud}")
        for r in rows:
            rec_no = None
            value = None
            try:
                rec_no = int(r["rec_no"])
                value = float(r[read_col])
            except Exception:
                continue

            logger.debug(f"[ROW] {table} rec_no={rec_no} value={value}")
            batch_date = None
            batch_date = parse_batch_date(r.get("BatchDate"))
            if batch_date is None:
                continue
            logger.debug(f"[ROW] {table} rec_no={rec_no} batch_date={batch_date}")
            if last_local is None:
                if batch_date != today:
                    continue
            else:
                if rec_no <= last_local:
                    continue

            params = (
                rec_no,
                r.get("BatchNo"),
                (r.get("TestMode") or store_param[:4]).strip(),
                store_param,
                batch_date,
                r.get("BatchTime"),
                r.get("Line"),
                now_ist(),
                value,
                table,
            )

            logger.debug(f"[INSERT] {table} rec_no={rec_no} params={params}")
            # Local DB (authoritative)
            if local_cur:
                local_cur.execute(
                    """
                    INSERT INTO sand_lab
                    (rec_no,batch_no,test_id,test_parameter,
                    batch_date,batch_time,foundry_line_id,
                    created_on,value,source_table)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    params,
                )

                logger.debug(f"[INSERTED] {table} rec_no={rec_no} into local DB")

            # Cloud DB (best effort)
            try:
                if cloud_cur and last_cloud is None or rec_no > last_cloud:
                    cloud_cur.execute(
                        """
                        INSERT INTO sand_lab
                        (rec_no,batch_no,test_id,test_parameter,
                         batch_date,batch_time,foundry_line_id,
                         created_on,value,source_table)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        params,
                    )
                    logger.debug(f"[INSERTED] {table} rec_no={rec_no} into cloud DB")
            except Exception as e:
                logger.error(f"[CLOUD FAIL] rec_no={rec_no} table={table}: {e}")

    if local_conn:
        local_conn.commit()
    if cloud_conn:
        cloud_conn.commit()

    logger.info("[JOB] All tables processed and committed")
    if local_conn:
        local_conn.close()
    if cloud_conn:
        cloud_conn.close()
    logger.info("[JOB] DB connections closed")


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    setup_logger()
    logger.info("[SERVICE] FileToDb watcher started")

    load_file_to_db_config()

    while True:
        try:
            setup_logger()  # rotate log daily

            if load_json(UPDATED_FILE, False):
                logger.info("[CONFIG] Reload requested")
                load_file_to_db_config()
                write_json(UPDATED_FILE, False)

            now = time.time()

            for idx, job in enumerate(FILE_JOBS):
                if now - LAST_RUN.get(idx, 0) < job["freq"]:
                    continue

                logger.info(f"[JOB] Running FileToDb job {idx}")
                run_job(job, logger)
                LAST_RUN[idx] = now

        except Exception as e:
            logger.exception(f"[ERROR] {e}")

        time.sleep(1)


# ============================================================
if __name__ == "__main__":
    main()
