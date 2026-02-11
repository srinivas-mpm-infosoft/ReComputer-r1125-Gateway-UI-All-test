#!/usr/bin/env python3

import json
import asyncio
import time
import multiprocessing as mp
import logging
import socket
import signal
import sys
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
import os

from dbutils.pooled_db import PooledDB
import pymysql

# ============================================================
# PATHS & CONFIG
# ============================================================
BASE_DIR = "/home/gateway/Downloads/Innodose-Gateway-UI/innodose_combined_application"
LOG_ROOT = Path("/home/gateway/logs")
PRODUCT_DIR = "Innodose_DB_Only"


def read_config():
    with open(f"{BASE_DIR}/config.json", "r") as f:
        return json.load(f)


CONFIG = read_config()
PLC_CONFIG = CONFIG["loadcellPlc"]
MODBUS_CONFIG = CONFIG["plc"]


# ============================================================
# LOGGING
# ============================================================
def setup_logger(role):
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = LOG_ROOT / today / PRODUCT_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(f"{PRODUCT_DIR}.{role}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(processName)s] %(levelname)s: %(message)s")
    fh = logging.FileHandler(log_dir / f"{role}.log")
    sh = logging.StreamHandler()

    fh.setFormatter(fmt)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# ============================================================
# DATABASE
# ============================================================
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "mpmgateway",
    "password": "mpmgateway",
    "database": "innodose",
    "charset": "utf8mb4",
    "autocommit": False,
}


def is_db_up():
    try:
        socket.create_connection(("127.0.0.1", 3306), 2).close()
        return True
    except OSError:
        return False


def ensure_tables():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dosing_start (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tag_value TINYINT NOT NULL,
            pouring_time FLOAT,
            innoculant_weight FLOAT,
            timestamp DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            read_time_ms FLOAT
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plc_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            registers INT NOT NULL,
            timestamp DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            read_time_ms FLOAT
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            HUMIDITY_shakeout DOUBLE NOT NULL,
            TEMPERATURE_shakeout DOUBLE NOT NULL,
            read_time DOUBLE NOT NULL
        )
        """
    )

    conn.commit()
    cur.close()
    conn.close()


# ============================================================
# CHILD PROCESSES
# ============================================================
async def plc_loop(cfg, shutdown):
    logger = setup_logger("plc_reader")

    from pycomm3 import SLCDriver

    if not is_db_up():
        logger.error("DB unavailable")
        return

    pool = PooledDB(creator=pymysql, ping=1, **DB_CONFIG)

    with SLCDriver(cfg["ip"]) as plc:
        logger.info("PLC connected")

        while not shutdown.is_set():
            try:
                t0 = time.perf_counter()

                values = {}
                for item in cfg["values"]:
                    res = plc.read(item["address"])
                    values[item["content"]] = res.value

                read_time_ms = (time.perf_counter() - t0) * 1000


                conn = pool.connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO dosing_start
                    (tag_value, pouring_time, innoculant_weight, timestamp, read_time_ms)
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    (
                        1 if values.get("tag") else 0,
                        values.get("pouring_time"),
                        values.get("innoculant_weight"),
                        datetime.now(),
                        round(read_time_ms, 3),
                    ),
                )

                conn.commit()
                conn.close()

                await asyncio.sleep(0.005)

            except Exception:
                logger.exception("PLC error")
                await asyncio.sleep(1)

    logger.info("PLC stopped")


def plc_process(shutdown):
    asyncio.run(plc_loop(PLC_CONFIG, shutdown))


async def modbus_loop(cfg, shutdown):
    logger = setup_logger("modbus_reader")

    from pymodbus.client import AsyncModbusTcpClient
    from datetime import datetime

    client = AsyncModbusTcpClient(cfg["ip"], port=cfg["port"])
    await client.connect()

    pool = PooledDB(creator=pymysql, ping=1, **DB_CONFIG)

    async with client:
        logger.info("Modbus connected")

        while not shutdown.is_set():
            try:
                humidity = None
                temperature = None
                max_read_time_ms = 0.0

                for item in cfg["values"]:
                    name = item.get("content", "").lower()
                    address = int(item["address"])
                    length = int(item["length"])

                    t0 = time.perf_counter()
                    res = await client.read_holding_registers(
                        address=address,
                        count=length,
                    )
                    read_time_ms = (time.perf_counter() - t0) * 1000
                    max_read_time_ms = max(max_read_time_ms, read_time_ms)

                    if res.isError():
                        logger.error("Read failed | %s @ %s", name, address)
                        continue

                    value = res.registers[0]  # <-- explicit, intentional

                    if name == "humidity":
                        humidity = value / 100
                    elif name == "temperature":
                        temperature = value / 20

                    print(
                        f"[{name.upper()}] "
                        f"addr={address} value={humidity if name.upper()=="HUMIDITY" else temperature} "
                        f"read_time={read_time_ms:.3f} ms"
                    )

                # Insert ONLY if both values are present
                if humidity is not None and temperature is not None:
                    conn = pool.connection()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO sensor_data
                        (timestamp, HUMIDITY_shakeout,
                        TEMPERATURE_shakeout, read_time)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            datetime.now(),
                            humidity,
                            temperature,
                            round(max_read_time_ms, 3),
                        ),
                    )
                    conn.commit()
                    conn.close()
                else:
                    logger.warning(
                        "Skipping DB insert (humidity=%s, temperature=%s)",
                        humidity, temperature
                    )

                await asyncio.sleep(5)

            except Exception:
                logger.exception("Modbus error")
                await asyncio.sleep(1)

    logger.info("Modbus stopped")


def modbus_process(shutdown):
    asyncio.run(modbus_loop(MODBUS_CONFIG, shutdown))


# ============================================================
# MAIN
# ============================================================
def main():
    ensure_tables()
    logger = setup_logger("parent")
    shutdown = mp.Event()

    def handle_signal(sig, frame):
        print(f"\n🛑 Signal {sig} received")
        shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGHUP, handle_signal)

    # p1 = mp.Process(target=plc_process, args=(shutdown,), name="PLC")
    p2 = mp.Process(target=modbus_process, args=(shutdown,), name="MODBUS")

    # p1.start()
    p2.start()

    while not shutdown.is_set():
        time.sleep(0.5)

    logger.info("Stopping children")

    # for p in (p1, p2):
    #     p.join(timeout=5)
    #     if p.is_alive():
    #         p.terminate()
    #         p.join()

    for p in (p2):
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
            p.join()

    logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
