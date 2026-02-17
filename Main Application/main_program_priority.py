#!/usr/bin/env python3
"""
Enhanced Data Logger & Uploader for Raspberry Pi CM4 PoE 4G Board

Main responsibilities:
1. Read analog voltage data from ADS1115 ADC and multiple analog channels.
2. Save readings to MySQL.
3. Log readings to CSV (prefer SD card if mounted).
4. Send new data to remote host (TCP) and/or upload via 4G LTE (HTTP POST).
5. Run as a TCP server to accept incoming data.
6. Poll HTTP API for control commands.
7. RS485 send/receive with multiple Modbus slaves.
8. Handle multiple digital I/O channels.
9. Process alarm settings and thresholds.
"""

import os
import io
import time
import csv
import json
import threading
import random
import socket
import subprocess
import struct
import glob
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import re
try:
    import smbus
except ImportError:
    import smbus2 as smbus
import snap7
from snap7.util import get_int, get_real, set_int, set_real
from pymodbus.client import ModbusTcpClient
import base64
import psutil
import serial
from io import StringIO
import tempfile
import pymysql
from dbutils.pooled_db import PooledDB
import requests
import sys
import minimalmodbus
import RPi.GPIO as GPIO
# from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
# from kafka.admin import NewTopic
# from plc.ethernet_ip_gateway import Ethernet_ip
# from plc.S7_protocol import S7_protocol
from smb.SMBConnection import SMBConnection  # pysmb
import pandas as pd
import math
import logging
from logging.handlers import TimedRotatingFileHandler
import mariadb

# from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from threading import Event
import queue

CONFIG_RELOAD_EVENT = Event()
PLC_THREADS = []


class S7_protocol:
    """
    Siemens PLC → MySQL (PyMySQL)

    - Reads DB config from Database.targets
    - Supports multiple DBs (local, cloud)
    - Respects enabled flag
    - Creates table if not exists
    - Adds new columns only (no deletes)
    """

    def __init__(self, config, etl_config=None):
        print("S7:- ", config)

        # ---------------- PLC ----------------
        plc_cfg = config["PLC"]

        self.generate_random = bool(config.get("generate_random", False))

        self.PLC_IP_ADDRESS = plc_cfg["cred"]["ip"]
        self.PLC_RACK = plc_cfg["cred"]["rack"]
        self.PLC_SLOT = plc_cfg["cred"]["slot"]

        self.read_instructions = plc_cfg["address_access"]["read"]
        self.write_instructions = plc_cfg["address_access"].get("write", [])
        self.write_data = False

        # ---------------- RUNTIME ----------------
        self.data_freq = config["PLC"]["data_reading_freq(in secs)"]
        self.log_folder = config["PLC"]["log_path"]

        # ---------------- DATABASE TARGETS ----------------
        self.db_targets = {}

        db_name = plc_cfg["Database"]["db_name"]

        print(f"DB name: {db_name} ")
        for name, db in plc_cfg["Database"]["targets"].items():
            if not db.get("enabled", False):
                continue

            self.db_targets[name] = {
                "cred": db["cred"],
                "database": db_name,          # ✅ FIX
                "table_name": db["table_name"],
                "schema": db["schema"]
            }


        if not self.db_targets:
            raise ValueError("No enabled database targets found")

        # ---------------- PLC CLIENT ----------------
        self.plc = snap7.client.Client()

    # --------------------------------------------------
    # PLC CONNECTION
    # --------------------------------------------------
    def connect_plc(self):
        if not self.plc.get_connected():
            self.plc.connect(
                self.PLC_IP_ADDRESS,
                self.PLC_RACK,
                self.PLC_SLOT
            )
        return self.plc.get_connected()

    # --------------------------------------------------
    # DB CONNECTION (PyMySQL)
    # --------------------------------------------------
    def connect_to_db(self, db_cred, database):
        return pymysql.connect(
            host=db_cred["host"],
            user=db_cred["user"],
            password=db_cred["password"],
            database=database,
            port=db_cred.get("port", 3306),
            charset=db_cred.get("charset", "utf8mb4"),
            autocommit=False,
            connection_timeout=timeout,
        )

        def write_slc_data(self, instruction):
            """Write data into PLC based on instruction"""
            try:
                value = instruction["value_to_write"]
                data_type = instruction["type"]
                size = instruction["size"]

                buffer = bytearray(size)

                if data_type == "int":
                    set_int(buffer, 0, int(value))
                elif data_type == "real":
                    set_real(buffer, 0, float(value))
                else:
                    raise ValueError(f"Unsupported write type: {data_type}")

                if instruction['storage'] == "DB":
                    self.plc.write_area(
                        snap7.types.Areas.DB,
                        instruction["DB_no"],
                        instruction["address"],
                        buffer
                    )
                elif instruction['storage'] == "MK":
                    self.plc.write_area(
                        snap7.types.Areas.MK,
                        0,
                        instruction["address"],
                        buffer
                    )
                else:
                    raise ValueError(f"Unsupported storage: {instruction['storage']}")

                logging.info(f"Wrote {value} ({data_type}) to {instruction}")
                print(f"Wrote {value} ({data_type}) to {instruction}")

            except Exception as e:
                logging.error(f"Error writing to PLC: {e}")
                print(f"Error writing to PLC: {e}")

    # --------------------------------------------------
    # READ PLC DATA
    # --------------------------------------------------
    def get_data(self):
        data_dict = {}
        self.instructions = self.write_instructions if self.write_data else self.read_instructions
        
        try:
            if not self.generate_random:
                if not self.connect_plc():
                    logging.error("PLC connection failed")
                    return None

            for instr in self.read_instructions:
                if instr["storage"] != "DB":
                    continue

                name = instr["content"]

                # ===== RANDOM MODE =====
                if self.generate_random:
                    min_v = safe_float(instr.get("min"), 0)
                    max_v = safe_float(instr.get("max"), 100)
                    if min_v > max_v:
                        min_v, max_v = max_v, min_v

                    value = random.randint(min_v, max_v)
                    logging.info(f"[S7][RANDOM] {name} = {value}")

                # ===== REAL PLC =====
                else:
                        
                    for instruction in self.instructions:
                        if "value_to_write" in instruction:
                            # Perform write
                            self.write_slc_data(instruction)

                        data = self.plc.read_area(
                            snap7.types.Areas.DB,
                            instr["DB_no"],
                            instr["address"],
                            instr["size"]
                        )

                        if instr["type"] == "int":
                            value = get_int(data, 0)
                        elif instr["type"] == "real":
                            value = get_real(data, 0)
                        else:
                            value = data

                data_dict[name] = value

            self.data = data_dict
            return data_dict

        except Exception as e:
            logging.error(f"PLC read error: {e}")
            return None

    # --------------------------------------------------
    # TABLE MANAGEMENT
    # --------------------------------------------------
    def create_table_if_not_exists(self, cursor, table, schema):
        columns = [f"`{col}` {dtype}" for col, dtype in schema.items()]

        query = f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
            {", ".join(columns)}
        ) ENGINE=InnoDB;
        """
        cursor.execute(query)

    def get_existing_columns(self, cursor, table):
        cursor.execute(f"SHOW COLUMNS FROM `{table}`")
        return {row[0] for row in cursor.fetchall()}

    def add_new_columns_only(self, cursor, table, schema):
        existing = self.get_existing_columns(cursor, table)

        for col, dtype in schema.items():
            if col not in existing:
                alter = f"""
                ALTER TABLE `{table}`
                ADD COLUMN `{col}` {dtype} DEFAULT NULL
                """
                cursor.execute(alter)
                logging.info(f"Added column '{col}' to {table}")

    # --------------------------------------------------
    # INSERT DATA
    # --------------------------------------------------
    def send_data_to_db(self, cursor, table, schema):
        cols = []
        values = []

        for col in schema:
            if col in ("id", "ts"):
                continue
            if col in self.data:
                cols.append(f"`{col}`")
                values.append(self.data[col])

        if not cols:
            return

        placeholders = ", ".join(["%s"] * len(cols))
        cols_sql = ", ".join(cols)

        query = f"""
            INSERT INTO `{table}` ({cols_sql})
            VALUES ({placeholders})
        """

        cursor.execute(query, values)


    # --------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------
    def plc_to_db(self):
        os.makedirs(self.log_folder, exist_ok=True)

        log_file = os.path.join(
            self.log_folder,
            f"plc_connection_{datetime.now().strftime('%Y-%m-%d')}.log"
        )


        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s — %(levelname)s — %(message)s"
        )

        if not isinstance(config, dict):
            raise TypeError(f"Invalid PLC config type: {type(config)} → {config}")


        while True:
            start_time = time.time()
            try:
                self.data = self.get_data()
                if not self.data:
                    time.sleep(self.data_freq)
                    continue

                # Write to ALL enabled DBs
                for db_name, db in self.db_targets.items():

                    conn = self.connect_to_db(db["cred"], db["database"])
                    cursor = conn.cursor()

                    self.create_table_if_not_exists(
                        cursor,
                        db["table_name"],
                        db["schema"]
                    )

                    self.add_new_columns_only(
                        cursor,
                        db["table_name"],
                        db["schema"]
                    )

                    self.send_data_to_db(
                        cursor,
                        db["table_name"],
                        db["schema"]
                    )

                    conn.commit()
                    conn.close()

                logging.info("Data stored successfully")

                process_modbus_tcp_alarms(
                    plc_type="Siemens",
                    plc_key=self.db_targets[next(iter(self.db_targets))]["table_name"],
                    data=self.data,
                    config=self.config
                )

            except Exception as e:
                logging.error(f"Main loop error: {e}")

            finally:
                logging.info(f"Cycle time: {time.time() - start_time:.2f}s")
                time.sleep(self.data_freq)


class ModbusTCPProtocol:
    """
    Modbus TCP → MySQL (SYNC)

    Mirrors S7_protocol behavior:
    - Per-cycle bulk read
    - Per-cycle single INSERT
    - Local + Cloud DB routing
    - Schema-driven table creation
    - Column types from config
    """

    def __init__(self, config):
        print("MODBUS:-", config)

        plc_cfg = config["PLC"]

        self.config = config
        self.generate_random = bool(config.get("generate_random", False))

        # ---------------- PLC ----------------
        self.ip = plc_cfg["cred"]["ip"]
        self.port = plc_cfg["cred"].get("port", 502)
        self.read_items = plc_cfg["address_access"]["read"]

        # ---------------- RUNTIME ----------------
        self.data_freq = plc_cfg["data_reading_freq(in secs)"]
        self.log_folder = plc_cfg["log_path"]

        # ---------------- DATABASE TARGETS ----------------
        self.db_targets = {}

        db_name = plc_cfg["Database"]["db_name"]

        log_info(f"[INFO] DB Name: {db_name}")

        for name, db in plc_cfg["Database"]["targets"].items():
            if not db.get("enabled", False):
                continue

            self.db_targets[name] = {
                "cred": db["cred"],
                "database": db_name,          # ✅ FIX
                "table_name": db["table_name"],
                "schema": db["schema"]
            }


        if not self.db_targets:
            raise ValueError("No enabled DB targets for Modbus TCP")

        # ---------------- LOGGING ----------------
        os.makedirs(self.log_folder, exist_ok=True)

        logging.basicConfig(
            filename=os.path.join(
                self.log_folder,
                f"modbus_tcp_{datetime.now().strftime('%Y-%m-%d')}.log"
            ),
            level=logging.INFO,
            format="%(asctime)s — %(levelname)s — %(message)s"
        )

        # ---------------- MODBUS CLIENT ----------------
        self.client = ModbusTcpClient(self.ip, port=self.port)

    # --------------------------------------------------
    # DB CONNECTION
    # --------------------------------------------------

    def connect_to_db(self, db_cred, database):
        return mariadb.connect(
            host=db_cred["host"],
            user=db_cred["user"],
            password=db_cred["password"],
            database=database,
            port=db_cred.get("port", 3306),
            autocommit=False,
            connect_timeout=5
        )


    # --------------------------------------------------
    # TABLE MANAGEMENT (IDENTICAL TO S7)
    # --------------------------------------------------
    def create_table_if_not_exists(self, cursor, table, schema):
        cols = [f"`{c}` {t}" for c, t in schema.items()]
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(cols)}) ENGINE=InnoDB;"
        )

    def add_new_columns_only(self, cursor, table, schema):
        cursor.execute(f"SHOW COLUMNS FROM `{table}`")
        existing = {row[0] for row in cursor.fetchall()}

        for col, dtype in schema.items():
            if col not in existing:
                cursor.execute(
                    f"ALTER TABLE `{table}` ADD COLUMN `{col}` {dtype} DEFAULT NULL"
                )
                logging.info(f"Added column '{col}' to {table}")

    
    # --------------------------------------------------
    # WRITE MODBUS DATA
    # --------------------------------------------------

    def scale_percent_to_range(self,percent, min_v, max_v):
        try:
            percent = float(percent)
        except (TypeError, ValueError):
            raise ValueError("Invalid percentage value")

        if percent < 0 or percent > 100:
            raise ValueError("Percentage must be between 0 and 100")

        min_v = float(min_v)
        max_v = float(max_v)

        if min_v == max_v:
            return min_v

        return min_v + (percent / 100.0) * (max_v - min_v)

    def write_register(self, address, value):
        if not self.client.connect():
            raise ConnectionError(f"Modbus TCP write connect failed {self.ip}:{self.port}")

        response = self.client.write_register(int(address), int(value))

        if response.isError():
            raise RuntimeError(f"Modbus write failed @ {address}: {response}")

        logging.info(f"[MODBUS TCP][WRITE] D{address} = {value}")

    # --------------------------------------------------
    # READ MODBUS DATA (ONE DICT)
    # --------------------------------------------------
    def read_all_data(self):
        data = {}

        # ---------------- RANDOM MODE ----------------
        if self.generate_random:
            for item in self.read_items:
                if not item.get("read", True):
                    continue

                tag = item["tag"]
                min_v = safe_float(item.get("min"), 0)
                max_v = safe_float(item.get("max"), 100)

                if min_v > max_v:
                    min_v, max_v = max_v, min_v

                value = random.randint(int(min_v), int(max_v))
                data[tag] = value

            return data   # 🔥 EXIT EARLY, NO MODBUS

        # ---------------- REAL DEVICE MODE ----------------
        if not self.client.connect():
            raise ConnectionError(
                f"Modbus TCP connection failed {self.ip}:{self.port}"
            )

        # ---------------- WRITE FIRST ----------------
        for item in self.read_items:
            if not item.get("write", False):
                continue

            addr = item["address"]
            percent = item.get("value")

            min_v = safe_float(item.get("min"), 0)
            max_v = safe_float(item.get("max"), 100)

            if percent in ("", None):
                logging.warning(f"Skipping write @ {addr}: empty value")
                continue

            try:
                scaled_value = self.scale_percent_to_range(percent, min_v, max_v)
            except ValueError as e:
                logging.error(f"Invalid write value @ {addr}: {e}")
                continue

            logging.info(
                f"[MODBUS TCP][WRITE] Addr={addr} "
                f"Percent={percent}% → Scaled={scaled_value}"
            )

            self.write_register(addr, int(round(scaled_value)))

        # ---------------- THEN READ ----------------
        for item in self.read_items:
            if not item.get("read", True):
                continue

            res = self.client.read_holding_registers(
                address=int(item["address"]),
                count=int(item.get("length", 1))
            )

            if res.isError():
                raise RuntimeError(
                    f"Modbus read failed @ {item['address']}"
                )

            data[item["tag"]] = res.registers[0]

        self.client.close()
        return data

    # --------------------------------------------------
    # INSERT DATA (ONE ROW)
    # --------------------------------------------------
    def insert_data(self, cursor, table, schema, data):
        cols = []
        vals = []

        for col in schema:
            if col in ("id", "ts"):
                continue
            if col in data:
                cols.append(f"`{col}`")
                vals.append(data[col])

        if not cols:
            return

        placeholders = ", ".join(["%s"] * len(cols))
        cols_sql = ", ".join(cols)

        cursor.execute(
            f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})",
            vals
        )

    # --------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------
    def plc_to_db(self):

        while not CONFIG_RELOAD_EVENT.is_set():
            start_cycle = time.time()

            try:
                data = self.read_all_data()
                logging.info(f"Modbus data: {data}")

                for db in self.db_targets.values():
                    print(f"Connecting to {db}")
                    conn = self.connect_to_db(
                        db["cred"],
                        db["database"]
                    )
                    cursor = conn.cursor()

                    self.create_table_if_not_exists(
                        cursor,
                        db["table_name"],
                        db["schema"]
                    )

                    self.add_new_columns_only(
                        cursor,
                        db["table_name"],
                        db["schema"]
                    )

                    self.insert_data(
                        cursor,
                        db["table_name"],
                        db["schema"],
                        data
                    )

                    conn.commit()
                    conn.close()

                # ---- Alarms (same philosophy as S7) ----
                process_modbus_tcp_alarms(
                    plc_type="Delta",
                    plc_key=next(iter(self.db_targets.values()))["table_name"],
                    data=data,
                )

            except Exception as e:
                logging.exception(f"Modbus TCP loop error: {e}")
                try:
                    self.client.close()
                except:
                    pass

            finally:
                elapsed = time.time() - start_cycle
                time.sleep(max(0, self.data_freq - elapsed))


# Versioning and Synchronization
CONFIG_VERSION = 0
THREAD_LOCK = threading.Lock()
HW_LOCK = threading.Lock()  # Protects I2C and Serial Bus
GPIO_LOCK   = threading.Lock()
I2C_LOCK    = threading.Lock()
RS485_LOCK  = threading.Lock()
DIGITAL_QUEUE = queue.Queue(maxsize=1000)
MODBUS_DB_QUEUE = queue.Queue(maxsize=1000)
STOP_EVENT = threading.Event()
MANAGED_THREADS = []



# Setup comprehensive logging with daily rotation
def setup_logging():
    """Initialize logging with daily rotation and multiple handlers"""
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = Path(f"/home/recomputer/logs/{today}")
    log_dir.mkdir(exist_ok=True)

    # Main logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Console handler with colors
    class ColoredFormatter(logging.Formatter):
        COLORS = {
            "DEBUG": "\033[36m",
            "INFO": "\033[32m",
            "WARNING": "\033[33m",
            "ERROR": "\033[31m",
            "CRITICAL": "\033[35m",
            "ALARM": "\033[91m",
        }
        RESET = "\033[0m"

        def format(self, record):
            if hasattr(record, "levelname"):
                color = self.COLORS.get(record.levelname, "")
                record.levelname = f"{color}[{record.levelname}]{self.RESET}"
            return super().format(record)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        ColoredFormatter(
            "%(asctime)s %(levelname)-8s %(threadName)-12s %(message)s",
            datefmt="%H:%M:%S",
        )
    )

    # Daily rotating file handler
    file_handler = TimedRotatingFileHandler(
        log_dir / "gateway.log",
        when="midnight",
        interval=1,
        backupCount=30,  # Keep 30 days
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s %(threadName)-12s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    # Alarm-specific handler (separate file)
    alarm_handler = TimedRotatingFileHandler(
        log_dir / "alarms.log",
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8",
    )
    alarm_handler.setLevel(logging.WARNING)
    alarm_handler.setFormatter(logging.Formatter("%(asctime)s ALARM %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(alarm_handler)

    # Custom alarm level
    logging.ALARM = 35
    logging.addLevelName(logging.ALARM, "ALARM")

    def alarm(self, message, *args, **kwargs):
        if self.isEnabledFor(logging.ALARM):
            self._log(logging.ALARM, message, args, **kwargs)

    logging.Logger.alarm = alarm

    return logger


# Initialize logging immediately
logger = setup_logging()
logger.info("🚀 Gateway started with daily log rotation enabled")
# === Load full configuration JSON ===


BASE_DIR = (
    "/home/recomputer/Gateway-UI/Main Application"
)
CONFIG_FILE_PATH = f"{BASE_DIR}/config.json"
config = None

def normalize_digital_pins(di_channels, do_channels):
    """
    Convert pin strings → int
    Filter invalid entries safely
    """
    for ch in di_channels:
        try:
            ch["pin"] = int(ch["pin"])
        except Exception:
            ch["pin"] = None

    for ch in do_channels:
        try:
            ch["pin"] = int(ch["pin"])
        except Exception:
            ch["pin"] = None


GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# def setup_digital_gpio(di_channels, do_channels):
#     for ch in di_channels:
#         if ch.get("enabled") and ch.get("pin") is not None:
#             GPIO.setup(ch["pin"], GPIO.IN)

#     for ch in do_channels:
#         if ch.get("pin") is not None:
#             GPIO.setup(ch["pin"], GPIO.OUT)



"""
What: Load the JSON configuration from CONFIG_FILE_PATH and return its dict.
Calls: json.load()
Required by: update_global_config(), start_config_monitor(), any place needing config.
Notes: Returns None on error; caller should handle fallbacks/logging.
Side effects: None.
"""


def load_config():
    try:
        with open(CONFIG_FILE_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"[ERROR] Failed to load config: {e}")
        return None


# === GLOBAL VARIABLE DECLARATIONS (at module level) ===
# Global configuration variables (shared across functions)


# Logging convenience functions to replace all print statements
def log_info(msg):
    logger.info(msg)


def log_debug(msg):
    logger.debug(msg)


def log_warn(msg):
    logger.warning(msg)


def log_error(msg):
    logger.error(msg)


def log_alarm(msg):
    logger.alarm(msg)


UPDATED = False
# Com Port Configuration (global)
CONNECTION_TYPE = None
COM_PORT = None
FIRMWARE = None
SERIAL_NUMBER = None
COM_BAUD_RATE = None

# I/O Settings (global)
MODBUS_ENABLED = None
ANALOG_ENABLED = None
DIGITAL_INPUT_ENABLED = None

# Digital Input Configuration (global)
DIGITAL_SAVE_LOG = None
DIGITAL_MODE = None
DIGITAL_CHANNELS = None
DIGITAL_INPUT_CONFIG = None
DIGITAL_OUTPUT_CHANNELS = None
DIGITAL_COUNTS = None
DIGITAL_TIMES = None

# Analog Configuration (global)
ANALOG_POLLING_INTERVAL = None
ANALOG_POLLING_UNIT = None
ANALOG_SAVE_LOG = None
ANALOG_CHANNELS = None
ANALOG_CONFIG = None
EXTENSION_ADC = None
SCALING_CONFIG = None

# Modbus Configuration (global)
MODBUS_BAUD_RATE = None
MODBUS_DATA_BIT = None
MODBUS_PARITY_MAP = None
MODBUS_PARITY = None
MODBUS_STOP_BIT = None

# Modbus Register configurations (global)
MODBUS_R1_CFG = None
MODBUS_R2_CFG = None
MODBUS_R3_CFG = None
POLLING_INTERVALS = None

# Wireless Configuration (global)
COMMUNICATION_MEDIA = None
SEND_FORMAT = None
FTP_CONFIG = None
WIRELESS_POLLING_INTERVAL = None
WIRELESS_POLLING_UNIT = None
APN = None
JSON_ENDPOINT = None

# WiFi-specific
WIFI_SSID = None
WIFI_PASSWORD = None
WIFI_IP_MODE = None
WIFI_IP = None
WIFI_SUBNET = None
WIFI_GATEWAY = None
WIFI_DNS1 = None
WIFI_DNS2 = None


# Ethernet Configuration (global)
RECV_IP = None
RECV_PORT = None
SEND_TARGETS = None

# PLC configurations
SIEMENS_CONFIGS = []
COMBINED_CONFIG = []

# RS485 Configuration (global)
RS485_PORT = None
RS485_BAUD_RATE = None
MODEM_PORT = "/dev/ttyUSB2"

# Kafka Configuration (global)
KAFKA_BROKERS = None
KAFKA_TOPIC = None
KAFKA_CERT_FILES = None
kafka_topic = None

kafka_producer = None
kafka_last_err = 0

# Alarm Settings (global)
DIGITAL_IO_ALARMS = None
ANALOG_ALARMS = None
MODBUS_ALARMS = None

# Offline Data Configuration (global)
OFFLINE_ENABLED = None
OFFLINE_MODE = None
OFFLINE_FTP = None
OFFLINE_SCHEDULE = None

# Legacy configurations (global)
CSV_FILENAME = None
LAST_SENT_FILE = None
LTE_INTERFACE = None
DATABASE = None
MYSQL_CONFIG = None

# ADC I2C address (global)
ADC_I2C_ADDRESS = None

# Global data storage
data_map = None
analog_readings = None
digital_readings = None
modbus_readings = None

# File to DB
TABLE_PARAMETER_COLUMN = {
    "RECompactibility": ("Compactibility", "Compactability"),
    "REDCGTemp": ("Temperature", "Temperature"),
    "REDWT": ("Strength", "Strength(WTS)"),
    "REMoisture": ("Moisture", "Moisture"),
    "REPermeability": ("Permeability", "Permeability"),
    "REDSMMaster": ("Strength", "Strength(GCS)"),
}

# Global I2C bus (initialize once)
bus = None

# ==================================================
# UTILITY FUNCTIONS
# ==================================================


def apply_serial_settings(inst, settings):
    """
    Apply ModbusRTU.settings to a MinimalModbus instrument.
    settings example:
      { "baudRate": "9600", "parity": "Even", "dataBits": 8, "stopBits": 1 }
    """
    # Defaults
    baud = int(settings.get("baudRate", "9600"))
    parity_name = settings.get("parity", "Even")  # None/Even/Odd/Mark/Space
    data_bits = int(settings.get("dataBits", 8))
    stop_bits = int(settings.get("stopBits", 1))

    inst.serial.baudrate = baud
    inst.serial.bytesize = data_bits

    p = parity_name.lower()
    if p == "none":
        inst.serial.parity = minimalmodbus.serial.PARITY_NONE
    elif p == "even":
        inst.serial.parity = minimalmodbus.serial.PARITY_EVEN
    elif p == "odd":
        inst.serial.parity = minimalmodbus.serial.PARITY_ODD
    elif p == "mark":
        inst.serial.parity = minimalmodbus.serial.PARITY_MARK
    elif p == "space":
        inst.serial.parity = minimalmodbus.serial.PARITY_SPACE
    else:
        inst.serial.parity = minimalmodbus.serial.PARITY_EVEN

    inst.serial.stopbits = stop_bits
    inst.serial.timeout = 1.0


"""
What: Create and configure minimalmodbus.Instrument objects for all enabled Modbus R1/R2/R3 entries.
Calls: minimalmodbus.Instrument()
Required by: main_data_collection_loop() to get instruments; read_modbus_registers() consumes the mapping created here.
Notes: Uses RS485_PORT, MODBUS_BAUD_RATE, MODBUS_DATA_BIT, MODBUS_PARITY, MODBUS_STOP_BIT, and MODBUS_Rx_CFG.
        Keys returned look like 'R1_slave_{id}' / 'R2_slave_{id}' / 'R3_slave_{id}'.
Side effects: Opens serial configuration on instruments.
"""


def setup_modbus_instruments():
    """
    Build MinimalModbus instruments for all enabled RS485 slaves.
    Each slave uses its own RS485 port and serial settings.
    """
    instruments = {}
    log_info("[MODBUS] Initializing RTU instruments")

    brands = get_energy_brand_blocks(config)

    for brand_key, brand in brands:
        log_info(f"[MODBUS] Processing brand: {brand_key}")

        for s in brand.get("slaves", []):

            sid = s.get("id")
            if not sid:
                continue

            if not s.get("enabled", True):
                log_info(f"[MODBUS] Slave {brand_key}_{sid} disabled, skipping")
                continue

            # 🔥 USB slaves handled elsewhere
            if s.get("use_usb") is True:
                log_info(f"[MODBUS] Slave {brand_key}_{sid} uses USB, skipping RTU")
                continue

            port = s.get("rs485_port")
            if not port:
                log_error(f"[MODBUS] Slave {brand_key}_{sid} has no RS485 port")
                continue

            try:
                inst = minimalmodbus.Instrument(port, int(sid))

                # 🔥 APPLY PER-SLAVE SERIAL SETTINGS
                inst.serial.baudrate = int(s.get("baudRate", 9600))
                inst.serial.parity   = {
                    "None": minimalmodbus.serial.PARITY_NONE,
                    "Even": minimalmodbus.serial.PARITY_EVEN,
                    "Odd":  minimalmodbus.serial.PARITY_ODD,
                }.get(s.get("parity", "Even"), minimalmodbus.serial.PARITY_EVEN)

                inst.serial.bytesize = int(s.get("dataBits", 8))
                inst.serial.stopbits = int(s.get("stopBits", 1))
                inst.serial.timeout  = 1.0

                # Optional but recommended
                inst.clear_buffers_before_each_transaction = True
                inst.close_port_after_each_call = False

                key = f"{brand_key}_{sid}"
                instruments[key] = inst

                log_info(
                    f"[MODBUS] Ready: {key} "
                    f"port={port} "
                    f"baud={inst.serial.baudrate} "
                    f"parity={s.get('parity')} "
                    f"dbits={inst.serial.bytesize} "
                    f"sbits={inst.serial.stopbits}"
                )

            except Exception as e:
                log_error(f"[MODBUS][ERROR] {brand_key} slave {sid}: {e}")

    return instruments

"""
What: Convert a numeric interval with unit into seconds.
Calls: None
Required by: main_data_collection_loop() for analog/wireless intervals, and Modbus polling scheduling.
Notes: Units supported: Sec, Min, Hour, Day (default multiplier 1 if unknown).
Side effects: None.
"""


def convert_polling_interval_to_seconds(interval, unit):
    multipliers = {"Sec": 1, "Min": 60, "Hour": 3600, "Day": 86400}
    return interval * multipliers.get(unit, 1)


"""
What: Load config and populate all global variables, initialize I2C bus and in-memory stores.
Calls: load_config(), smbus.SMBus(1)
Required by: start_config_monitor() initially and on config changes; other logic expects globals to be set.
Notes: Sets UPDATED=True on success. Initializes bus (I2C), data_map, analog/digital/modbus reading dicts.
        Parses numerous nested sections from config.json. Sets ADC_I2C_ADDRESS to 0x48.
Side effects: Mutates many globals; opens I2C bus.
Failure modes: Prints errors and returns False; callers should stop or retry.
"""


def update_global_config():
    global config, CONNECTION_TYPE, COM_PORT, FIRMWARE, SERIAL_NUMBER, COM_BAUD_RATE, SIEMENS_CONFIGS, COMBINED_CONFIG
    global MODBUS_ENABLED, ANALOG_ENABLED, DIGITAL_INPUT_ENABLED, MODBUS_TCP_ENABLED
    global DIGITAL_SAVE_LOG, DIGITAL_MODE, DIGITAL_CHANNELS, DIGITAL_COUNTS, DIGITAL_TIMES, DIGITAL_OUTPUT_CHANNELS,DIGITAL_INPUT_CONFIG
    global ANALOG_POLLING_INTERVAL, ANALOG_POLLING_UNIT, ANALOG_SAVE_LOG, ANALOG_CHANNELS,ANALOG_CONFIG
    global EXTENSION_ADC, SCALING_CONFIG, MODBUS_BAUD_RATE, MODBUS_DATA_BIT
    global MODBUS_PARITY, MODBUS_STOP_BIT, MODBUS_R1_CFG, MODBUS_R2_CFG, MODBUS_R3_CFG
    global POLLING_INTERVALS, COMMUNICATION_MEDIA, SEND_FORMAT, FTP_CONFIG
    global WIRELESS_POLLING_INTERVAL, WIRELESS_POLLING_UNIT, APN, JSON_ENDPOINT
    global MODBUS_TCP_MAC, MODBUS_TCP_IP, MODBUS_TCP_SUBNET, MODBUS_TCP_GATEWAY
    global MODBUS_TCP_INTERVAL, MODBUS_TCP_LOG, MODBUS_TCP_TABLE, RECV_IP, RECV_PORT
    global SEND_TARGETS, RS485_PORT, RS485_BAUD_RATE, KAFKA_BROKERS, KAFKA_TOPIC
    global KAFKA_CERT_FILES, DIGITAL_IO_ALARMS, ANALOG_ALARMS, OFFLINE_ENABLED, MODBUS_ALARMS
    global OFFLINE_MODE, OFFLINE_FTP, OFFLINE_SCHEDULE, LTE_INTERFACE, MYSQL_CONFIG
    global CSV_FILENAME, LAST_SENT_FILE, DATABASE
    global ADC_I2C_ADDRESS, data_map
    global analog_readings, digital_readings, modbus_readings, bus, MODBUS_PARITY_MAP
    global UPDATED
    global WIFI_SSID, WIFI_PASSWORD, WIFI_IP_MODE, WIFI_IP, WIFI_SUBNET, WIFI_GATEWAY, WIFI_DNS1, WIFI_DNS2

    log_info("[INFO] 🔄 Updating global configuration variables...")

    # Load fresh config
    config = load_config()
    
    if config is None:
        log_error("[ERROR] Failed to load configuration")
        return False

    # Com Port Configuration
    com_port_cfg = config.get("comPort", {})
    CONNECTION_TYPE = com_port_cfg.get("connectionType", "USB")
    COM_PORT = com_port_cfg.get("comPort", "COM7")
    FIRMWARE = com_port_cfg.get("firmware", "RDL V2.00.1")
    SERIAL_NUMBER = com_port_cfg.get("serial", "RDL0009")
    COM_BAUD_RATE = int(com_port_cfg.get("baudRate", "19200"))

    # I/O Settings
    io_settings = config.get("ioSettings", {})
    settings = io_settings.get("settings", {})
    MODBUS_ENABLED = settings.get("modbus", True)
    ANALOG_ENABLED = settings.get("analog", True)
    DIGITAL_INPUT_ENABLED = settings.get("digitalInput", True)
    MODBUS_TCP_ENABLED = settings.get("modbusTCP", False)

    # Digital Input Configuration
    digital_input_cfg = io_settings.get("digitalInput", {})
    DIGITAL_INPUT_CONFIG = digital_input_cfg
    digital_output_cfg = io_settings.get("digitalOutput", {})
    DIGITAL_SAVE_LOG = digital_input_cfg.get("saveLog", False)
    DIGITAL_MODE = digital_input_cfg.get("mode", "time")
    DIGITAL_CHANNELS = digital_input_cfg.get("channels")
    DIGITAL_OUTPUT_CHANNELS = digital_output_cfg.get("channels")
    normalize_digital_pins(
        DIGITAL_CHANNELS,
        DIGITAL_OUTPUT_CHANNELS
    )
    # setup_digital_gpio(DIGITAL_CHANNELS, DIGITAL_OUTPUT_CHANNELS)

    DIGITAL_COUNTS = digital_input_cfg.get("counts", [1, 0, 0, 0])
    DIGITAL_TIMES = digital_input_cfg.get("times", [0, 5, 0, 0])

    # Analog Configuration
    analog_cfg = io_settings.get("analog", {})
    ANALOG_POLLING_INTERVAL = analog_cfg.get("pollingInterval", 30)
    ANALOG_POLLING_UNIT = analog_cfg.get("pollingIntervalUnit", "Sec")
    ANALOG_SAVE_LOG = analog_cfg.get("saveLog", True)
    ANALOG_CHANNELS = analog_cfg.get("channels", [])
    ANALOG_CONFIG = analog_cfg
    print(ANALOG_CONFIG)
    EXTENSION_ADC = analog_cfg.get("extensionADC", {})
    SCALING_CONFIG = analog_cfg.get("scaling", {})

    # Modbus Configuration
    modbus_cfg = config.get("modbus", {})
    modbus_com = modbus_cfg.get("comPort", {})
    MODBUS_BAUD_RATE = int(modbus_com.get("baudRate", "9600"))
    MODBUS_DATA_BIT = (
        int(modbus_com.get("dataBit", "8 bit").split()[0])
        if modbus_com.get("dataBit")
        else 8
    )
    MODBUS_PARITY_MAP = {
        "None": "N",
        "Even": "E",
        "Odd": "O",
        "Mark": "M",
        "Space": "S",
    }
    MODBUS_PARITY = MODBUS_PARITY_MAP.get(modbus_com.get("parity", "None"), "N")
    MODBUS_STOP_BIT = (
        int(modbus_com.get("stopBit", "1 bit").split()[0])
        if modbus_com.get("stopBit")
        else 1
    )

    # Modbus Register configurations
    MODBUS_R1_CFG = modbus_cfg.get("modbusR1", {})
    MODBUS_R2_CFG = modbus_cfg.get("modbusR2", {})
    MODBUS_R3_CFG = modbus_cfg.get("modbusR3", {})
    POLLING_INTERVALS = modbus_cfg.get("pollingInterval", {})

    # Wireless Configuration
    wireless_cfg = config.get("wireless", {})
    COMMUNICATION_MEDIA = wireless_cfg.get("communicationMedia", "4G/LTE")
    SEND_FORMAT = wireless_cfg.get("sendFormat", "MQTT")
    FTP_CONFIG = wireless_cfg.get("ftp", {})
    WIRELESS_POLLING_INTERVAL = wireless_cfg.get("pollingInterval", 5)
    WIRELESS_POLLING_UNIT = wireless_cfg.get("pollingTimeUnit", "Sec")
    APN = wireless_cfg.get("apn", "airtelgprs.com")
    JSON_ENDPOINT = wireless_cfg.get(
        "jsonEndpoint", "https://github.com/srinivas2200030392"
    )

    # WiFi Credentials
    WIFI_SSID = wireless_cfg.get("wifiSsid", "")
    WIFI_PASSWORD = wireless_cfg.get("wifiPassword", "")

    # WiFi IP Configuration
    WIFI_IP_MODE = wireless_cfg.get("wifiIpMode", "DHCP")
    WIFI_IP = wireless_cfg.get("wifiIp", "")
    WIFI_SUBNET = wireless_cfg.get("wifiSubnet", "")
    WIFI_GATEWAY = wireless_cfg.get("wifiGateway", "")
    WIFI_DNS1 = wireless_cfg.get("wifiDns1", "")
    WIFI_DNS2 = wireless_cfg.get("wifiDns2", "")

    # --- Start of PLC Configuration Processing ---
    plc_configurations = config.get("plc_configurations", [])
    DATABASE = config.get("Database")

    # ---- Central Database Config ----
    global_db = config.get("Database", {})
    if not global_db:
        raise ValueError("Central Database config missing")

    # Clear previous configs
    SIEMENS_CONFIGS.clear()
    COMBINED_CONFIG.clear()


    def normalize_item(item, plc_type):
        """
        Normalize Siemens / Allen-Bradley read items
        """
        if plc_type == "Siemens":
            return {
                "name": item["content"],
                "type": item["type"].lower()
            }

        elif plc_type in ["Allen Bradley","Delta"]:
            return {
                "name": item["tag"],
                "type": item["datatype"].lower()
            }

        raise ValueError(f"Unsupported PLC type: {plc_type}")


    def build_schema(read_items, plc_type):
        schema = {
            "id": "BIGINT AUTO_INCREMENT PRIMARY KEY",
            "ts": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        }

        for item in read_items:
            normalized = normalize_item(item, plc_type)
            name = normalized["name"]
            dtype = normalized["type"]

            if dtype in ("real", "float","int", "dint", "word", "uint"):
                schema[name] = "DOUBLE DEFAULT NULL"
            elif dtype in ("bool", "boolean"):
                schema[name] = "TINYINT(1) DEFAULT NULL"
            else:
                schema[name] = "VARCHAR(255) DEFAULT NULL"

        return schema

    for plc_entry in plc_configurations:
        plc_type = plc_entry.get("plcType")
        plc_data = plc_entry.get("PLC", {})

        # ---- Validate table name ----
        table_name = plc_data.get("Database", {}).get("table_name")
        if not table_name:
            raise ValueError(f"{plc_type} PLC missing Database.table_name")

        # ---- Build schema ----
        read_items = plc_data.get("address_access", {}).get("read", [])
        schema = build_schema(read_items, plc_type)
        plc_db_cfg = plc_data["Database"]

        plc_data["Database"]["targets"] = {}

        # LOCAL
        if plc_db_cfg.get("upload_local"):
            plc_data["Database"]["targets"]["local"] = {
                "enabled": True,
                "cred": DATABASE["local"]["cred"],
                "database": plc_db_cfg["db_name"],
                "table_name": plc_db_cfg["table_name"],
                "schema": schema
            }

        # CLOUD
        if plc_db_cfg.get("upload_cloud"):
            plc_data["Database"]["targets"]["cloud"] = {
                "enabled": True,
                "cred": DATABASE["cloud"]["cred"],
                "database": plc_db_cfg["db_name"],
                "table_name": plc_db_cfg["table_name"],
                "schema": schema
            }

        if not plc_data["Database"]["targets"]:
            raise ValueError(f"{plc_type} PLC has no enabled DB targets")

        for db_name, db in global_db.items():

            plc_data["Database"]["targets"][db_name] = {
                "cred": db["cred"],
                "table_name": table_name,
                "schema": schema,
                "enabled":db["enabled"]
            }

        if not plc_data["Database"]["targets"]:
            raise ValueError("No enabled database targets found")

        # ---- Common runtime settings ----
        plc_data["log_path"] = "/home/recomputer/logs"
        plc_data["data_reading_freq(in secs)"] = plc_data["data_freq_sec"]

        # ---- Route PLCs ----
        if plc_type == "Siemens":
            SIEMENS_CONFIGS.append({"PLC": plc_data,"generate_random": plc_entry.get("generate_random", False)})

        elif plc_type in ["Allen Bradley","Delta"]:
            COMBINED_CONFIG.append({"PLC": plc_data,"generate_random": plc_entry.get("generate_random", False)})

        else:
            raise ValueError(f"Unknown PLC type: {plc_type}")


    # print(SIEMENS_CONFIGS)
    log_info(f"[INFO] Loaded {len(SIEMENS_CONFIGS)} Siemens configurations.")
    log_info(f"[INFO] Loaded {len(COMBINED_CONFIG)} Combined configurations")

    # Ethernet Configuration
    ethernet_cfg = config.get("ethernet", {})
    RECV_IP = ethernet_cfg.get("recvIp", "0.0.0.0")
    RECV_PORT = ethernet_cfg.get("recvPort", 12345)
    SEND_TARGETS = ethernet_cfg.get("sendTargets", [])

    # RS485 Configuration
    rs485_cfg = config.get("rs485", {})
    RS485_PORT = rs485_cfg.get("port", "/dev/ttyAMA5")
    RS485_BAUD_RATE = int(rs485_cfg.get("baud", 115200))

    # Kafka Configuration
    kafka_cfg = config.get("kafka", {})
    KAFKA_BROKERS = kafka_cfg.get("brokers", ["broker1:9092"])
    KAFKA_TOPIC = kafka_cfg.get("topic", "create-commands")
    KAFKA_CERT_FILES = kafka_cfg.get("certFiles", {})

    # Alarm Settings
    alarm_settings = config.get("alarmSettings", {})
    DIGITAL_IO_ALARMS = alarm_settings.get("Digital I/O", {})
    ANALOG_ALARMS = alarm_settings.get("Analog", {})
    MODBUS_ALARMS = alarm_settings.get("Modbus", {})

    # Offline Data Configuration
    offline_data_cfg = config.get("offlineData", {})
    OFFLINE_ENABLED = offline_data_cfg.get("enabled", True)
    OFFLINE_MODE = offline_data_cfg.get("mode", "schedule")
    OFFLINE_FTP = offline_data_cfg.get("ftp", {})
    OFFLINE_SCHEDULE = offline_data_cfg.get("schedule", {})

    # Legacy configurations
    CSV_FILENAME = "data_log.csv"
    LAST_SENT_FILE = (
        "/home/gateway/GATEWAY-COMPLETE/Demo application/Main Application/last_sent.txt"
    )
    LTE_INTERFACE = config.get("lteInterface", "usb0")
    MYSQL_CONFIG = config.get(
        "mysqlConfig",
        {
            "user": "gateway",
            "password": "gateway",
            "host": "localhost",
            "database": "gateway",
        },
    )
    # ADC I2C address
    ADC_I2C_ADDRESS = 0x48

    # Data storage initialization
    if data_map is None:
        data_map = {}
    if analog_readings is None:
        analog_readings = {}
    if digital_readings is None:
        digital_readings = {}
    if modbus_readings is None:
        modbus_readings = {}

    # Initialize I2C bus if not already done
    if bus is None:
        try:
            bus = smbus.SMBus(1)
        except Exception as e:
            log_error(f"[ERROR] Failed to initialize I2C bus: {e}")

    log_info("[INFO] ✅ Global configuration variables updated successfully")
    log_info(
        f"[INFO] Key settings - Analog: {ANALOG_ENABLED}, Digital: {DIGITAL_INPUT_ENABLED}"
    )
    log_info(
        f"[INFO] Key settings - Modbus: {MODBUS_ENABLED}, ModbusTCP: {MODBUS_TCP_ENABLED}"
    )
    try:
        f2db = config.get("fileToDb", {}) or {}
        file_items = f2db.get("files", []) or []
        for fcfg in file_items:
            # start background poll thread per file
            log_info(f"FCFG: {fcfg}")
            if fcfg.get("_internal").get("enabled"):
                t = threading.Thread(
                    target=file_to_db_poll_loop, args=(fcfg,), daemon=True
                )
                t.start()
        log_info(f"[INFO] Started {len(file_items)} file-to-db poller(s).")
    except Exception as e:
        log_error(f"[ERROR] Failed to start file-to-db pollers: {e}")

    UPDATED = True
    # connect_wifi()
    return True


"""
What: Check is_updated.json for a text boolean ('true'/'false') within first two lines to trigger config reload.
Calls: Path.exists(), built-in file I/O
Required by: config_monitor_loop()
Notes: Returns True/False/None. Uses first two lines but substitutes second into first if present.
Side effects: None.
"""


def read_update_flag(file_path=f"{BASE_DIR}/is_updated.json"):
    try:
        update_file = Path(file_path)
        if not update_file.exists():
            return None

        with open(update_file, "r") as f:
            lines = []
            for _ in range(1):  # read first 2 lines only
                line = f.readline()
                if not line:
                    break
                lines.append(line.strip().lower())

        log_info(f"First two lines: {lines}")
        lines[0] = lines[1] if len(lines) > 1 else lines[0]
        # interpret the first line only as boolean flag
        if lines and lines[0].startswith("true"):
            return True
        elif lines and lines[0].startswith("false"):
            return False
        else:
            return None

    except Exception as e:
        log_error(f"[ERROR] Failed to read update flag: {e}")
        return None


"""
What: Write 'false' to is_updated.json to acknowledge config reload completion.
Calls: built-in file I/O
Required by: config_monitor_loop() after successful update_global_config().
Notes: Prevents repeated reloads.
Side effects: Overwrites file content.
"""


def clear_update_flag(file_path=f"{BASE_DIR}/is_updated.json"):
    try:
        with open(file_path, "w") as f:
            f.write("false")
        log_info(f"[INFO] Cleared update flag in {file_path}")
    except Exception as e:
        log_error(f"[ERROR] Failed to clear update flag: {e}")

def _connect_smb_and_open(smb_cfg, file_cfg=None):
    # smb_cfg: dict with smb_share (//host/share), creds
    # file_cfg: parent dict, used to pick file_keyword
    try:
        smb_url = smb_cfg.get("smb_share", "")
        if not smb_url.startswith("//"):
            return None, None

        # Split into server + share
        parts = smb_url[2:].split("/", 1)
        server = parts[0]
        share_and_path = parts[1] if len(parts) > 1 else ""
        if "/" in share_and_path:
            share_name, share_rel_path = share_and_path.split("/", 1)
        else:
            share_name, share_rel_path = share_and_path, ""

        user = smb_cfg.get("share_username") or ""
        pwd = smb_cfg.get("share_password") or ""
        client_name = os.uname().nodename if hasattr(os, "uname") else "client"
        server_name = server

        conn = SMBConnection(user, pwd, client_name, server_name, use_ntlm_v2=True, is_direct_tcp=True)
        if not conn.connect(server, 445, timeout=30):
            print("[WARN] SMB Connection failed")
            return None, None

        # Build file path
        if share_rel_path:  
            # Full path already provided
            read_path = share_rel_path
        else:
            # No file path → use file_keyword
            keyword = ""
            if file_cfg:
                keyword = file_cfg.get("file_details", {}).get("file_keyword", "")
            if not keyword:
                print("[WARN] No file_keyword provided for SMB file lookup")
                return None, None
            read_path = keyword + ".csv"

        fbuf = io.BytesIO()
        conn.retrieveFile(share_name, "/" + read_path.lstrip("/"), fbuf)
        fbuf.seek(0)
        return fbuf.read(), os.path.basename(read_path)

    except Exception as e:
        print(f"[WARN] SMB read failed: {e}")
        return None, None

def _load_file_to_df(file_bytes, file_path_hint, file_type):
    # Supports json, csv, excel based on file_type or extension
    try:
        if file_type == "json" or (not file_type and str(file_path_hint).lower().endswith(".json")):
            # JSON can be array or lines
            try:
                data = json.loads(file_bytes.decode("utf-8"))
                return pd.json_normalize(data) if isinstance(data, list) else pd.json_normalize([data])
            except Exception:
                # Try line-delimited JSON
                return pd.read_json(io.BytesIO(file_bytes), lines=True)
        elif file_type == "csv" or str(file_path_hint).lower().endswith(".csv"):
            return pd.read_csv(io.BytesIO(file_bytes))
        elif file_type == "excel" or str(file_path_hint).lower().endswith((".xlsx", ".xls")):
            return pd.read_excel(io.BytesIO(file_bytes))
        else:
            # Fallback try JSON
            data = json.loads(file_bytes.decode("utf-8"))
            return pd.json_normalize(data) if isinstance(data, list) else pd.json_normalize([data])
    except Exception as e:
        print(f"[ERROR] Failed to parse file: {e}")
        return pd.DataFrame()

def _apply_datetime_handling(df, internal_cfg):
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    created_on = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
    df["created_on"] = pd.to_datetime(created_on)
    # Combine date/time if separate
    if internal_cfg and internal_cfg.get("hasDatetime"):
        dtinfo = internal_cfg.get("datetimeInfo") or {}
        if dtinfo.get("type") == "combined":
            col = dtinfo.get("datetimeColumn")
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        elif dtinfo.get("type") == "separate":
            dcol = dtinfo.get("dateColumn")
            tcol = dtinfo.get("timeColumn")
            if dcol in df.columns and tcol in df.columns:
                # Combine date and time into datetime
                df["datetime"] = pd.to_datetime(df[dcol].astype(str) + " " + df[tcol].astype(str), errors="coerce", utc=True)  # [6][12][15]
    return df

def _infer_schema_from_df(df, table_name):
    # Build MySQL schema dict: id, file_name, created_on + columns
    schema = {
        "id": "INT AUTO_INCREMENT PRIMARY KEY",  # [7][13]
        "file_name": "VARCHAR(255)",
        "created_on": "TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)"
    }
    for col, dtype in df.dtypes.items():
        col_l = str(col).lower()
        if col in ("id", "file_name", "created_on"):
            continue
        if "datetime" in col_l or "time" in col_l or str(dtype).startswith(("datetime64", "datetime")):
            schema[col] = "DATETIME"
        elif "int" in str(dtype):
            schema[col] = "INT"
        elif "float" in str(dtype) or "double" in str(dtype):
            schema[col] = "DOUBLE"
        else:
            # default text size
            schema[col] = "VARCHAR(255)"
    return schema

def _ensure_table(conn, db_name, table_name, schema):
    cur = conn.cursor()
    print("Create table started")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    cur.execute(f"USE `{db_name}`")
    # Create table if not exists with base columns; then add missing columns
    cols_sql = ", ".join([f"`{c}` {t}" for c, t in schema.items()])
    cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols_sql}) ENGINE=InnoDB")
    # Ensure columns exist
    cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing = {row for row in cur.fetchall()}
    existing = [i[0] for i in existing]
    print("Existing: ",existing)
    for c, t in schema.items():
        if c not in ["id"] and c not in existing:
            print("Not Existing",c,t)
            cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` {t}")
    conn.commit()
    cur.close()

def _make_row_hash(row, key_cols):
    # Build a simple hashable tuple for dedup
    return tuple((k, row.get(k)) for k in key_cols if k in row)

def _insert_new_rows(conn, db_name, table, df, filter_cols, file_name_value):
    if df.empty:
        return 0

    cur = conn.cursor(dictionary=True)
    cur.execute(f"USE `{db_name}`")

    # Detect datetime columns
    dt_cols = [c for c in df.columns if "datetime" in c.lower()]

    if dt_cols:
        # Use the first datetime column for dedup
        dt_col = dt_cols[0]

        # Get distinct datetime values in the dataframe
        incoming_dt = df[dt_col].dropna().unique().tolist()
        if not incoming_dt:
            cur.close()
            return 0

        # Check if any of these already exist in DB
        placeholders = ", ".join(["%s"] * len(incoming_dt))
        cur.execute(
            f"SELECT `{dt_col}` FROM `{table}` WHERE `{dt_col}` IN ({placeholders}) LIMIT 1",
            tuple(incoming_dt),
        )
        exists = cur.fetchone()

        if exists:
            cur.close()
            return 0  # Skip entire insert

    # Otherwise, proceed with normal dedup based on filter_cols
    if filter_cols:
        key_list = ", ".join([f"`{c}`" for c in filter_cols if c in df.columns])
        cur.execute(f"SELECT {key_list} FROM `{table}` ORDER BY id DESC LIMIT 10000")
        existing = {_make_row_hash(r, filter_cols) for r in cur.fetchall()}
    else:
        key_cols = df.columns[:1].tolist()
        key_list = ", ".join([f"`{c}`" for c in key_cols])
        cur.execute(f"SELECT {key_list} FROM `{table}` ORDER BY id DESC LIMIT 10000")
        existing = {_make_row_hash(r, key_cols) for r in cur.fetchall()}

    insert_cols = list(df.columns)
    if "file_name" not in insert_cols:
        insert_cols = ["file_name"] + insert_cols

    rows = []
    for _, r in df.iterrows():
        rd = r.to_dict()
        rh = _make_row_hash(rd, filter_cols or key_cols)
        if rh in existing:
            continue
        existing.add(rh)
        rd["file_name"] = file_name_value or ""
        rows.append([rd.get(c) for c in insert_cols])

    if not rows:
        cur.close()
        return 0

    placeholders = ", ".join(["%s"] * len(insert_cols))
    cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
    cur.executemany(
        f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})",
        rows,
    )
    conn.commit()
    cur.close()
    return len(rows)

def _poll_file_source_loop(file_cfg):
    # Runs in thread, polls each data_freq seconds
    db_name = "sensor_readings"  # configurable if needed
    table_name = file_cfg.get("storing_database", {}).get("table_name") or file_cfg.get("file_details", {}).get("file_keyword") or "file_data"
    freq = int(file_cfg.get("data_freq(in secs)", 60))
    log_path = file_cfg.get("log_file_path", "")
    file_type = file_cfg.get("file_details", {}).get("file_type", "json")
    internal = file_cfg.get("_internal", {}) or {}
    skip_lines = int(internal.get("skipLines", 0))
    columns = file_cfg.get("file_details", {}).get("columns_to_fetch") or []
    filter_field = internal.get("filterField") or None
    smb_cfg = file_cfg.get("SMBShare", {}) or {}

    # MySQL connection (adjust as per deployment)
    conn = mysql.connector.connect(user="grafana", password="grafana_pass", host="localhost", database=db_name)
    last_schema = None

    while True:
        try:
            file_bytes = None
            file_name = None
            # Prefer explicit log_file_path local file if exists
            if log_path and os.path.exists(log_path):
                with open(log_path, "rb") as f:
                    file_bytes = f.read()
                file_name = os.path.basename(log_path)
                print("Log Path: ",file_name)
            else:
                # Try SMB if configured
                if smb_cfg.get("smb_share"):
                    file_bytes, file_name = _connect_smb_and_open(smb_cfg,file_cfg)
                    # If no target_path in SMB config and local_mount_point is provided and exists, try that too
                    # print("File bytes: ",file_bytes)
                    print("File Name: ",file_name)
                if not file_bytes and smb_cfg.get("local_mount_point") and os.path.exists(smb_cfg["local_mount_point"]):
                    mp = smb_cfg["local_mount_point"]
                    # If file_keyword is present, prefer matching file
                    file_keyword = file_cfg.get("file_details", {}).get("file_keyword") or ""
                    candidate = None
                    if os.path.isdir(mp):
                        # find first matching file by keyword
                        for fn in os.listdir(mp):
                            if not file_keyword or file_keyword in fn:
                                candidate = os.path.join(mp, fn)
                                break
                    else:
                        candidate = mp
                    if candidate and os.path.exists(candidate):
                        with open(candidate, "rb") as f:
                            file_bytes = f.read()
                        file_name = os.path.basename(candidate)

            if not file_bytes:
                print("[WARN] No file bytes read this cycle.")
                time.sleep(freq)
                continue

            df = _load_file_to_df(file_bytes, log_path or file_name, file_type)
            if skip_lines and not df.empty:
                df = df.iloc[skip_lines:].reset_index(drop=True)
            print("SMB DF: ",df)
            # Project to selected columns if specified
            if columns:
                keep_cols = [c for c in columns if c in df.columns]
                if keep_cols:
                    df = df[keep_cols]
            # Datetime handling + created_on
            print("DF After columns: ",df)
            df = _apply_datetime_handling(df, internal)
            print("Date time DF: ",df)
            # Ensure schema
            schema = _infer_schema_from_df(df, table_name)
            if last_schema != schema:
                _ensure_table(conn, db_name, table_name, schema)
                last_schema = schema
            print("Schema: ",schema)
            # Dedup and insert
            filter_cols = [filter_field] if (filter_field and filter_field in df.columns) else None
            print("Filter_cols: ",filter_cols)
            inserted = _insert_new_rows(conn, db_name, table_name, df, filter_cols, file_name_value=file_name)
            if inserted:
                print(f"[INFO] Inserted {inserted} new rows into {table_name}.")
            else:
                print("[INFO] No new rows (duplicates skipped).")

        except Exception as e:
            print(f"[ERROR] Polling error for {table_name}: {e}")

        time.sleep(freq)


def start_enabled_threads():
    global MANAGED_THREADS

    STOP_EVENT.clear()

    with THREAD_LOCK:
        MANAGED_THREADS = []

        def start(name, target):
            t = threading.Thread(
                target=target,
                daemon=True,      # 🔴 IMPORTANT
                name=name
            )
            t.start()
            MANAGED_THREADS.append(t)
            log_info(f"[THREAD] {name} started")

        start("NetworkWatcher", network_watcher_loop)
        start("SystemStatus", system_status_monitor)

        if ANALOG_ENABLED:
            start("AnalogReader", analog_reader_loop)

        if DIGITAL_INPUT_ENABLED:
            start("DigitalReader", digital_io_loop)
            start("DigitalWriter", digital_db_writer_loop)

        if MODBUS_ENABLED:
            start("ModbusReader", modbus_reader_loop)
            start("ModbusDBWriter",modbus_db_writer_loop)

        if MODBUS_TCP_ENABLED:
            start("ModbusTCP", modbus_tcp_handler)


def stop_all_threads():
    global MANAGED_THREADS
    log_info("[THREAD] Stopping all background tasks...")
    STOP_EVENT.set()  # Signal everyone to stop

    # We iterate through the list and wait for each thread to actually close
    with THREAD_LOCK:
        for t in MANAGED_THREADS:
            if t.is_alive():
                log_info(f"[THREAD] Waiting for {t.name} to exit...")
                t.join(timeout=3.0)  # Give it 3 seconds to finish its current loop
        
        MANAGED_THREADS.clear() # Clear the list for the next start
    
    log_info("[THREAD] All tasks fully stopped.")


"""
What: Background loop that watches is_updated.json and reloads global config when it becomes true.
Calls: read_update_flag(), update_global_config(), clear_update_flag()
Required by: start_config_monitor() (spawns as thread)
Notes: Sleeps 5s between checks; tracks last_flag_state to avoid duplicate logs.
Side effects: Mutates globals via update_global_config(); writes is_updated.json via clear_update_flag().
"""

CONFIG_VERSION = 0

def config_monitor_loop():
    global CONFIG_VERSION
    log_info("[CONFIG] Independent Monitor started")

    while not STOP_EVENT.is_set():
        try:
            # Check if the JSON flag file says we need an update
            if read_update_flag(f"{BASE_DIR}/is_updated.json"):
                log_info("[CONFIG] 🔄 Update detected. Reloading globals...")
                
                # Update global variables
                if update_global_config():
                    with THREAD_LOCK:
                        CONFIG_VERSION += 1  # Increment version to signal threads
                    clear_update_flag(f"{BASE_DIR}/is_updated.json")
                    log_info(f"[CONFIG] 🚀 Global Config Version {CONFIG_VERSION} applied")

            # Efficient sleep
            for _ in range(50):
                if STOP_EVENT.is_set(): return
                time.sleep(0.01)
        except Exception as e:
            log_error(f"[CONFIG] Monitor error: {e}")
            time.sleep(5)

"""
What: Ensure initial configuration is loaded, then spawn config_monitor_loop() as a daemon thread.
Calls: update_global_config(), threading.Thread(target=config_monitor_loop)
Required by: main() when bootstrapping background services.
Notes: Returns the thread object for tracking in main().
Side effects: Starts a background thread; may exit(1) if initial config load fails.
"""


def start_config_monitor():
    # Initialize global configuration first
    if not UPDATED:
        if not update_global_config():
            log_error("[CRITICAL] Initial configuration load failed. Exiting.")
            exit(1)

    # Start monitoring thread
    config_thread = threading.Thread(target=config_monitor_loop, daemon=True)
    config_thread.start()
    log_info("[INFO] 🚀 Configuration monitor thread started")
    return config_thread


# ==================================================
# ANALOG READING FUNCTIONS
# ==================================================

"""
What: Read one analog channel from ADS1115 via I2C, apply optional scaling, and return voltage.
Calls: bus.write_word_data(), bus.read_word_data()
Required by: read_all_analog_channels()
Notes: Uses ANALOG_CHANNELS config and ADC_I2C_ADDRESS. On exceptions returns a random fallback value (simulated).
        Performs byte swap and signed conversion; scales using linear mapping if configured.
Side effects: Prints diagnostic info; touches I2C device.
"""

def read_analog_channel(channel_idx, real=True):
    try:
        if channel_idx >= len(ANALOG_CHANNELS):
            return None

        ch = ANALOG_CHANNELS[channel_idx]
        if not ch.get("enabled", False):
            return None

        mode = ch.get("mode", "0-10V")
        scale_range = ch.get("range", "0-10")
        adc_addr = ch.get("address")

        if adc_addr is None:
            log_error(f"[ANALOG] CH{channel_idx} invalid I2C address")
            return None

        # ---------------- SIMULATION ----------------
        if not real:
            if mode == "0-10V":
                max_v = 10.0 if scale_range == "0-10" else 5.0
                value = round(random.uniform(0.0, max_v), 2)
                log_info(f"[SIM] CH{channel_idx} {scale_range}V = {value}")
                return value

            elif mode == "4-20mA":
                value = round(random.uniform(4.0, 20.0), 2)
                log_info(f"[SIM] CH{channel_idx} 4–20mA = {value}")
                return value

            return None

        # ---------------- REAL ADC ----------------
        config_value = 0xC000 | (channel_idx << 12)
        bus.write_word_data(adc_addr, 0x01, config_value)
        time.sleep(0.1)

        raw = bus.read_word_data(adc_addr, 0x00)
        raw = ((raw << 8) & 0xFF00) | (raw >> 8)
        if raw >= 0x8000:
            raw -= 0x10000

        voltage = raw * 2.048 / 32768.0  # DO NOT TOUCH

        # ---------------- SCALING ----------------
        if mode == "0-10V":
            max_v = 10.0 if scale_range == "0-10" else 5.0
            value = (voltage / 2.048) * max_v
            value = round(max(0.0, min(value, max_v)), 2)
            log_info(f"[ANALOG] CH{channel_idx} {scale_range}V = {value}")
            return value

        elif mode == "4-20mA":
            value = 4.0 + (voltage / 2.048) * 16.0
            value = round(max(4.0, min(value, 20.0)), 2)
            log_info(f"[ANALOG] CH{channel_idx} 4–20mA = {value}")
            return value

        log_error(f"[ANALOG] Unsupported mode {mode}")
        return None

    except Exception as e:
        log_error(f"[ANALOG][ERROR] CH{channel_idx}: {e}")
        return None


"""
What: Read all enabled local ADC channels plus optional extension ADC simulated channels.
Calls: read_analog_channel()
Required by: main_data_collection_loop() (to gather sensor data periodically)
Notes: EXTENSION_ADC readings are simulated with random values; replace with real driver if needed.
Side effects: Console logging; returns dict like {'analog_ch_0': x, 'ext_adc_ch_0': y, ...}
"""


def read_all_analog_channels():
    readings = {}
    for idx, channel in enumerate(ANALOG_CHANNELS):
        if channel.get("enabled", False):
            voltage = read_analog_channel(idx, ANALOG_CONFIG["generate_random"])
            if voltage is not None:
                readings[f"analog_ch_{idx}"] = voltage

    # Read extension ADC if enabled
    # if EXTENSION_ADC.get("enabled", False):
    #     ext_data = EXTENSION_ADC.get("data", [])
    #     for idx, _ext_channel in enumerate(ext_data):
    #         # Simulate reading extension ADC
    #         voltage = round(random.uniform(2.0, 8.0), 2)
    #         readings[f"ext_adc_ch_{idx}"] = voltage
    #         print(f"[INFO] Extension ADC Channel {idx}: {voltage:.2f} V")

    return readings


# ==================================================
# DIGITAL I/O FUNCTIONS
# ==================================================

def setup_gpio_once(gpio_num, direction="out"):
    """
    Ensures the GPIO is exported and the direction is set.
    """
    base_path = f"/sys/class/gpio/gpio{gpio_num}"
    
    # 1. Export the pin if the directory doesn't exist
    if not os.path.exists(base_path):
        try:
            with open("/sys/class/gpio/export", "w") as f:
                f.write(str(gpio_num))
            # Critical: Give the OS time to create the file nodes
            time.sleep(0.1) 
        except OSError as e:
            # Error 16 is "Device or resource busy", which is fine
            if e.errno != 16:
                raise

    # 2. Set the direction (in or out)
    direction_path = f"{base_path}/direction"
    with open(direction_path, "w") as f:
        f.write(direction)


def write_sysfs_gpio(gpio_num, value):
    """
    Prepares the pin and writes the value (1 or 0).
    """
    try:
        # Step A: Setup (Ensure exported and set to 'out')
        setup_gpio_once(gpio_num, "out")
        
        # Step B: Write Value
        path = f"/sys/class/gpio/gpio{gpio_num}/value"
        with open(path, "w") as f:
            f.write("1" if value else "0")
            
    except Exception as e:
        # Assuming log_error is defined in your environment
        log_error(f"[SYSFS GPIO ERROR] gpio{gpio_num}: {e}")


def read_digital_outputs(do_channels):
    outputs = {}

    for idx, ch in enumerate(do_channels):
        state = 1 if ch.get("state") in [1, "1", True] else 0
        outputs[f"digital_out_{idx}"] = state

    return outputs


def apply_digital_outputs(do_channels):
    """
    Iterates through channels and applies states.
    """
    for idx, ch in enumerate(do_channels):
        pin = ch.get("pin")
        # Ensure state is handled as boolean/int 1/0
        state = 1 if ch.get("state") in [1, "1", True] else 0

        if pin is None:
            continue

        try:
            write_sysfs_gpio(pin, state)
            log_info(f"[DO] CH{idx} GPIO{pin} ← {state}")
        except Exception as e:
            log_error(f"[DO][ERROR] CH{idx} (pin={pin}): {e}")


def apply_digital_outputs_gpio(do_channels):
    for idx, ch in enumerate(do_channels):
        pin = ch.get("pin")
        state = ch.get("state", 0)

        if pin is None:
            continue

        try:
            GPIO.output(pin, GPIO.HIGH if state else GPIO.LOW)
            log_info(f"[DO] CH{idx} GPIO {pin} ← {state}")
        except Exception as e:
            log_error(f"[DO][ERROR] CH{idx} (pin={pin}): {e}")



_LAST_DO_STATE = [None, None]


"""
What: Sample enabled digital inputs and return states, also check digital alarms per channel.
Calls: check_digital_alarm()
Required by: main_data_collection_loop()
Notes: Currently simulates inputs randomly (0/1). Replace with GPIO reads as needed.
Side effects: May emit alarm printouts via check_digital_alarm().
"""

def get_digital_interval():
    try:
        cfg = DIGITAL_INPUT_CONFIG
        return convert_polling_interval_to_seconds(
            cfg.get("pollingInterval", 1),
            cfg.get("pollingIntervalUnit", "Sec"),
        )
    except Exception as e:
        log_error(f"[DIGITAL] Interval error, using default: {e}")
        return 10.0

def digital_db_writer_loop():
    log_info("[THREAD] DigitalDBWriter started")

    while not STOP_EVENT.is_set():
        try:
            ts, readings = DIGITAL_QUEUE.get(timeout=0.5)
            insert_digital_data(ts, readings)
            DIGITAL_QUEUE.task_done()

        except queue.Empty:
            continue

        except Exception as e:
            log_error(f"[DIGITAL][DB] {e}")


def digital_io_loop():
    log_info("[THREAD] DigitalReader started")

    interval = get_digital_interval()  # seconds

    while not STOP_EVENT.is_set():
        try:
            ts = datetime.now(
                ZoneInfo("Asia/Kolkata")
            ).strftime("%Y-%m-%d %H:%M:%S")

            with GPIO_LOCK:
                apply_digital_outputs(DIGITAL_OUTPUT_CHANNELS)

                di_readings = read_digital_inputs(
                    DIGITAL_CHANNELS,
                    DIGITAL_INPUT_CONFIG.get("generate_random", False)
                )

                do_readings = read_digital_outputs(DIGITAL_OUTPUT_CHANNELS)

                readings = {}
                if isinstance(di_readings, dict):
                    readings.update(di_readings)

                readings.update(do_readings)

            if readings:
                DIGITAL_QUEUE.put_nowait((ts, readings))

        except queue.Full:
            log_warn("[DIGITAL] Queue full, dropping sample")

        except Exception as e:
            log_error(f"[DIGITAL] Error: {e}")

        STOP_EVENT.wait(timeout=interval)


"""
What: Evaluate a digital input state against configured alarm thresholds and report alarms.
Calls: None
Required by: read_digital_inputs()
Notes: Uses DIGITAL_IO_ALARMS config with up to 5 levels; prints [ALARM] lines when triggered.
Side effects: Print to console; could be extended to send notifications.
"""


DIGITAL_ALARM_STATE = {}   # {(channel, alert_index): bool}
DIGITAL_ALARM_TIMER = {}  # {(channel, alert_index): first_trigger_time}

def normalize_digital_channel_key(key):
    if isinstance(key, int):
        return key

    if isinstance(key, str):
        key = key.strip()
        if key.isdigit():
            return int(key)
        if key.lower().startswith("channel"):
            try:
                return int(key.split()[-1]) - 1
            except Exception:
                pass

    return None


def check_digital_alarm(channel, state):
    now = time.time()
    log_info(f"[INFO] Channel is {channel}")
    # Find matching config block
    channel_cfg = None
    for k, v in DIGITAL_IO_ALARMS.items():
        ch = normalize_digital_channel_key(k)
        log_info(f"[INFO] Channel ch is {ch}")
        if ch == channel:
            channel_cfg = v
            log_info(f"[INFO] Channel is {channel}")
            break

    if not channel_cfg:
        log_info(f'[INFO] No channel_cfg found')
        return

    alerts = channel_cfg.get("alerts", [])
    if not isinstance(alerts, list):
        return

    for idx, alert in enumerate(alerts):
        if not alert.get("enabled", False):
            DIGITAL_ALARM_STATE.pop((channel, idx), None)
            DIGITAL_ALARM_TIMER.pop((channel, idx), None)
            continue

        trigger = alert.get("trigger", "").strip().upper()
        delay = int(alert.get("delay", 0))
        email = alert.get("email", "")
        contact = alert.get("contact", "")
        message = alert.get("message", "")

        # Trigger logic (ACTIVE-LOW aware)
        if trigger == "HIGH":
            triggered = state == 1
        elif trigger == "LOW":
            triggered = state == 0
        else:
            log_error(f"[DIGITAL ALARM] Invalid trigger '{trigger}'")
            continue

        key = (channel, idx)
        prev_state = DIGITAL_ALARM_STATE.get(key, False)

        if triggered:
            if key not in DIGITAL_ALARM_TIMER:
                DIGITAL_ALARM_TIMER[key] = now
                log_info(
                    f"[DIGITAL ALARM] CH{channel} trigger detected, delay started ({delay}s)"
                )

            if now - DIGITAL_ALARM_TIMER[key] < delay:
                continue

            # 🔴 Rising edge after delay
            if not prev_state:
                DIGITAL_ALARM_STATE[key] = True
                DIGITAL_ALARM_TIMER.pop(key, None)

                subject = f"[DIGITAL ALARM] Channel {channel + 1} - {trigger}"
                body = (
                    f"Digital Alarm Triggered\n\n"
                    f"Channel: Channel {channel + 1}\n"
                    f"Trigger: {trigger}\n"
                    f"State: {state}\n"
                    f"Message: {message}\n"
                    f"Contact: {contact}\n"
                )

                log_warn(f"[DIGITAL ALARM] CH{channel + 1}: {message}")

                if email:
                    send_email(email, subject, body)
                else:
                    log_error(
                        f"[DIGITAL ALARM] No email configured for CH{channel + 1}"
                    )

        else:
            # Condition cleared → reset latch & timer
            DIGITAL_ALARM_STATE.pop(key, None)
            DIGITAL_ALARM_TIMER.pop(key, None)


# ==================================================
# WIFI
# ==================================================


def connect_wifi():
    global WIFI_SSID, WIFI_PASSWORD, WIFI_IP_MODE
    global WIFI_IP, WIFI_SUBNET, WIFI_GATEWAY, WIFI_DNS1, WIFI_DNS2

    if not WIFI_SSID:
        log_error("❌ No WiFi SSID configured.")
        return False

    try:
        # Get current active SSID
        result = subprocess.run(
            ["nmcli", "-t", "-f", "active,ssid", "dev", "wifi"],
            capture_output=True,
            text=True,
        )
        current_ssid = None
        for line in result.stdout.strip().splitlines():
            if line.startswith("yes:"):
                current_ssid = line.split(":", 1)[1]
                break

        if current_ssid == WIFI_SSID:
            log_info(f"✅ Already connected to {WIFI_SSID}, skipping reconfigure.")
            return True

        # Not connected or different network → reconfigure
        log_info(f"🔄 Switching WiFi from {current_ssid} → {WIFI_SSID}")
        # Delete old connection for target SSID
        subprocess.run(
            ["nmcli", "connection", "delete", WIFI_SSID],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Connect with password
        cmd = [
            "nmcli",
            "device",
            "wifi",
            "connect",
            WIFI_SSID,
            "password",
            WIFI_PASSWORD,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log_error(f"❌ Failed to connect WiFi: {result.stderr.strip()}")
            return False

        # Configure static IP if required
        if WIFI_IP_MODE.upper() == "STATIC":
            ip_config = f"{WIFI_IP}/{WIFI_SUBNET} {WIFI_GATEWAY}"
            subprocess.run(
                [
                    "nmcli",
                    "connection",
                    "modify",
                    WIFI_SSID,
                    "ipv4.addresses",
                    ip_config,
                    "ipv4.gateway",
                    WIFI_GATEWAY,
                    "ipv4.dns",
                    ",".join(filter(None, [WIFI_DNS1, WIFI_DNS2])),
                    "ipv4.method",
                    "manual",
                ],
                check=True,
            )
            subprocess.run(["nmcli", "connection", "up", WIFI_SSID], check=True)
        else:
            subprocess.run(
                ["nmcli", "connection", "modify", WIFI_SSID, "ipv4.method", "auto"],
                check=True,
            )

        log_info(f"✅ Connected to WiFi {WIFI_SSID} ({WIFI_IP_MODE})")
        return True

    except Exception as e:
        log_error(f"❌ WiFi connection failed: {e}")
        return False


# ==================================================
# MODBUS FUNCTIONS
# ==================================================


def check_modbus_alarms(readings):
    """
    Checks all MODBUS alarms (alerts and per-channel configs) and calls send_sms if triggered.
    """
    # --- ALERTS: list-of-dict conditions (generic) ---
    for alert in MODBUS_ALARMS.get("alerts", []):
        log_info("Hello1")
        if not alert.get("enabled"):
            continue
        slave_id = alert.get("slave_id")
        threshold = float(alert.get("threshold"))
        cond = alert.get("condition")
        contact = alert.get("contact")
        message = alert.get("message", "")

        # Reading keys use e.g. "R1_S{slave_id}_{start}"
        for key in readings:
            # Only match keys for this slave_id
            if f"S{slave_id}_" in key or f"slave_{slave_id}" in key:
                value = None
                v = readings[key]
                # Try to extract numerical value
                if isinstance(v, list):
                    if v:
                        value = v[0]
                elif isinstance(v, (int, float)):
                    value = v
                try:
                    if value is not None:
                        triggered = (
                            (cond == "<=" and value <= threshold)
                            or (cond == "<" and value < threshold)
                            or (cond == ">=" and value >= threshold)
                            or (cond == ">" and value > threshold)
                            or (cond == "==" and value == threshold)
                        )
                        if triggered:
                            log_info("Hello")
                            send_sms(
                                contact,
                                text=f"{message}: value={value} ({cond} {threshold})",
                            )
                except Exception as e:
                    log_warn(f"[WARN] SMS alarm check error: {e}")

    # --- CHANNEL ALARMS: per-channel structure ---
    for channel_name, channel_cfg in MODBUS_ALARMS.items():
        # Skip lists and settings
        if channel_name in ["alerts", "Settings"] or not isinstance(channel_cfg, dict):
            continue
        if channel_cfg.get("alert_enable") != "Enable":
            continue
        # Loop through all alert levels
        for i in range(5):
            if channel_cfg.get(f"level_enable_{i}") != "Enable":
                continue
            try:
                threshold = float(channel_cfg.get(f"level_threshold_{i}", 0))
                contact = channel_cfg.get(f"level_contact_{i}", "")
                message = channel_cfg.get(f"level_message_{i}", "")
                # You may need to define which reading to check: if your key naming allows, match on channel or slave
                # Here, just check every reading
                for key, value in readings.items():
                    if isinstance(value, list):
                        values = value
                    else:
                        values = [value]
                    for v in values:
                        try:
                            triggered = v >= threshold
                            if triggered:
                                send_sms(
                                    contact,
                                    text=f"{message}: value={v} (>= {threshold})",
                                )
                        except Exception as e:
                            pass
            except Exception as e:
                continue


"""
What: Convert a list of 16-bit registers into floats.
    - regs: list of integers from Modbus
    - byte_order: 'big' or 'little' for each 16-bit register
    - word_order: 'big' or 'little' for the two-register float
Returns:
    floats: list of valid floats
    leftover: list of leftover registers that can't form a float.
Calls: struct.unpack()
Required by: read_modbus_registers() when conversion == "Float: Big Endian"
Notes: Default uses big-endian bytes and little-endian word order; adjust per device documentation.
Side effects: None.
"""
import struct


def regs_to_float(regs):
    """
    Schneider EM6436H float:
    - 2 registers per float
    - word-swapped (CDAB)
    - big-endian float
    """
    floats = []

    for i in range(0, len(regs) - 1, 2):
        hi = regs[i + 1].to_bytes(2, 'big')
        lo = regs[i].to_bytes(2, 'big')
        raw = hi + lo
        floats.append(struct.unpack('>f', raw)[0])

    log_info(f"[INFO] Floats are converted {floats}")
    return floats



def convert_regs(regs, conversion, length):
    """
    conversion: "Float: Big Endian" or "Integer"
    length: number of 16-bit regs requested
    Returns a list of numeric values
    """

    if conversion == "Float: Big Endian":
        floats = []
        for i in range(0, length, 2):
            b1 = regs[i].to_bytes(2, "big")
            b2 = regs[i + 1].to_bytes(2, "big")
            raw_bytes = b1 + b2
            floats.append(struct.unpack(">f", raw_bytes)[0])
        return floats

    elif conversion == "Integer":
        if length == 1:
            return [int(regs[0])]
        return [int(x) for x in regs[:length]]

    return [int(x) for x in regs[:length]]



def safe_column_name(name):
    # Basic sanitizer for column names: lowercase, spaces -> underscores, drop non-alnum/underscore
    s = (name or "").strip().lower().replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]", "", s)
    if not s:
        s = "col_" + datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
    return s[:64]  # limit length

def get_energy_brand_blocks(config):
    modbus_rtu = config.get("ModbusRTU", {})
    energy = modbus_rtu.get("Devices", {})
    brands = energy.get("brands", {})
    order = energy.get("order", [])

    result = []

    # Respect order first
    for key in order:
        if key in brands:
            result.append((key, brands[key]))

    # Add remaining brands not in order
    for key, val in brands.items():
        if key not in order:
            result.append((key, val))

    if not result:
        log_info("[INFO] NO brands found")

    return result


def get_serial_settings(config):
    """
    Returns ModbusRTU.settings with defaults if missing.
    Example: { "baudRate": "9600", "parity": "Even", "dataBits": 8, "stopBits": 1 }
    """
    return (config.get("ModbusRTU") or {}).get("settings") or {}


"""
What: Iterate configured Modbus tables (R1/R2) and fetch registers for each enabled slave.
Calls: minimalmodbus.Instrument.read_registers()/read_input_registers(), regs_to_float()
Required by: main_data_collection_loop()
Notes: Uses MODBUS_R1_CFG/MODBUS_R2_CFG tables and per-reg conversion rules (Integer/Float/Hex).
        Adds entries in the returned dict with keys like 'R1_S{slave}_{start}'.
Side effects: Console logs; serial comms to Modbus slaves; catches and logs errors per register.
"""


def parse_eng_unit(eng_unit):
    """
    Parse engineering unit like:
    "1-5V", "4-20mA", "0-10V", "2.5-7.5V"

    Returns (eng_min, eng_max) or (None, None)
    """
    if not eng_unit or not isinstance(eng_unit, str):
        return None, None

    # Extract numbers like 1, 5 or 2.5
    nums = re.findall(r"[-+]?\d*\.?\d+", eng_unit)
    if len(nums) != 2:
        return None, None

    try:
        return abs(float(nums[0])), abs(float(nums[1]))
    except ValueError:
        return None, None

def scale_value(raw, eng_min, eng_max, proc_min, proc_max):
    try:
        raw = float(raw)/1000.0
        eng_min = float(eng_min)
        eng_max = float(eng_max)
        proc_min = float(proc_min)
        proc_max = float(proc_max)

        if eng_max == eng_min:
            return None

        return ((raw - eng_min) / (eng_max - eng_min)) * (proc_max - proc_min) + proc_min
    except Exception:
        return None



USB_INSTRUMENTS = {}


def read_usb_register(brand_key, slave_id, start, length, reg_type):
    slave_cfg = None
    brands = config.get("ModbusRTU", {}).get("Devices", {}).get("brands", {})

    for b in brands.values():
        for s in b.get("slaves", []):
            if s.get("id") == slave_id:
                slave_cfg = s
                break

    if not slave_cfg:
        raise RuntimeError(f"USB slave config not found for slave {slave_id}")

    port = "/dev/energy_meter_usb"

    if not port:
        raise RuntimeError(f"usb_port missing for USB slave {slave_id}")

    key = f"{port}_{slave_id}"

    if key not in USB_INSTRUMENTS:
        inst = minimalmodbus.Instrument(port, int(slave_id))
        inst.serial.baudrate = int(config["ModbusRTU"]["settings"]["baudRate"])
        inst.serial.bytesize = serial.EIGHTBITS
        inst.serial.parity = serial.PARITY_NONE
        inst.serial.stopbits = serial.STOPBITS_TWO
        inst.serial.timeout = 1.0
        inst.mode = minimalmodbus.MODE_RTU
        inst.clear_buffers_before_each_transaction = True
        USB_INSTRUMENTS[key] = inst

    inst = USB_INSTRUMENTS[key]

    fc = 4 if "input" in reg_type.lower() else 3

    try:
        regs = inst.read_registers(start, length)
        print(f"Raw registers at {start}: {regs}")
        floats = []
        for i in range(0, length, 2):
            f = regs_to_float_energy(regs[i], regs[i+1], byte_order='big', word_order='little')
            floats.append(f)
        return floats

    except Exception as e:
        raise RuntimeError(
            f"USB Modbus read failed [{brand_key} slave {slave_id} @ {port}]: {e}"
        )

def regs_to_float_energy(reg1, reg2, byte_order='big', word_order='little'):
    """
    Convert two 16-bit registers to a float with configurable byte and word order.
    
    Parameters:
    - reg1, reg2: integer register values
    - byte_order: 'big' or 'little' (order of bytes inside each register)
    - word_order: 'big' or 'little' (order of registers)
    
    Returns:
    - float decoded value
    """
    if byte_order == 'big':
        b1 = reg1.to_bytes(2, 'big')
        b2 = reg2.to_bytes(2, 'big')
    else:
        b1 = reg1.to_bytes(2, 'little')
        b2 = reg2.to_bytes(2, 'little')
        
    if word_order == 'big':
        raw_bytes = b1 + b2
    else:
        raw_bytes = b2 + b1
    
    return struct.unpack('>f', raw_bytes)[0]

def safe_read(inst, start, length, fc):
    """
    Tries to read registers, handling variations in library support.
    """
    try:
        # Most modern versions of MinimalModbus use this:
        return inst.read_registers(start, int(length), functioncode=fc)
    except Exception as e:
        # Fallback for older libraries or specific instrument types
        try:
            return inst.read_registers(start, int(length))
        except Exception as e_inner:
            raise Exception(f"Modbus Read Failed: {e_inner}")


def read_modbus_devices(instruments):
    readings = {}

    devices = (
        config.get("ModbusRTU", {})
        .get("Devices", {})
        .get("brands", {})
    )

    for brand_key, brand in devices.items():
        registers_by_slave = brand.get("registersBySlave", {})
        slaves = brand.get("slaves", [])

        for slave in slaves:
            slave_id = slave.get("id")
            if slave_id is None:
                continue

            generate = bool(slave.get("generate_random", False))
            use_usb = bool(slave.get("use_usb", False))
            regs = registers_by_slave.get(str(slave_id), [])
            inst_key = f"{brand_key}_{slave_id}"
            inst = instruments.get(inst_key)

            if not generate and not use_usb and not inst:
                log_warn(f"[ENERGY] Instrument missing for {inst_key}")
                continue

            for reg in regs:
                if not reg.get("enabled"):
                    continue

                name = reg.get("name", "unknown")
                key = f"{brand_key}_S{slave_id}_{name}"

                try:
                    # ===== RANDOM MODE =====
                    if generate:
                        # min_v = safe_float(reg.get("process_min"), 0.0)
                        # max_v = safe_float(reg.get("process_max"), 100.0)
                        min_v = 0
                        max_v = 250
                        if min_v > max_v:
                            min_v, max_v = max_v, min_v
                        value = round(random.uniform(min_v, max_v), 3)
                        readings[key] = value
                        log_info(f"[ENERGY][RANDOM] {key} = {value}")
                        continue

                    # ===== REAL MODBUS =====
                    else:
                        start = int(reg["start"])+int(reg["offset"])
                        length = int(reg.get("length", 1))
                        reg_type = reg.get("type", "Input Register")
                        conversion = reg.get("conversion", "Integer")

                        fc = 4 if "input" in reg_type.lower() else 3
                        if use_usb:
                            # 🔌 USB READ PATH
                            raw = read_usb_register(
                                brand_key=brand_key,
                                slave_id=slave_id,
                                start=start,
                                length=length,
                                reg_type=reg_type
                            )
                            value = float(raw[0])
                            if value=="nan":
                                value = "NULL"
                            readings[key] = value
                            print(f"Values for usb read: {value}")
                            continue
                        else:
                            # 🔁 RS485 RTU
                            log_info(f"[INFO] Reading from {start} of length {length}")
                            raw = safe_read(inst, start, length, fc)
                        if length == 1:
                            vals = [float(raw[0])]
                        else:
                            vals = []
                            for i in range(0, length, 2):
                                f = regs_to_float_energy(
                                    raw[i],
                                    raw[i+1],
                                    byte_order='big',
                                    word_order='little'
                                )
                                vals.append(f)

                        # vals = raw
                        print(f"Values for sid {slave_id} are", raw)
                        value = vals[0] if vals is not None and len(vals) > 0 else None

                        print(f"Values after convert regs {vals} for sid {slave_id} and value is {value}")
                        # ---- multiply / divide ----
                        mul = safe_float(reg.get("multiply"), 1.0)
                        div = safe_float(reg.get("divide"), 1.0)
                        log_info(f"[INFO] Multiplication Factor {mul} for sid {slave_id}")
                        log_info(f"[INFO] Division Factor {div} for sid {slave_id}")
                        if div == 0:
                            div = 1.0
                        value = (value * mul) / div
                        log_info(f"[INFO] Value after applying mul and div {value} for sid {slave_id}")

                        if reg.get("eng_unit")=="none":
                        # ---- parse eng_unit ----
                            if value=="nan":
                                value = "NULL"
                            readings[key] = value
                            print(f"Values for read: {value}")
                            continue
                        log_info(
                            f"[DEBUG] REG OBJ ID={id(reg)} "
                            f"NAME={reg.get('name')} "
                            f"ENG_UNIT={reg.get('eng_unit')}"
)
                        eng_min, eng_max = parse_eng_unit(reg.get("eng_unit"))
                        log_info(f"[INFO] ENG_MIN: {eng_min}, ENG_MAX {eng_max}")

                        # ---- process range ----
                        proc_min = safe_float(reg.get("process_min"),1.0)
                        proc_max = safe_float(reg.get("process_max"),100)

                        log_info(f"[INFO] Process_MIN: {proc_min}, Process_MAX {proc_max} for sid {slave_id}")

                        # ---- apply scaling only if ALL present ----
                        if (
                            eng_min is not None
                            and eng_max is not None
                            and proc_min not in ("", None)
                            and proc_max not in ("", None)
                        ):
                            scaled = scale_value(value, eng_min, eng_max, proc_min, proc_max)
                            if scaled is not None:
                                value = round(scaled, 3)
                        log_info(f"[INFO] Value after scaling {value} for sid {slave_id}")

                        if isinstance(value, (int, float)):
                            if math.isnan(value) or math.isinf(value):
                                value = None
                    if not generate:
                        readings[key] = value
                        log_info(f"[INFO] The reading for {slave_id} after conversion is {value} for sid {slave_id}")

                except Exception as e:
                    log_error(f"[ENERGY] Failed {key}: {e}")

    return readings



def read_modbus_devices_setup_every_time(instruments):
    readings = {}
    channel_meta = {}  # 🔥 NEW

    devices = (
        config.get("ModbusRTU", {})
        .get("Devices", {})
        .get("brands", {})
    )

    for brand_key, brand in devices.items():
        registers_by_slave = brand.get("registersBySlave", {})
        slaves = brand.get("slaves", [])

        for slave in slaves:
            slave_id = slave.get("id")
            if slave_id is None:
                continue

            generate = bool(slave.get("generate_random", False))
            use_usb = bool(slave.get("use_usb", False))
            regs = registers_by_slave.get(str(slave_id), [])
            inst_key = f"{brand_key}_{slave_id}"
            inst = instruments.get(inst_key)

            if not generate and not use_usb and not inst:
                log_warn(f"[ENERGY] Instrument missing for {inst_key}")
                continue

            for reg in regs:
                if not reg.get("enabled"):
                    continue

                name = reg.get("name", "unknown")
                key = f"{brand_key}_S{slave_id}_{name}"

                try:
                    # ===== RANDOM MODE =====
                    if generate:
                        min_v = safe_float(reg.get("process_min"), 0.0)
                        max_v = safe_float(reg.get("process_max"), 100.0)
                        if min_v > max_v:
                            min_v, max_v = max_v, min_v
                        value = round(random.uniform(min_v, max_v), 3)
                        log_info(f"[ENERGY][RANDOM] {key} = {value}")

                    # ===== REAL MODBUS =====
                    else:
                        start = int(reg["start"])+int(reg["offset"])
                        length = int(reg.get("length", 1))
                        reg_type = reg.get("type", "Input Register")
                        conversion = reg.get("conversion", "Integer")

                        fc = 4 if "input" in reg_type.lower() else 3
                        inst.serial.baudrate = int(slave.get("baudRate", 9600))
                        inst.serial.stopbits = int(slave.get("stopBits", 1))
                        inst.serial.parity = {
                            "None": "N",
                            "Even": "E",
                            "Odd": "O"
                        }.get(slave.get("parity", "None"), "N")

                        if use_usb:
                            # 🔌 USB READ PATH
                            raw = read_usb_register(
                                brand_key=brand_key,
                                slave_id=slave_id,
                                start=start,
                                length=length,
                                reg_type=reg_type
                            )
                            value = float(raw[0])
                            if value=="nan":
                                value = "NULL"
                            readings[key] = value
                            print(f"Values for usb read: {value}")
                            continue
                        else:
                            # 🔁 RS485 RTU
                            log_info(f"[INFO] Reading from {start} of length {length}")
                            raw = safe_read(inst, start, length, fc)
                        if length == 1:
                            vals = [float(raw[0])]
                        else:
                            vals = []
                            for i in range(0, length, 2):
                                f = regs_to_float_energy(
                                    raw[i],
                                    raw[i+1],
                                    byte_order='big',
                                    word_order='little'
                                )
                                vals.append(f)

                        # vals = raw
                        print(f"Values for sid {slave_id} are", raw)
                        value = vals[0] if vals is not None and len(vals) > 0 else None

                        print(f"Values after convert regs {vals} for sid {slave_id} and value is {value}")
                        # ---- multiply / divide ----
                        mul = safe_float(reg.get("multiply"), 1.0)
                        div = safe_float(reg.get("divide"), 1.0)
                        log_info(f"[INFO] Multiplication Factor {mul} for sid {slave_id}")
                        log_info(f"[INFO] Division Factor {div} for sid {slave_id}")
                        if div == 0:
                            div = 1.0
                        value = (value * mul) / div
                        log_info(f"[INFO] Value after applying mul and div {value} for sid {slave_id}")

                        if reg.get("eng_unit")=="none":
                        # ---- parse eng_unit ----
                            if value=="nan":
                                value = "NULL"
                            readings[key] = value
                            print(f"Values for read: {value}")
                            continue
                        log_info(
                            f"[DEBUG] REG OBJ ID={id(reg)} "
                            f"NAME={reg.get('name')} "
                            f"ENG_UNIT={reg.get('eng_unit')}"
)
                        eng_min, eng_max = parse_eng_unit(reg.get("eng_unit"))
                        log_info(f"[INFO] ENG_MIN: {eng_min}, ENG_MAX {eng_max}")

                        # ---- process range ----
                        proc_min = safe_float(reg.get("process_min"),1.0)
                        proc_max = safe_float(reg.get("process_max"),100)

                        log_info(f"[INFO] Process_MIN: {proc_min}, Process_MAX {proc_max} for sid {slave_id}")

                        # ---- apply scaling only if ALL present ----
                        if (
                            eng_min is not None
                            and eng_max is not None
                            and proc_min not in ("", None)
                            and proc_max not in ("", None)
                        ):
                            scaled = scale_value(value, eng_min, eng_max, proc_min, proc_max)
                            if scaled is not None:
                                value = round(scaled, 3)
                        log_info(f"[INFO] Value after scaling {value} for sid {slave_id}")

                        if isinstance(value, (int, float)):
                            if math.isnan(value) or math.isinf(value):
                                value = None
                        
                    readings[key] = value
                    channel_meta[key] = {
                        "column": name,
                        "sensor_type": reg.get("sensor_type"),
                        "eng_symbol": reg.get("eng_symbol")
                    }
                    log_info(f"[INFO] The reading for {slave_id} after conversion is {value} for sid {slave_id}")

                except Exception as e:
                    log_error(f"[ENERGY] Failed {key}: {e}")
    
    return readings, channel_meta


def safe_float(val, default):
    try:
        if val is None or val == "":
            return default
        return float(val)
    except Exception:
        return default

def infer_sql_type(value):
    if isinstance(value, bool):
        return "TINYINT(1)"
    if isinstance(value, int):
        return "INT"
    if isinstance(value, float):
        return "FLOAT"
    return "VARCHAR(255)"


def insert_energy_data(timestamp, readings, channel_meta):
    """
    Insert energy readings into per-slave DB & table
    using ONLY tag name as column.
    """
    if not isinstance(readings, dict) or not readings:
        return

    brands = (
        config.get("ModbusRTU", {})
        .get("Devices", {})
        .get("brands", {})
    )

    DATABASE_CFG = config.get("Database", {})

    grouped = {}  # {(brand, slave_id): {tag: value}}

    for key, value in readings.items():
        try:
            # dvp_plc_S1_Current_L1
            brand, rest = key.split("_S", 1)
            slave_id, tag = rest.split("_", 1)
            slave_id = int(slave_id)
        except Exception:
            log_warn(f"[ENERGY][DB] Invalid key format: {key}")
            continue

        col = tag  # ONLY tag name
        grouped.setdefault((brand, slave_id), {})[col] = value

    for (brand, slave_id), values in grouped.items():
        brand_cfg = brands.get(brand)
        if not brand_cfg:
            continue

        slave_cfg = next(
            (s for s in brand_cfg.get("slaves", []) if s.get("id") == slave_id),
            None,
        )
        if not slave_cfg:
            continue

        db_name = slave_cfg.get("db_name")
        table = slave_cfg.get("table_name") or f"S{slave_id}"

        if not db_name:
            log_warn(f"[ENERGY][DB] db_name missing for {brand} S{slave_id}")
            continue

        # ---------------- LOCAL DB ----------------
        local_cfg = DATABASE_CFG.get("local")
        if (
            isinstance(local_cfg, dict)
            and slave_cfg.get("upload_local") is True
            and isinstance(local_cfg.get("cred"), dict)
        ):
            try:
                insert_energy_into_single_db(
                    local_cfg["cred"],
                    table,
                    db_name,
                    timestamp,
                    values,
                    channel_meta
                )
            except Exception as e:
                log_error(f"[ENERGY][LOCAL] Insert failed: {e}")

        # ---------------- CLOUD DB ----------------
        cloud_cfg = DATABASE_CFG.get("cloud")
        if (
            isinstance(cloud_cfg, dict)
            and slave_cfg.get("upload_cloud") is True
            and isinstance(cloud_cfg.get("cred"), dict)
        ):
            try:
                insert_energy_into_single_db(
                    cloud_cfg["cred"],
                    table,
                    db_name,
                    timestamp,
                    values,
                    channel_meta
                )
            except Exception as e:
                log_error(f"[ENERGY][CLOUD] Insert failed: {e}")


def insert_energy_into_single_db(db_cred, table, database, timestamp, values,  channel_meta):
    conn = None
    try:
        conn = get_db_connection(db_cred, timeout=3)
        if not conn:
            return

        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        conn.database = database

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts DATETIME NOT NULL
            ) ENGINE=InnoDB;
        """)

        meta_table = f"{table}_channels"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{meta_table}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                column_name VARCHAR(64) NOT NULL,
                sensor_type VARCHAR(32),
                eng_unit VARCHAR(32),
                UNIQUE KEY uniq_col (column_name)
            ) ENGINE=InnoDB;
        """)

        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        existing = {r[0] for r in cur.fetchall()}

        for col in values:
            if col not in existing:
                cur.execute(
                    f"ALTER TABLE `{table}` ADD COLUMN `{col}` DOUBLE DEFAULT NULL"
                )

        for key, meta in channel_meta.items():
            if meta["column"] not in values:
                continue

            cur.execute(f"""
                INSERT INTO `{meta_table}`
                (column_name, sensor_type, eng_unit)
                VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    sensor_type=VALUES(sensor_type),
                    eng_unit=VALUES(eng_unit)
            """, (
                meta["column"],
                meta["sensor_type"],
                meta["eng_symbol"]
            ))

        
        cols = ["ts"] + list(values.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        cols_sql = ", ".join(f"`{c}`" for c in cols)

        cur.execute(
            f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})",
            [timestamp] + list(values.values()),
        )

        conn.commit()
        log_info(
            f"[ENERGY][DB] Inserted {len(values)} values into {database}.{table}"
        )

    except Exception as e:
        log_error(f"[ENERGY][DB] Insert failed: {e}")
    finally:
        if conn:
            conn.close()


# ==================================================
# DATABASE AND LOGGING FUNCTIONS
# ==================================================

"""
What: Insert numeric readings into MySQL table sensor_readings (auto-creates table).
Calls: mysql.connector.connect(), cursor.execute(), conn.commit()
Required by: main_data_collection_loop(), process_rs485_data()
Notes: Only inserts values that are int/float; others skipped. Uses MYSQL_CONFIG.
Side effects: Writes to MySQL; creates table if missing.
"""


def insert_readings_mysql(timestamp, readings):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Create tables for different types of readings
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS io_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                sensor_type VARCHAR(50),
                sensor_id VARCHAR(50),
                value FLOAT,
                unit VARCHAR(20)
            )
        """
        )

        # Insert readings
        for sensor_id, value in readings.items():
            if isinstance(value, (int, float)):
                cursor.execute(
                    "INSERT INTO io_readings (timestamp, sensor_type, sensor_id, value) VALUES (%s, %s, %s, %s)",
                    (timestamp, "mixed", sensor_id, float(value)),
                )

        conn.commit()
        cursor.close()
        conn.close()
        log_info(f"[INFO] Inserted {len(readings)} readings into MySQL")

    except Exception as e:
        log_error(f"[ERROR] MySQL insert failed: {e}")


"""
What: Choose CSV output path, preferring mounted SD card (/media/*) else home directory.
Calls: psutil.disk_partitions()
Required by: main_data_collection_loop() prior to write_to_csv()
Notes: Returns a full file path for CSV_FILENAME.
Side effects: None (just selects a path).
"""


def get_csv_path():
    for part in psutil.disk_partitions():
        if "/media/" in part.mountpoint:
            log_info(f"[INFO] SD card found at {part.mountpoint}")
            return os.path.join(part.mountpoint, CSV_FILENAME)
    log_warn("[WARN] SD card not inserted, using internal path")
    return os.path.join(Path.home(), CSV_FILENAME)


"""
What: Append buffered readings (data_map) to CSV with a header and rows per timestamp.
Calls: built-in file I/O
Required by: main_data_collection_loop() for local logging and preparing temp file to send.
Notes: readings_data is a dict: {timestamp: {sensor_id: value}}.
        IMPORTANT: The original header-writing line had a bug (",".join(header[0])) which splits 'timestamp'
                    into characters; fixed below to join the full list.
Side effects: Writes/creates CSV files; can be called for SD card and temp files.
"""


def write_to_csv(path, readings_data):
    try:
        file_exists = os.path.isfile(path)
        needs_header = not file_exists or os.path.getsize(path) == 0
        header = ["timestamp"]
        if readings_data:
            # Use the first timestamp's keys as header columns
            first_ts = next(iter(readings_data))
            for sensor_id in readings_data.get(first_ts, {}).keys():
                header.append(sensor_id)
        with open(path, "a", newline="") as csvfile:
            if needs_header:
                # FIX: write the entire header list
                csvfile.write(",".join(header) + "\n")

            # Write data
            for ts, readings in readings_data.items():
                row = [ts]
                for sensor_id in header[1:]:  # Skip timestamp column
                    value = readings.get(sensor_id, "")
                    row.append(str(value))
                csvfile.write(",".join(row) + "\n")

        log_info(f"[INFO] Logged {len(readings_data)} entries to CSV")

    except Exception as e:
        log_error(f"[ERROR] Failed to write CSV: {e}")


# ==================================================
# DATABASE AND LOGGING FUNCTIONS (updated)
# ==================================================


def get_table_columns(cursor, table_name):
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    return [row[0] for row in cursor.fetchall()]


def ensure_column(cursor, conn, table, col, col_type="FLOAT"):
    cols = set(get_table_columns(cursor, table))
    if col not in cols:
        cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {col_type} NULL")
        conn.commit()


def rename_column(cursor, conn, table, old_col, new_col, col_type="FLOAT"):
    cursor.execute(
        f"ALTER TABLE `{table}` CHANGE COLUMN `{old_col}` `{new_col}` {col_type} NULL"
    )
    conn.commit()


def ensure_mapping_table(cursor, conn):
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS energy_meter_map (
        address INT PRIMARY KEY,
        column_name VARCHAR(128) NOT NULL
    )
    """
    )
    conn.commit()


def load_mapping(cursor):
    cursor.execute("SELECT address, column_name FROM energy_meter_map")
    return {int(a): c for (a, c) in cursor.fetchall()}


def upsert_mapping(cursor, conn, address, column_name):
    cursor.execute(
        """
        INSERT INTO energy_meter_map (address, column_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE column_name = VALUES(column_name)
    """,
        (int(address), column_name),
    )
    conn.commit()


def ensure_energy_dynamic_table(cursor, conn, table_name="energy_meter_readings"):
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        time DATETIME DEFAULT CURRENT_TIMESTAMP
        -- dynamic columns added later
    ) ENGINE=InnoDB
    """
    )
    conn.commit()


def insert_energy_row(cursor, conn, row_data, table_name="energy_meter_readings"):
    if not row_data:
        return
    cols = list(row_data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(f"`{c}`" for c in cols)
    sql = f"INSERT INTO `{table_name}` ({collist}) VALUES ({placeholders})"
    cursor.execute(sql, [row_data[c] for c in cols])
    conn.commit()


# ==================================================
# GENERIC WIDE-TABLE UTILITIES (edited/new)
# ==================================================


def ensure_wide_base(cursor, conn, table_name):
    """
    Ensures a wide table exists with at least id PK and timestamp.
    Additional columns are added dynamically.
    """
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    """
    )
    conn.commit()


def add_missing_columns(cursor, conn, table_name, columns_to_add, col_type):
    """
    Add missing columns (list of names) to a table using provided SQL type.
    """
    if not columns_to_add:
        return
    existing = set(get_table_columns(cursor, table_name))
    for col in columns_to_add:
        if col not in existing:
            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {col_type} NULL"
            )
    conn.commit()


def insert_wide_row_v2(
    cursor,
    conn,
    table_name,
    timestamp,
    values_dict,
    zero_fill_cols=None,
    col_type="DOUBLE",
):
    """
    Insert one wide row:
      - Ensures table exists and all columns exist.
      - values_dict: dict of column -> numeric value to set.
      - zero_fill_cols: iterable of columns that must be present; if absent in values_dict, value=0 is inserted.
      - col_type: SQL type for dynamically added columns (e.g., DOUBLE, TINYINT(1)).
    Behavior:
      - Columns are created dynamically if missing.
      - Row contains provided values plus zero for any zero_fill_cols not present.
    """
    if zero_fill_cols is None:
        zero_fill_cols = []

    ensure_wide_base(cursor, conn, table_name)

    # Determine final columns to have in this row
    row_map = {}
    # Numeric sanitize and collect provided values
    for k, v in (values_dict or {}).items():
        try:
            row_map[k] = float(v)
        except Exception:
            # Skip non-numeric quietly
            pass

    # Force zero for all requested zero-fill columns that are missing
    for col in zero_fill_cols:
        if col not in row_map:
            row_map[col] = 0.0

    # Ensure columns exist before insert
    all_needed_cols = list(row_map.keys())
    add_missing_columns(cursor, conn, table_name, all_needed_cols, col_type)

    # Build deterministic column ordering for insert
    ordered_cols = sorted(row_map.keys())
    placeholders = ", ".join(["%s"] * (1 + len(ordered_cols)))  # +1 for timestamp
    col_list_sql = ", ".join(["`timestamp`"] + [f"`{c}`" for c in ordered_cols])
    sql = f"INSERT INTO `{table_name}` ({col_list_sql}) VALUES ({placeholders})"
    args = [timestamp] + [row_map[c] for c in ordered_cols]

    cursor.execute(sql, args)
    conn.commit()


# ==================================================
# ANALOG/DIGITAL WIDE INSERTS
# ==================================================


def get_db_connection(db_cred, timeout=3):
    try:
        return mariadb.connect(
            host=db_cred["host"],
            user=db_cred["user"],
            password=db_cred["password"],
            port=db_cred.get("port", 3306),
            autocommit=False if db_cred.get("autocommit", False) in ['false',False] else True,
            connect_timeout=timeout,
        )
    except Exception as e:
        log_error(f"[DB] Connection failed ({db_cred.get('host')}): {e}")
        return None


def insert_into_single_db(db_cred, table, database, column, timestamp, value):
    conn = None
    try:
        conn = get_db_connection(db_cred, timeout=3)
        if not conn:
            log_error("DB Not connected")
            return  # HARD skip, do not block others

        cur = conn.cursor()

        cur.execute(f"""
            CREATE DATABASE IF NOT EXISTS `{database}`;
        """)

        log_info("DB created")
        # 2. Switch DB
        conn.database = database   # ← THIS is the correct switch
        log_info("DB changed")

        # Create table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts DATETIME NOT NULL
            ) ENGINE=InnoDB;
        """)
        log_info("Table created")
        # Add column if missing
        cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (column,))
        if not cur.fetchone():
            cur.execute(
                f"ALTER TABLE `{table}` ADD COLUMN `{column}` DOUBLE DEFAULT 0"
            )
        
        log_info("Columns added")
        # Insert
        cur.execute(
            f"INSERT INTO `{table}` (ts, `{column}`) VALUES (%s, %s)",
            (timestamp, value),
        )
        log_info("Data inserted")
        conn.commit()
        log_info(f"[DB] Inserted into {db_cred['host']}:{table}")

    except Exception as e:
        log_error(f"[DB] Insert failed ({db_cred.get('host')}): {e}")

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def insert_analog_data(timestamp, readings):
    try:
        if not readings:
            log_info("Not Received readings")
            return

        analog_cfg = ANALOG_CONFIG
        db_cfg = DATABASE
        analog_db_cfg = analog_cfg.get("db", {})

        database = analog_db_cfg.get("db_name")
        table = analog_db_cfg.get("table_name") or "analog_reading"
        column = analog_cfg.get("name", "analog")
        print(
            "INSERT_ANALOG_DATA:",
            "upload_local=", analog_cfg.get("db", {}).get("upload_local"),
            "db_name=", analog_cfg.get("db", {}).get("db_name"),
            "table=", analog_cfg.get("db", {}).get("table_name"),
        )


        # Aggregate value (example: average)
        try:
            value = sum(readings.values()) / len(readings)
        except Exception:
            log_error("[ANALOG] Failed to aggregate readings")
            return

        # ---------- LOCAL DB ----------
        if analog_db_cfg.get("upload_local"):
            try:
                log_info("Inserting into Local DB")
                print("DB_CFG_LOCAL =", db_cfg.get("local"))
                print("DB_CFG_LOCAL_CRED =", db_cfg.get("local", {}).get("cred"))

                insert_into_single_db(
                    db_cfg["local"]["cred"],
                    table,
                    database,
                    column,
                    timestamp,
                    value,
                )
            except Exception as e:
                log_error(f"[LOCAL DB] Unexpected error: {e}")

        # ---------- CLOUD DB ----------
        if analog_db_cfg.get("upload_cloud"):
            try:
                insert_into_single_db(
                    db_cfg["cloud"]["cred"],
                    table,
                    database,
                    column,
                    timestamp,
                    value,
                )
            except Exception as e:
                log_error(f"[CLOUD DB] Unexpected error: {e}")

    except Exception as e:
        log_error(f"[ANALOG INSERT] Fatal error: {e}")


def normalize_state(v):
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        return 0 if float(v) == 0.0 else 1
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "on", "high"):
            return 1
        if s in ("0", "false", "off", "low"):
            return 0
    return None


def expected_digital_columns():
    cols = []
    try:
        if "DIGITAL_CHANNELS" in globals() and DIGITAL_CHANNELS:
            for i, ch in enumerate(DIGITAL_CHANNELS):
                if ch.get("enabled", False):
                    cols.append(f"digitalch{i}")
    except Exception:
        pass
    return cols


def insert_digital_wide(timestamp, readings):
    """
    Write to digital_wide table with a column per digital sensor_id (0/1).
    Missing expected sensors are written as 0.
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor()
        zero_cols = expected_digital_columns()
        normalized = {}
        for k, v in (readings or {}).items():
            st = normalize_state(v)
            if st is not None:
                normalized[k] = int(st)
        # Cast to DOUBLE or TINYINT(1); use TINYINT(1) here
        insert_wide_row_v2(
            cur,
            conn,
            "digital_wide",
            timestamp,
            normalized,
            zero_fill_cols=zero_cols,
            col_type="TINYINT(1)",
        )
        cur.close()
        conn.close()
        log_info(
            f"[INFO] digital_wide inserted at {timestamp} with {len(normalized)} values (zeros padded for {len(zero_cols)})"
        )
    except Exception as e:
        log_error(f"[ERROR] insert_digital_wide: {e}")


# ==================================================
# PER-BRAND, PER-SLAVE WIDE TABLES
# ==================================================


def brand_slave_table_name(brand_key, slave_id):
    """
    Build table name for a specific brand+slave.
    Format: {brand}_S1{slave_id}
    """
    return f"{str(brand_key).lower()}_S1{int(slave_id)}"


def safe_col_name(name):
    # Keep alnum and underscores; replace spaces and special chars
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in str(name))


def insert_brand_slave_wide(
    timestamp, brand_key, slave_id, tag_values, zero_pad_tags=None, tag_type="DOUBLE"
):
    """
    Insert into a per-brand-per-slave wide table:
      - Table name: {brand}_S1{slave_id}
      - tag_values: dict of tag/point name -> numeric value
      - zero_pad_tags: iterable of tag names that must exist with 0 if missing
      - tag_type: SQL type for dynamic columns (DOUBLE by default)
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor()
        table = brand_slave_table_name(brand_key, slave_id)

        # Sanitize column names and numeric values
        vals = {}
        for k, v in (tag_values or {}).items():
            col = safe_col_name(k)
            try:
                vals[col] = float(v)
            except Exception:
                pass

        zero_cols = [safe_col_name(z) for z in (zero_pad_tags or [])]

        insert_wide_row_v2(
            cur,
            conn,
            table,
            timestamp,
            vals,
            zero_fill_cols=zero_cols,
            col_type=tag_type,
        )

        cur.close()
        conn.close()
        log_info(
            f"[INFO] {table} inserted at {timestamp} with {len(vals)} tags (zeros padded for {len(zero_cols)})"
        )
    except Exception as e:
        log_error(f"[ERROR] insert_brand_slave_wide: {e}")


# ==================================================
# COMMUNICATION FUNCTIONS
# ==================================================

"""
What: Power ON the 4G module via GPIO using gpioset.
Calls: subprocess.run()
Required by: main() when COMMUNICATION_MEDIA == "4G/LTE"
Notes: Requires root privileges and correct GPIO chip/index on target.
Side effects: Toggles hardware GPIO; blocks ~5s for stabilization.
"""


def enable_4g_module():
    try:
        subprocess.run(["sudo", "gpioset", "0", "6=0"], check=True)
        # subprocess.run(["gpioset", "0", "6=0"], check=True)
        log_info("[INFO] 4G module powered ON (GPIO6=0)")
        time.sleep(5)
    except subprocess.CalledProcessError as e:
        log_error(f"[ERROR] Failed to power ON 4G module: {e}")


"""
What: Request DHCP lease on LTE_INTERFACE using dhclient.
Calls: subprocess.run()
Required by: main() when COMMUNICATION_MEDIA == "4G/LTE"
Notes: Assumes modem provides a network interface and DHCP server; may need APN/ppp for some modems.
Side effects: Network config; interface state changes.
"""


def connect_4g():
    try:
        subprocess.run(
            ["sudo", "dhclient", "-v", "-e", "IF_METRIC=600", LTE_INTERFACE], check=True
        )
        # subprocess.run(["dhclient", "-v", "-e", "IF_METRIC=600", LTE_INTERFACE], check=True)
        log_info(f"[INFO] Connected via 4G LTE on {LTE_INTERFACE}")
    except subprocess.CalledProcessError as e:
        log_error(f"[ERROR] 4G LTE network setup failed: {e}")


def encrypt_bytes_aes128_gcm(key: bytes, plaintext: bytes) -> bytes:
    """
    Returns: nonce(12) || ciphertext || tag(16)
    AESGCM.encrypt returns ciphertext||tag, so we prepend nonce.
    """
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit recommended for GCM
    ct_and_tag = aesgcm.encrypt(nonce, plaintext, associated_data=None)
    return nonce + ct_and_tag


"""
What: POST a file (CSV) to JSON_ENDPOINT over HTTP.
Calls: requests.post()
Required by: main_data_collection_loop() when sending buffered data.
Notes: Expects JSON_ENDPOINT to accept multipart/form-data; adjust to application/json if needed.
Side effects: Network egress; prints error/status.
"""


def send_data_via_4g(filepath):
    key_b64 = os.environ.get("ENCRYPTION_KEY")
    if not key_b64:
        log_error("[ERROR] ENCRYPTION_KEY env var not set")
        return

    try:
        key = base64.b64decode(key_b64)
    except Exception as e:
        log_error(f"[ERROR] Invalid ENCRYPTION_KEY (must be base64): {e}")
        return

    if len(key) != 16:
        log_error(
            "[ERROR] ENCRYPTION_KEY must be 16 bytes (AES-128). Provide base64 of 16 raw bytes."
        )
        return

    try:
        with open(filepath, "rb") as f:
            plaintext = f.read()

        payload_bytes = encrypt_bytes_aes128_gcm(key, plaintext)

        # Send as file named <original>.enc and include original filename in a form field
        files = {
            "file": (os.path.basename(filepath) + ".enc", io.BytesIO(payload_bytes))
        }
        data = {"orig_filename": os.path.basename(filepath)}
        response = requests.post(
            JSON_ENDPOINT, files=files, data=data, timeout=20, verify=True
        )

        if response.status_code in (200, 201):
            log_info("[INFO] Data uploaded via 4G successfully")
        else:
            log_error(
                f"[ERROR] Upload failed: HTTP {response.status_code} - {response.text}"
            )
    except Exception as e:
        log_error(f"[ERROR] 4G upload failed: {e}")


"""
What: Spawn a TCP server to receive incoming data and append to a file, also echo to Kafka.
Calls: socket library; kafka_publish() internally for decoded messages
Required by: main() as a background thread
Notes: Binds to RECV_IP:RECV_PORT; per-connection handler writes 'received_data.txt' and pushes to Kafka.
Side effects: Opens listening socket; creates/updates received_data.txt; emits Kafka messages.
"""


def start_tcp_server():
    def handle_client(client_socket, addr):
        """
        What: Per-connection handler that receives data, stores to file, and attempts Kafka publish.
        Calls: kafka_publish()
        Required by: start_tcp_server() internal use
        Side effects: Appends to file; network I/O.
        """
        log_info(f"[INFO] Incoming TCP connection from {addr}")
        try:
            with open("received_data.txt", "ab") as f:
                while True:
                    data = client_socket.recv(4096)
                    if not data:
                        break
                    f.write(data)
                    try:
                        decoded = data.decode(errors="ignore")
                        # kafka_publish("tcp", {"ts": datetime.now(timezone.utc).isoformat(), "data": decoded})
                    except Exception:
                        pass
            log_info(f"[INFO] Data received and saved from {addr}")
        except Exception as e:
            log_error(f"[ERROR] TCP server error: {e}")
        finally:
            client_socket.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((RECV_IP, RECV_PORT))
    server.listen(5)
    log_info(f"[INFO] TCP Server listening on {RECV_IP}:{RECV_PORT}")

    while True:
        client_sock, addr = server.accept()
        threading.Thread(
            target=handle_client, args=(client_sock, addr), daemon=True
        ).start()


# ==================================================
# ALARMS
# ==================================================


def free_port(port):
    """Kill any process using the serial port."""
    try:
        # Find processes using the port
        output = subprocess.check_output(["fuser", port], stderr=subprocess.DEVNULL)
        pids = [int(pid) for pid in output.decode().split()]
        for pid in pids:
            log_info(f"Killing process {pid} holding {port}")
            os.kill(pid, 9)
    except subprocess.CalledProcessError:
        # fuser returns non-zero if no process is using it
        pass


def send_at(ser, cmd, delay=0.5):
    ser.write((cmd + "\r").encode())
    time.sleep(delay)
    return ser.read_all().decode(errors="ignore")


def is_registered(ser):
    resp = send_at(ser, "AT+CREG?")
    log_info(f"CREG response: {resp.strip()}")
    return "+CREG: 0,1" in resp or "+CREG: 0,5" in resp


def is_attached(ser):
    resp = send_at(ser, "AT+CGATT?")
    log_info(f"CGATT response:{resp.strip()}")
    return "+CGATT: 1" in resp


def send_sms(number, text):
    # Free the port before opening
    try:
        free_port(MODEM_PORT)

        with serial.Serial(MODEM_PORT, RS485_BAUD_RATE, timeout=5) as ser:
            if "OK" not in send_at(ser, "AT"):
                log_info("Modem not responding.")
                return

            if not is_registered(ser):
                log_error("❌ SIM not registered on network.")
                return
            if not is_attached(ser):
                log_error("❌ SIM not attached to packet service.")
                return

            log_info(send_at(ser, "AT+CMGF=1"))

            ser.write(f'AT+CMGS="{number}"\r'.encode())
            time.sleep(0.5)

            ser.write(text.encode() + b"\x1a")
            time.sleep(5)

            response = ser.read_all().decode(errors="ignore")
            log_info(f"Final response: {response}")

            if "OK" in response and "+CMGS" in response:
                log_info("✅ SMS sent successfully!")
            else:
                log_error("❌ SMS failed.")
    except Exception as e:
        log_error(f"❌ SMS sending error: {e}")


# SMB SHARE


# ==================================================
# MAIN LOOP FUNCTIONS
# ==================================================

"""
What: The core scheduler/loop that periodically reads analog/digital/Modbus sensors,
        persists to DB/CSV, and transmits via 4G/TCP per thresholds.
Calls:
    - setup_modbus_instruments()
    - convert_polling_interval_to_seconds()
    - read_all_analog_channels(), read_digital_inputs(), read_modbus_registers()
    - insert_readings_mysql(), get_csv_path(), write_to_csv(), send_data_via_4g()
    - kafka_publish() for aggregate payloads
Required by: main() (runs in main thread)
Notes:
    - Manages per-source timers and batching via data_map.
    - Sends via 4G when batch >=5 or time interval threshold; also TCP-broadcasts when len>=10.
    - Clears data_map after sending.
    - Robust to exceptions with short backoff.
Side effects: DB writes, file writes (CSV), network sends, Kafka publish, console logs.
"""
# ==================================================
# THREAD-SAFE BUFFER FOR CSV/SEND
# ==================================================
csv_buffer = {}  # { timestamp: {sensor_id: value, ...} }
csv_lock = threading.Lock()


def buffer_merge(timestamp, readings):
    """Merge readings into the timestamp row for CSV/telemetry in a thread-safe way."""
    if not readings:
        return
    with csv_lock:
        row = csv_buffer.get(timestamp, {})
        row.update(readings)
        csv_buffer[timestamp] = row


# ==================================================
# SENSOR READER THREADS (PARALLEL)
# ==================================================


def evaluate_condition(value, condition, threshold):
    try:
        if condition == "<":
            log_info(f"[DEBUG] Evaluating: {value} < {threshold}")
            return value < threshold
        elif condition == "<=":
            log_info(f"[DEBUG] Evaluating: {value} <= {threshold}")
            return value <= threshold
        elif condition == ">":
            log_info(f"[DEBUG] Evaluating: {value} > {threshold}")
            return value > threshold
        elif condition == ">=":
            log_info(f"[DEBUG] Evaluating: {value} >= {threshold}")
            return value >= threshold
        elif condition == "==":
            log_info(f"[DEBUG] Evaluating: {value} == {threshold}")
            return value == threshold
        elif condition == "!=":
            log_info(f"[DEBUG] Evaluating: {value} != {threshold}")
            return value != threshold
        else:
            return False
    except Exception:
        return False


def map_alarm_channel_to_analog_key(ch_name):
    ch_name = ch_name.strip().lower()

    if ch_name.startswith("a"):
        # A1 → analog_ch_0
        idx = int(ch_name[1:]) - 1
        return f"analog_ch_{idx}"

    if "channel" in ch_name:
        # Channel 1 → analog_ch_0
        idx = int(ch_name.split()[-1]) - 1
        return f"analog_ch_{idx}"

    return None

def process_modbus_tcp_alarms(
    plc_type: str,
    plc_key: str,
    data: dict,
):
    """
    Evaluate Modbus TCP alarms for one PLC cycle
    """

    tcp_cfg = (
        config
        .get("alarmSettings", {})
        .get("ModbusTCP", {})
        .get(plc_type, {})
        .get(plc_key, {})
    )

    if not tcp_cfg:
        log_warn(f"[WARN] No tcp configuration")
        return

    log_info(f"[INFO] Modbus TCP Alarms config: {tcp_cfg}")

    for tag_name, alerts in tcp_cfg.items():
        if tag_name not in data:
            continue

        value = data[tag_name]


        if value is None:
            continue

        value = data[tag_name]

        for alert in alerts:
            if not alert.get("enabled", False):
                continue

            condition = alert.get("condition")
            threshold = alert.get("threshold")
            email = alert.get("email")
            message = alert.get("message", "Modbus TCP Alarm")

            if threshold in ("", None):
                continue

            try:
                threshold = float(threshold)
            except ValueError:
                continue

            if not evaluate_condition(value, condition, threshold):
                continue

            subject = f"[ALERT] Modbus TCP Alarm – {plc_key}"

            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
              <h2 style="color:#b30000;">⚠ Modbus TCP Alarm Triggered</h2>
              <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;">
                <tr><td><b>PLC Type</b></td><td>{plc_type}</td></tr>
                <tr><td><b>PLC</b></td><td>{plc_key}</td></tr>
                <tr><td><b>Tag</b></td><td>{tag_name}</td></tr>
                <tr><td><b>Current Value</b></td><td>{value}</td></tr>
                <tr><td><b>Condition</b></td><td>{condition} {threshold}</td></tr>
                <tr><td><b>Message</b></td><td>{message}</td></tr>
                <tr><td><b>Timestamp (IST)</b></td>
                  <td>{datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')}</td>
                </tr>
              </table>
            </body>
            </html>
            """

            if email:
                log_info(f"[ALARM] Sending Modbus TCP email to {email}")
                send_email(email, subject, body)

            log_error(
                f"[ALARM] Modbus TCP {plc_key}/{tag_name}: "
                f"{value} {condition} {threshold}"
            )

ANALOG_ALARM_STATE = {}  # {(channel, alert_index): first_trigger_time}

def process_analog_alarms(analog_readings):
    analog_cfg = config.get("alarmSettings", {}).get("Analog", {})

    now = time.time()

    for channel_str, channel_cfg in analog_cfg.items():
        try:
            channel = int(channel_str)
        except ValueError:
            log_error(f"[ALARM] Invalid analog channel key: {channel_str}")
            continue

        if channel not in analog_readings:
            continue

        value = analog_readings[channel]
        alerts = channel_cfg.get("alerts", [])

        for idx, alert in enumerate(alerts):
            if not alert.get("enabled", False):
                ANALOG_ALARM_STATE.pop((channel, idx), None)
                continue

            condition = alert.get("condition")
            threshold = alert.get("threshold")
            delay = int(alert.get("delay", 0))
            email = alert.get("email", "")
            contact = alert.get("contact", "")
            message = alert.get("message", "")

            if threshold in ("", None):
                continue

            try:
                threshold = float(threshold)
            except ValueError:
                continue

            if evaluate_condition(value, condition, threshold):
                key = (channel, idx)

                # Start delay timer
                if key not in ANALOG_ALARM_STATE:
                    ANALOG_ALARM_STATE[key] = now
                    log_info(
                        f"[ALARM] Channel {channel} condition met, delay started ({delay}s)"
                    )
                    continue

                # Check delay expiry
                if now - ANALOG_ALARM_STATE[key] < delay:
                    continue

                # ---- ALARM FIRED ----
                ANALOG_ALARM_STATE.pop(key, None)

                subject = f"[ALERT] Analog Alarm – Channel {channel}"

                body = f"""
                <html>
                <body style="font-family: Arial;">
                    <h2 style="color:#b30000;">⚠ Analog Alarm Triggered</h2>
                    <table border="1" cellpadding="6" cellspacing="0">
                        <tr><td><b>Channel</b></td><td>{channel}</td></tr>
                        <tr><td><b>Value</b></td><td>{value}</td></tr>
                        <tr><td><b>Condition</b></td><td>{condition} {threshold}</td></tr>
                        <tr><td><b>Message</b></td><td>{message}</td></tr>
                        <tr><td><b>Timestamp (IST)</b></td>
                            <td>{datetime.now(timezone.utc).astimezone(
                                ZoneInfo("Asia/Kolkata")
                            ).strftime('%Y-%m-%d %H:%M:%S')}</td>
                        </tr>
                    </table>
                </body>
                </html>
                """

                if email:
                    log_info(f"[ALARM] Sending email to {email}")
                    send_email(email, subject, body)

                if contact:
                    log_info(f"[ALARM] Contact configured (SMS pending): {contact}")
                    # send_sms(contact, ...)

                log_error(
                    f"[ALARM] Channel {channel} violated: {value} {condition} {threshold}"
                )

            else:
                # Reset delay timer if condition clears
                ANALOG_ALARM_STATE.pop((channel, idx), None)


def analog_reader_loop():
    log_info("[THREAD] Analog reader started")
    local_version = -1
    last_run = 0
    interval = 5 # Default

    while not STOP_EVENT.is_set():
        # Check for config updates specific to this thread
        if local_version != CONFIG_VERSION:
            interval = convert_polling_interval_to_seconds(ANALOG_POLLING_INTERVAL, ANALOG_POLLING_UNIT)
            local_version = CONFIG_VERSION
            log_info(f"[ANALOG] Settings refreshed (Interval: {interval}s)")

        try:
            now = time.time()
            if ANALOG_ENABLED and (now - last_run) >= interval:
                ts = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
                
                with HW_LOCK: # Prevent I2C collision
                    readings = read_all_analog_channels()
                
                if readings:
                    insert_analog_data(ts, readings)
                    process_analog_alarms(readings)
                last_run = now
        except Exception as e:
            log_error(f"[ERROR] Analog reader: {e}")
        
        time.sleep(0.5)


def insert_digital_data(timestamp, readings):
    try:
        print("INSERT_DIGITAL_DATA:", readings)

        if not isinstance(readings, dict):
            log_error("[DIGITAL] readings is not a dict")
            return

        digital_cfg = DIGITAL_INPUT_CONFIG
        db_cfg = DATABASE
        digital_db_cfg = digital_cfg.get("db", {})

        database = digital_db_cfg.get("db_name")
        table = digital_db_cfg.get("table_name") or "digital_reading"

        if not database:
            log_error("[DIGITAL] db_name missing in config")
            return

        di_channels = DIGITAL_INPUT_CONFIG.get("channels", [])
        do_channels = DIGITAL_OUTPUT_CHANNELS


        values = {}
        
        for key, raw_val in readings.items():
            try:
                idx = int(key.split("_")[-1])
            except Exception:
                log_error(f"[DIGITAL] Invalid reading key: {key}")
                continue

            if key.startswith("digital_out_"):
                ch_name = (
                    do_channels[idx].get("name")
                    if idx < len(do_channels)
                    else f"digital_out_{idx}"
                )
            else:
                ch_name = (
                    di_channels[idx].get("name")
                    if idx < len(di_channels)
                    else f"digital_ch_{idx}"
                )

            values[ch_name] = int(bool(raw_val))


        if not values:
            log_error("[DIGITAL] No valid digital values to insert")
            return

        # ---------- LOCAL DB ----------
        local_cfg = db_cfg.get("local")

        if (
            digital_db_cfg.get("upload_local") is True
            and isinstance(local_cfg, dict)
            and local_cfg.get("enabled") is True
            and isinstance(local_cfg.get("cred"), dict)
        ):
            insert_digital_into_single_db(
                local_cfg["cred"],
                table,
                database,
                timestamp,
                values,
            )

        # ---------- CLOUD DB ----------
        cloud_cfg = db_cfg.get("cloud")

        if (
            digital_db_cfg.get("upload_cloud") is True
            and isinstance(cloud_cfg, dict)
            and cloud_cfg.get("enabled") is True
            and isinstance(cloud_cfg.get("cred"), dict)
        ):
            insert_digital_into_single_db(
                cloud_cfg["cred"],
                table,
                database,
                timestamp,
                values,
            )

    except Exception as e:
        log_error(f"[DIGITAL INSERT] Fatal error: {e}")

def insert_digital_into_single_db(db_cred, table, database, timestamp, values):
    conn = None
    try:
        if not isinstance(db_cred, dict):
            log_error("[DIGITAL][DB] Invalid db_cred")
            return

        conn = get_db_connection(db_cred, timeout=3)
        if not conn:
            return

        cur = conn.cursor()

        # Create DB
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        conn.database = database

        # Create table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts DATETIME NOT NULL
            ) ENGINE=InnoDB;
        """)

        # Add missing columns
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        existing_cols = {row[0] for row in cur.fetchall()}

        for col in values.keys():
            if col not in existing_cols:
                cur.execute(
                    f"ALTER TABLE `{table}` ADD COLUMN `{col}` TINYINT(1) DEFAULT 0"
                )

        # Insert row
        cols = ["ts"] + list(values.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        cols_sql = ", ".join(f"`{c}`" for c in cols)

        data = [timestamp] + list(values.values())

        cur.execute(
            f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})",
            data,
        )

        conn.commit()
        log_info(f"[DIGITAL][DB] Inserted into {database}.{table}")

    except Exception as e:
        log_error(f"[DIGITAL][DB] Insert failed: {e}")

    finally:
        if conn:
            conn.close()


def setup_gpio_input(gpio_num):
    """
    Ensures the GPIO is exported and set to 'in' direction.
    """
    base_path = f"/sys/class/gpio/gpio{gpio_num}"
    
    # 1. Export if directory doesn't exist
    if not os.path.exists(base_path):
        try:
            with open("/sys/class/gpio/export", "w") as f:
                f.write(str(gpio_num))
            time.sleep(0.1)  # Wait for udev to create sysfs nodes
        except OSError as e:
            if e.errno != 16: # 16 is 'Device or resource busy'
                raise

    # 2. Set direction to 'in'
    with open(f"{base_path}/direction", "w") as f:
        f.write("in")

def read_sysfs_gpio(gpio_num):
    """
    Ensures pin is ready, then reads the value.
    """
    try:
        # Step A: Ensure pin is exported and set to 'in'
        setup_gpio_input(gpio_num)
        
        # Step B: Read the value
        path = f"/sys/class/gpio/gpio{gpio_num}/value"
        with open(path, "r") as f:
            return int(f.read().strip())
    except Exception as e:
        log_error(f"[SYSFS GPIO ERROR] gpio{gpio_num}: {e}")
        return None

def read_digital_inputs(di_channels, generate=False):
    readings = {}

    for idx, ch in enumerate(di_channels):
        if not ch.get("enabled", False):
            continue

        pin = ch.get("pin")
        if pin is None:
            continue

        try:
            if generate:
                raw = random.randint(0, 1)
                log_info(f"[DI][RANDOM] CH{idx} = {raw}")
            else:
                raw = read_sysfs_gpio(pin)
                if raw is None:
                    continue

            # 🔥 ACTIVE-LOW inversion: 
            # High voltage (1) -> state 0 (OFF)
            # Low voltage (0)  -> state 1 (ON)
            state = 0 if raw == 1 else 1

            readings[f"digital_ch_{idx}"] = state
            log_info(f"[DI] CH{idx} GPIO{pin} raw={raw} state={state}")

            # Assuming check_digital_alarm is defined elsewhere
            check_digital_alarm(idx, state)

        except Exception as e:
            log_error(f"[DI][ERROR] CH{idx} (pin={pin}): {e}")

    return readings


def read_digital_inputs_gpio(di_channels, generate=False):
    readings = {}

    for idx, ch in enumerate(di_channels):
        if not ch.get("enabled", False):
            continue

        pin = ch.get("pin")

        if not generate and pin is None:
            log_warn(f"[DI] CH{idx} has no valid pin, skipping")
            continue

        try:
            if generate:
                state = random.randint(0, 1)
                log_info(f"[DI][RANDOM] CH{idx} = {state}")
            else:
                state = GPIO.input(pin)
                log_info(f"[DI] CH{idx} GPIO {pin} = {state}")

            readings[f"digital_ch_{idx}"] = int(state)

            check_digital_alarm(idx, state)

        except Exception as e:
            log_error(f"[DI][ERROR] CH{idx} (pin={pin}): {e}")

    return readings


ENERGY_ALARM_STATE = {}   # {(slave_id, tag_key, idx): bool}
ENERGY_ALARM_TIMER = {}  # {(slave_id, tag_key, idx): first_trigger_time}


def check_energy_alarms(readings):
    now = time.time()
    alerts_cfg = config.get("alarmSettings", {}).get("ModbusRTU", {})

    for alert_key, alert_list in alerts_cfg.items():

        # Reading must exist with EXACT SAME KEY
        if alert_key not in readings:
            continue

        value = readings[alert_key]

        for idx, alert in enumerate(alert_list):
            state_key = (alert_key, idx)

            if not alert.get("enabled", False):
                ENERGY_ALARM_STATE.pop(state_key, None)
                ENERGY_ALARM_TIMER.pop(state_key, None)
                continue

            raw_threshold = alert.get("threshold")
            try:
                threshold = float(raw_threshold)
            except (TypeError, ValueError):
                log_error(
                    f"[ENERGY ALARM] Invalid threshold "
                    f"key={alert_key} value={raw_threshold!r}"
                )
                continue

            cond = alert.get("condition")
            delay = int(alert.get("delay", 0))
            email = alert.get("email")
            contact = alert.get("contact")
            message = alert.get("message", "")

            triggered = (
                (cond == "<=" and value <= threshold) or
                (cond == "<"  and value <  threshold) or
                (cond == ">=" and value >= threshold) or
                (cond == ">"  and value >  threshold)
            )

            prev_state = ENERGY_ALARM_STATE.get(state_key, False)

            if triggered:
                if state_key not in ENERGY_ALARM_TIMER:
                    ENERGY_ALARM_TIMER[state_key] = now
                    log_info(
                        f"[ENERGY ALARM] {alert_key} trigger detected "
                        f"(delay {delay}s)"
                    )

                if now - ENERGY_ALARM_TIMER[state_key] < delay:
                    continue

                # 🔴 Rising edge
                if not prev_state:
                    ENERGY_ALARM_STATE[state_key] = True
                    ENERGY_ALARM_TIMER.pop(state_key, None)

                    text = (
                        f"{message}\n"
                        f"Tag: {alert_key}\n"
                        f"Value: {value}\n"
                        f"Threshold: {cond} {threshold}"
                    )

                    log_warn(f"[ENERGY ALARM] {text}")

                    if contact:
                        send_sms(contact, text=text)
                    if email:
                        send_email(
                            email,
                            subject="Energy Alarm",
                            body=text
                        )

            else:
                # Condition cleared → reset latch
                ENERGY_ALARM_STATE.pop(state_key, None)
                ENERGY_ALARM_TIMER.pop(state_key, None)


def send_energy_alert(alert_info):
    """Send alert via SMS/Contact and Email"""
    brand = alert_info["brand"]
    slave = alert_info["slave"]
    register = alert_info["register"]
    current = alert_info["current"]
    threshold = alert_info["threshold"]
    condition = alert_info["condition"]
    message = alert_info["message"]
    contact = alert_info["contact"]
    email = alert_info["email"]

    full_message = f"{message}\nCurrent: {current}\nThreshold: {threshold} {condition}"

    log_info(f"[ALERT] Sending: {full_message}")

    # Send SMS/Contact
    if contact:
        try:
            send_sms(contact, full_message)
            log_info(f"[SMS] Sent to {contact}")
        except Exception as e:
            log_error(f"[ERROR] SMS failed: {e}")

    # Send Email
    if email:
        try:
            send_email(email, f"Energy Meter Alert: {brand} S{slave}", full_message)
            log_info(f"[EMAIL] Sent to {email}")
        except Exception as e:
            log_error(f"[ERROR] Email failed: {e}")


def send_email(to_email, subject, body):
    """Send email alert"""
    import smtplib
    from email.mime.text import MIMEText

    try:
        # Get SMTP config from config.json or use defaults
        smtp_config = config.get("smtp", {})
        smtp_server = smtp_config.get("server", "smtp.gmail.com")
        smtp_port = smtp_config.get("port", 587)
        smtp_user = smtp_config.get("user", "")
        smtp_pass = smtp_config.get("password", "")

        msg = MIMEText(body, "html")
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = to_email

        log_info(f"[INFO] Message for mail: {msg}")
        server = smtplib.SMTP(smtp_server, smtp_port)
        log_info(f"[INFO] Server: {server}")
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()

    except Exception as e:
        log_error(f"[ERROR] Email send failed: {e}")
        raise

def modbus_db_writer_loop():
    log_info("[THREAD] Modbus DB writer started")

    while not STOP_EVENT.is_set():
        try:
            ts, reading, channel_meta = MODBUS_DB_QUEUE.get(timeout=0.5)
            insert_energy_data(ts, reading,  channel_meta)
            check_energy_alarms(reading)
            MODBUS_DB_QUEUE.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            log_error(f"[MODBUS][DB] Error: {e}")


def modbus_reader_loop():
    log_info("[THREAD] Modbus reader started")
    local_version = -1
    instruments = None
    last_poll = 0

    while not STOP_EVENT.is_set():
        if local_version != CONFIG_VERSION:
            with RS485_LOCK:
                instruments = setup_modbus_instruments()
            local_version = CONFIG_VERSION
            log_info("[MODBUS] Serial instruments re-initialized")

        try:
            if MODBUS_ENABLED and (time.time() - last_poll >= 5):
                ts = datetime.now(timezone.utc)\
                    .astimezone(ZoneInfo("Asia/Kolkata"))\
                    .strftime("%Y-%m-%d %H:%M:%S")


                with RS485_LOCK:
                    readings, channel_meta = read_modbus_devices_setup_every_time(
                        instruments
                    )
                    MODBUS_DB_QUEUE.put((ts, readings,  channel_meta))
                    log_info(f"[INFO] Readings: {readings} with channel_meta: {channel_meta}")

                last_poll = time.time()

        except Exception as e:
            log_error(f"[MODBUS] Error: {e}")
            instruments = None
        
        time.sleep(0.5)



# ==================================================
# PERIODIC FLUSHER FOR CSV AND 4G/TCP SENDS
# ==================================================
def send_flush_loop():
    """Flushes the aggregated csv_buffer to CSV and handles 4G/TCP sends on cadence/size thresholds."""
    try:
        interval = convert_polling_interval_to_seconds(
            WIRELESS_POLLING_INTERVAL, WIRELESS_POLLING_UNIT
        )
    except Exception:
        interval = 10
    last_send = 0.0
    log_info(
        f"[THREAD] Sender started, interval={interval}s; batch thresholds size>=5 or interval expiry"
    )

    while True:
        try:
            now = time.time()
            do_flush = False
            with csv_lock:
                size = len(csv_buffer)
            if size >= 5:
                do_flush = True
            elif (now - last_send) >= interval and size > 0:
                do_flush = True

            if do_flush:
                # Snapshot and clear
                with csv_lock:
                    snapshot = dict(csv_buffer)
                    csv_buffer.clear()

                # CSV write
                csv_path = get_csv_path()
                write_to_csv(csv_path, snapshot)
                temp_file = "current_readings.csv"
                write_to_csv(temp_file, snapshot)

                # LTE upload if configured
                if COMMUNICATION_MEDIA == "4G/LTE":
                    try:
                        send_data_via_4g(temp_file)
                    except Exception as e:
                        log_error(f"[ERROR] 4G send failed: {e}")

                # TCP broadcast if big enough snapshot
                if len(snapshot) >= 10:
                    payload = json.dumps(snapshot, separators=(",", ":")).encode(
                        "utf-8"
                    )
                    for target in SEND_TARGETS:
                        try:
                            target_ip = target.get("ip", "")
                            target_port = int(target.get("port", 12345))
                            if not target_ip:
                                continue
                            with socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM
                            ) as sock:
                                sock.settimeout(3)
                                sock.connect((target_ip, target_port))
                                sock.sendall(payload)
                                log_info(
                                    f"[INFO] Sent {len(payload)} bytes to {target_ip}:{target_port}"
                                )
                        except Exception as e:
                            log_error(f"[ERROR] TCP send failed -> {e}")

                last_send = now

            time.sleep(0.5)
        except Exception as e:
            log_error(f"[ERROR] Sender loop: {e}")
            time.sleep(1.0)


"""
What: Periodically checks ANALOG_ALARMS rules against latest analog_readings and emits alarms.
Calls: check_analog_alarm()
Required by: main() as a background thread
Notes: Uses analog_readings dict populated by main_data_collection_loop(); checks every 5s.
Side effects: Console alarm output; hook to extend (SMS/Email).
"""


def alarm_monitoring_loop():
    while True:
        try:
            # Check analog alarms
            for channel_key, alarm_cfg in ANALOG_ALARMS.items():
                if alarm_cfg.get("alert_enable") == "Enable":
                    # Get current reading for this channel
                    channel_id = f"analog_ch_{channel_key.replace('A', '')}"
                    if channel_id in analog_readings:
                        current_value = analog_readings[channel_id]
                        check_analog_alarm(channel_key, current_value, alarm_cfg)

            time.sleep(5)  # Check alarms every 5 seconds

        except KeyboardInterrupt:
            log_warn("[INFO] Alarm monitoring stopped by user")
            break
        except Exception as e:
            log_error(f"[ERROR] Alarm monitoring error: {e}")
            time.sleep(5)


"""
What: Evaluate analog value against alarm thresholds (up to 5 levels) and print when exceeded.
Calls: None
Required by: alarm_monitoring_loop()
Notes: Thresholds and contact/message fields come from ANALOG_ALARMS config.
Side effects: Console output; extend for notifications.
"""


def check_analog_alarm(channel_key, value, alarm_cfg):
    for level in range(5):
        level_key = f"level_enable_{level}"
        if alarm_cfg.get(level_key) == "Enable":
            threshold = float(alarm_cfg.get(f"level_threshold_{level}", "0"))
            contact = alarm_cfg.get(f"level_contact_{level}", "")
            message = alarm_cfg.get(f"level_message_{level}", "")

            if value >= threshold:
                log_warn(
                    f"[ALARM] {channel_key}: {message} - Value: {value} >= {threshold} - Contact: {contact}"
                )
                # Here you could implement SMS/email notification


"""
What: Background scheduler for offline data operations, e.g., timed FTP transfers.
Calls: transfer_offline_data()
Required by: main() as a background thread
Notes: If OFFLINE_ENABLED and OFFLINE_MODE=='schedule', performs transfer at configured clock time.
Side effects: Potential network transfers; logging.
"""


def offline_data_handler():
    while True:
        try:
            if OFFLINE_ENABLED:
                if OFFLINE_MODE == "schedule":
                    schedule = OFFLINE_SCHEDULE
                    target_hour = schedule.get("hour", 0)
                    target_min = schedule.get("min", 0)
                    target_sec = schedule.get("sec", 0)

                    current = datetime.now()
                    if (
                        current.hour == target_hour
                        and current.minute == target_min
                        and current.second == target_sec
                    ):
                        log_info("[INFO] Scheduled offline data transfer")
                        # Transfer data via FTP
                        transfer_offline_data()

                time.sleep(60)  # Check every minute
            else:
                time.sleep(300)  # Check every 5 minutes if disabled

        except KeyboardInterrupt:
            log_info("[INFO] Offline data handler stopped by user")
            break
        except Exception as e:
            log_error(f"[ERROR] Offline data handler error: {e}")
            time.sleep(60)


"""
What: Perform FTP transfer using OFFLINE_FTP configuration (currently a stub/log-only).
Calls: (Potentially ftplib in future)
Required by: offline_data_handler()
Notes: Replace stub with actual FTP upload logic if required.
Side effects: None currently (only logs).
"""


def transfer_offline_data():
    try:
        ftp_cfg = OFFLINE_FTP
        server = ftp_cfg.get("server", "")
        username = ftp_cfg.get("user", "")
        password = ftp_cfg.get("pass", "")
        port = ftp_cfg.get("port", 21)
        folder = ftp_cfg.get("folder", "")

        if server and username:
            log_info(
                f"[INFO] Would transfer data to FTP server {server}:{port}/{folder}"
            )
            # Implement actual FTP transfer here
            # import ftplib
            # ftp = ftplib.FTP()
            # ftp.connect(server, port)
            # ftp.login(username, password)
            # ... transfer files

    except Exception as e:
        log_error(f"[ERROR] FTP transfer failed: {e}")


def poll_plc(plc_config, plc_type, thread_id):
    log_info(f"[PLC THREAD STARTED] {thread_id}")
    local_version = CONFIG_VERSION

    try:
        if plc_type == "Siemens":
            plc_obj = S7_protocol(plc_config)

        elif plc_type in ["Allen Bradley", "Delta"]:
            plc_obj = ModbusTCPProtocol(plc_config)

        else:
            raise ValueError(f"Unsupported PLC type: {plc_type}")

        while not STOP_EVENT.is_set():
            # Detect config change
            if local_version != CONFIG_VERSION:
                log_info(f"[{thread_id}] Config version changed. Exiting thread.")
                break

            plc_obj.plc_to_db()   # MUST be single-cycle
            time.sleep(1)

    except Exception as e:
        log_error(f"[{thread_id}] PLC polling error: {e}")

    finally:
        log_info(f"[PLC THREAD STOPPED] {thread_id}")



"""
What: Periodically iterate configured Modbus TCP entries and (stub) read data.
Calls: (Future) pymodbus or similar
Required by: main() as background thread when MODBUS_TCP_ENABLED True
Notes: Currently logs intended actions; implement actual TCP reads per device.
Side effects: None besides logs (for now).
"""


PLC_REGISTRY = {} # Keep track of {plc_id: thread_object}

def modbus_tcp_handler():
    log_info("[THREAD] Modbus TCP Manager started")
    local_version = -1

    while not STOP_EVENT.is_set():
        if local_version != CONFIG_VERSION:
            log_info("[MODBUS TCP] Config change detected. Updating PLC sub-threads...")
            
            # Note: We don't kill threads here. 
            # We let existing poll_plc threads detect the version change themselves.
            # We only need to start threads for NEW configurations if they aren't running.
            
            if MODBUS_TCP_ENABLED:
                # Start Siemens PLCs
                for i, cfg in enumerate(SIEMENS_CONFIGS):
                    thread_id = f"Siemens_{i}"
                    ensure_plc_thread_running(thread_id, cfg, "Siemens")

                # Start Allen Bradley / Delta PLCs
                for i, cfg in enumerate(COMBINED_CONFIG):
                    thread_id = f"AB_Delta_{i}"
                    ensure_plc_thread_running(thread_id, cfg, "Allen Bradley")
            
            local_version = CONFIG_VERSION

        time.sleep(2)

def ensure_plc_thread_running(thread_id, config, plc_type):
    global PLC_REGISTRY
    with THREAD_LOCK:
        if thread_id not in PLC_REGISTRY or not PLC_REGISTRY[thread_id].is_alive():
            log_info(f"[MODBUS TCP] Launching independent thread for {thread_id}")
            t = threading.Thread(
                target=poll_plc, 
                args=(config, plc_type, thread_id), 
                name=thread_id, 
                daemon=True
            )
            t.start()
            PLC_REGISTRY[thread_id] = t
        


"""
What: Read last processed timestamp for polling deltas from a file.
Calls: built-in file I/O
Required by: poll_new_data()
Notes: File path fixed to /home/gateway/GATEWAY-COMPLETE/Demo application/Main Application/last_kafka_timestamp.txt.
Side effects: None.
"""


def get_last_timestamp():
    timestamp_file = "/home/gateway/GATEWAY-COMPLETE/Demo application/Main Application/last_kafka_timestamp.txt"
    try:
        if os.path.exists(timestamp_file):
            with open(timestamp_file, "r") as f:
                return f.read().strip()
        return None
    except Exception as e:
        log_error(f"[ERROR] Failed to read last timestamp: {e}")
        return None


"""
What: Persist last processed timestamp to a file for incremental polling.
Calls: built-in file I/O
Required by: poll_new_data()
Notes: Writes to /home/gateway/GATEWAY-COMPLETE/Demo application/Main Application/last_kafka_timestamp.txt.
Side effects: Overwrites file.
"""


def set_last_timestamp(timestamp):
    timestamp_file = "/home/gateway/GATEWAY-COMPLETE/Demo application/Main Application/last_kafka_timestamp.txt"
    try:
        with open(timestamp_file, "w") as f:
            f.write(str(timestamp))
    except Exception as e:
        log_error(f"[ERROR] Failed to save timestamp: {e}")


"""
What: Poll HTTP API for new data since last timestamp and process/store it.
Calls: get_last_timestamp(), requests.get(), process_external_data(), set_last_timestamp()
Required by: (Not launched in main; add a thread if needed)
Notes: GETs {JSON_ENDPOINT}/api/new-data with optional 'since' param; expects JSON array.
Side effects: DB insert via process_external_data(); updates timestamp file.
"""


def poll_new_data():
    since = get_last_timestamp()
    http_new_url = f"{JSON_ENDPOINT}/api/new-data"  # Construct new data URL

    try:
        params = {"since": since} if since else {}
        response = requests.get(http_new_url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if data:
                log_info(f"[INFO] New records received: {len(data)} items")
                # Process the new data
                for record in data:
                    log_info(f"[DATA] Record: {record}")
                    # You can store this data or process it as needed
                    timestamp = record.get("timestamp")
                    if timestamp:
                        # Insert into database or process
                        process_external_data(record)

                # Update last timestamp
                if isinstance(data, list) and data:
                    latest_ts = max([d.get("timestamp", "") for d in data])
                    if latest_ts:
                        set_last_timestamp(latest_ts)
            else:
                log_info("[INFO] No new records found.")
        else:
            log_warn(f"[WARN] Poll failed: HTTP {response.status_code}")

    except Exception as e:
        log_error(f"[ERROR] Polling error: {e}")


"""
What: Persist a single external data record to MySQL (external_data table).
Calls: mysql.connector.connect(), cursor.execute(), conn.commit()
Required by: poll_new_data() after fetching new items from the HTTP endpoint.
Notes:
    - Auto-creates the 'external_data' table with columns:
        id (AUTO_INCREMENT), timestamp (DATETIME), data_source (VARCHAR),
        data_json (JSON), received_at (CURRENT_TIMESTAMP).
    - Stores the entire input 'record' as JSON in the data_json column and sets
    data_source='http_poll' by default.
    - Expects 'record' to be a dict; if 'timestamp' key exists, it's inserted
    into the timestamp column, otherwise NULL will be stored.
    - Uses MYSQL_CONFIG for connection parameters; ensure credentials/db exist.
    - Commits the transaction per call; batching may improve performance if needed.
Side effects:
    - Writes to MySQL (creates table if missing).
    - Prints status or error messages to stdout.
Failure modes:
    - Any DB connection/SQL errors are caught and logged; the exception is not re-raised.
    """


def process_external_data(record):
    try:
        # Insert external data into database
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS external_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                data_source VARCHAR(100),
                data_json JSON,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            "INSERT INTO external_data (timestamp, data_source, data_json) VALUES (%s, %s, %s)",
            (record.get("timestamp"), "http_poll", json.dumps(record)),
        )

        conn.commit()
        cursor.close()
        conn.close()
        log_info(f"[INFO] Stored external data record")

    except Exception as e:
        log_error(f"[ERROR] Failed to store external data: {e}")


"""
What: Handle config updates received over Kafka (dynamic parameter changes).
Calls: None
Required by: Potential Kafka command processing flow.
Notes: Example updates WIRELESS_POLLING_INTERVAL from message['config'].
Side effects: Mutates globals.
"""


def process_config_update(message):
    try:
        config_data = message.get("config", {})
        log_info(f"[INFO] Processing config update: {config_data}")

        if "pollingInterval" in config_data:
            global WIRELESS_POLLING_INTERVAL
            WIRELESS_POLLING_INTERVAL = config_data["pollingInterval"]
            log_info(f"[INFO] Updated polling interval to {WIRELESS_POLLING_INTERVAL}")

    except Exception as e:
        log_error(f"[ERROR] Config update processing failed: {e}")


# --- Kafka Consumer/Producer helpers ---

"""
What: Deserialize bytes to JSON for Kafka consumer; returns None on empty or invalid JSON.
Calls: json.loads()
Required by: start_local_kafka_consumer()
Notes: Logs invalid JSON and skips it.
Side effects: None.
"""


def safe_json_deserializer(v):
    if not v:
        return None
    try:
        val = json.loads(v.decode("utf-8"))
        log_info(val)
        return val
    except json.JSONDecodeError:
        log_warn(f"[WARN] NON-JSON message skipped: {v}")
        return None


def ensure_topic_exists(topic_name):
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        existing_topics = admin.list_topics()
        if topic_name not in existing_topics:
            log_info(f"[INFO] Topic '{topic_name}' not found, creating...")
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin.create_topics(new_topics=[topic], validate_only=False)
            log_info(f"[INFO] Topic '{topic_name}' created.")
        else:
            log_info(f"[INFO] Topic '{topic_name}' already exists.")
    except Exception as e:
        log_error(f"[ERROR] Could not ensure topic exists: {e}")


"""
What: Start a Kafka consumer for KAFKA_TOPIC and batch-forward messages via flush_batch_to_api().
Calls: flush_batch_to_api()
Required by: kafka_local_consumer_thread()
Notes: Commits offsets only on successful batch send; batches by size/time.
Side effects: Network to Kafka and API; console logs.
"""


def start_local_kafka_consumer():
    topic = KAFKA_TOPIC or "iot-readings"

    ensure_topic_exists(topic)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS or ["127.0.0.1:9092"],
        group_id="gateway-consumer",
        auto_offset_reset="latest",
        value_deserializer=safe_json_deserializer,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )

    batch = []
    last_flush = time.time()
    batch_max = int(config.get("kafka", {}).get("batchMaxMessages", 200))
    batch_sec = int(config.get("kafka", {}).get("batchMaxSeconds", 5))

    log_info(
        f"[INFO] Kafka consumer started on topic {topic} (batch {batch_max}/{batch_sec}s)"
    )

    while True:
        try:
            for msg in consumer:
                record = {"key": msg.key, "value": msg.value, "ts": time.time()}
                batch.append(record)
                log_info(batch)
                if len(batch) >= 10 or (time.time() - last_flush) >= batch_sec:
                    if flush_batch_to_api(batch):
                        consumer.commit()
                        batch.clear()
                        last_flush = time.time()
                    else:
                        time.sleep(2)

            if batch and (time.time() - last_flush) >= batch_sec:
                if flush_batch_to_api(batch):
                    consumer.commit()
                    batch.clear()
                    last_flush = time.time()

            time.sleep(0.1)

        except KeyboardInterrupt:
            log_info("[INFO] Kafka consumer stopping...")
            break
        except Exception as e:
            log_error(f"[ERROR] Kafka consumer error: {e}")
            time.sleep(2)


"""
What: Send batched messages to external API via LTE. Returns True on success.
Calls: (Commented out: requests.post to JSON_ENDPOINT)
Required by: start_local_kafka_consumer()
Notes: Currently returns True as a stub; implement actual POST if needed.
Side effects: None in stub form.
"""


def flush_batch_to_api(batch):
    # Real implementation commented out in original code.
    return True


"""
What: Wrapper to run start_local_kafka_consumer() with basic crash handling.
Calls: start_local_kafka_consumer()
Required by: main() to start consumer thread.
Side effects: Starts consumption loop.
"""


def kafka_local_consumer_thread():
    try:
        start_local_kafka_consumer()
    except Exception as e:
        log_error(f"[ERROR] Consumer thread crashed: {e}")
        time.sleep(2)


"""
What: Serialize Kafka message values: dict/list->JSON bytes, str->utf-8, else str(v)->utf-8.
Calls: json.dumps()
Required by: init_local_kafka_producer()
Side effects: None.
"""


def value_serializer(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":")).encode("utf-8")
    elif isinstance(v, str):
        return v.encode("utf-8")
    else:
        return str(v).encode("utf-8")


"""
What: Initialize a KafkaProducer using KAFKA_BROKERS and config settings.
Calls: KafkaProducer(...)
Required by: kafka_publish() on first use
Notes: Uses compression, batching, and value/key serializers. Prints chosen topic.
Side effects: Opens connection to Kafka.
"""


def init_local_kafka_producer():
    global kafka_producer, KAFKA_TOPIC
    kafka_topic = KAFKA_TOPIC or "iot-readings"
    compression = config.get("kafka", {}).get("compression", "gzip") or "gzip"
    acks = config.get("kafka", {}).get("acks", "1")
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS or ["127.0.0.1:9092"],
        compression_type=compression,
        linger_ms=100,
        batch_size=32768,
        value_serializer=value_serializer,
        key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else None),
        retries=3,
        max_in_flight_requests_per_connection=1,
    )

    log_info(f"[INFO] Kafka producer initialized on {kafka_topic}")


"""
What: Non-blocking publish to Kafka; source_key e.g. 'analog','digital','modbus','rs485','tcp'
Calls: init_local_kafka_producer() on first publish
Required by: start_tcp_server() handler, main_data_collection_loop(), process_rs485_data(), etc.
Notes: Waits for send() future with timeout=10, then flushes. Rate-limits error logs.
Side effects: Network to Kafka; console logs on error.
"""


def kafka_publish(source_key, payload):
    global kafka_producer, kafka_last_err, KAFKA_TOPIC
    if not kafka_producer:
        log_info("[INFO] Initializing Kafka producer lazily")
        init_local_kafka_producer()
    try:
        kafka_producer.send(KAFKA_TOPIC, key=source_key, value=payload).get(timeout=10)
        kafka_producer.flush()
    except Exception as e:
        now = time.time()
        if now - kafka_last_err > 10:
            log_warn(f"[WARN] Kafka publish failed: {e}")
            kafka_last_err = now


"""
What: Poll HTTP endpoint for commands and dispatch to process_remote_command().
Calls: requests.get(), process_remote_command()
Required by: main() as background thread
Notes: Polls every 30s from {JSON_ENDPOINT}/commands; expects JSON array of commands.
Side effects: None besides logs; could modify runtime if commands implemented.
"""


def http_command_poller():
    poll_url = JSON_ENDPOINT

    while True:
        try:
            response = requests.get(f"{poll_url}/commands", timeout=5)
            if response.status_code == 200:
                commands = response.json()
                log_info(f"[INFO] Received commands: {commands}")

                for command in commands:
                    process_remote_command(command)
            else:
                log_warn(f"[WARN] Command polling failed: HTTP {response.status_code}")

        except Exception as e:
            log_error(f"[ERROR] Command polling error: {e}")

        time.sleep(30)


"""
What: Process a remote command structure {type, data}.
Calls: None (extend to call appropriate handlers)
Required by: http_command_poller()
Notes: Stub with logging for config_update, restart, data_request.
Side effects: None currently (just logs).
"""


def process_remote_command(command):
    try:
        cmd_type = command.get("type", "")
        cmd_data = command.get("data", {})

        if cmd_type == "config_update":
            log_info(f"[INFO] Config update command received: {cmd_data}")
            # Update configuration dynamically

        elif cmd_type == "restart":
            log_info("[INFO] Restart command received")
            # Implement restart logic

        elif cmd_type == "data_request":
            log_info(f"[INFO] Data request command: {cmd_data}")
            # Send specific data

        else:
            log_warn(f"[WARN] Unknown command type: {cmd_type}")
    except Exception as e:
        log_error(f"[ERROR] Command processing error: {e}")


"""
What: Handle RS485 communication: periodically send latest data, and read/process inbound lines.
Calls: process_rs485_data()
Required by: main() as background thread
Notes: Writes a single-line JSON-framed message prefixed with 'RS485_DATA:'; replace with protocol as needed.
Side effects: Serial I/O; DB/Kafka via process_rs485_data().
"""


def rs485_communication_handler():
    try:
        ser = serial.Serial(RS485_PORT, MODBUS_BAUD_RATE, timeout=1)
        log_info(
            f"[INFO] RS485 communication started on {RS485_PORT} at {MODBUS_BAUD_RATE} baud"
        )

        while True:
            # Send any queued data
            if data_map:
                latest_timestamp = max(data_map.keys())
                latest_data = data_map[latest_timestamp]
                message = f"RS485_DATA:{json.dumps(latest_data)}\n"
                ser.write(message.encode())
                log_info(f"[INFO] Sent RS485 data: {len(message)} bytes")

            # Check for incoming data
            if ser.in_waiting:
                line = ser.readline().decode(errors="ignore").strip()
                if line:
                    log_info(f"[RS485 IN] {line}")
                    process_rs485_data(line)

            time.sleep(2)

    except Exception as e:
        log_error(f"[ERROR] RS485 communication error: {e}")


"""
What: Process incoming RS485 data lines that start with 'RS485_DATA:' containing JSON.
Calls: insert_readings_mysql(), kafka_publish()
Required by: rs485_communication_handler()
Notes: Stores received values into DB with sensor_id prefixed 'rs485_' and publishes to Kafka.
Side effects: DB insert; Kafka publish; console logs.
"""


def process_rs485_data(data):
    try:
        if data.startswith("RS485_DATA:"):
            json_data = data[11:]  # Remove prefix
            parsed_data = json.loads(json_data)
            log_info(f"[INFO] Received RS485 data: {parsed_data}")

            # Store or process the received data
            timestamp = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
            insert_readings_mysql(
                timestamp, {f"rs485_{k}": v for k, v in parsed_data.items()}
            )
            # kafka_publish("rs485", {"ts": datetime.now(timezone.utc).isoformat(), "data": parsed_data})
    except Exception as e:
        log_error(f"[ERROR] RS485 data processing error: {e}")


"""
What: Monitor system resource usage and persist periodic status metrics.
Calls: mysql.connector.connect() to insert into system_status
Required by: main() as background thread
Notes: Logs CPU/Memory/Disk; also checks network interfaces (LTE, eth0, wlan0).
Side effects: DB insert each minute; console logs.
"""


def system_status_monitor():
    while True:
        try:
            # Check system resources
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk_usage = psutil.disk_usage("/")

            log_info(
                f"[STATUS] CPU: {cpu_percent}%, Memory: {memory.percent}%, Disk: {disk_usage.percent}%"
            )

            # Check network interfaces
            net_stats = psutil.net_if_stats()
            for interface, stats in net_stats.items():
                if interface in [LTE_INTERFACE, "eth0", "wlan0"]:
                    status = "UP" if stats.isup else "DOWN"
                    log_info(f"[STATUS] {interface}: {status}")

            # Log system status
            status_data = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": disk_usage.percent,
                "timestamp": datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S'),
            }

            # Insert system status into database
            try:
                conn = mariadb.connect(**MYSQL_CONFIG)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS system_status (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        cpu_percent FLOAT,
                        memory_percent FLOAT,
                        disk_percent FLOAT
                    )
                """
                )
                cursor.execute(
                    "INSERT INTO system_status (timestamp, cpu_percent, memory_percent, disk_percent) VALUES (%s, %s, %s, %s)",
                    (
                        status_data["timestamp"],
                        cpu_percent,
                        memory.percent,
                        disk_usage.percent,
                    ),
                )
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                log_error(f"[ERROR] System status database insert failed: {e}")

            time.sleep(60)  # Monitor every minute

        except KeyboardInterrupt:
            log_info("[INFO] System status monitor stopped by user")
            break
        except Exception as e:
            log_error(f"[ERROR] System status monitor error: {e}")
            time.sleep(60)


# ==================================================
# WIFI, 4G, Ethernet
# ==================================================

#!/usr/bin/env python3

import json
import time
import hashlib
import subprocess
import serial
from pathlib import Path
import os
import logging
from datetime import datetime


# ---------------- PATHS ----------------
BASE = Path("/home/recomputer/Gateway-UI/Main Application/")

CONFIG_FILE  = BASE / "config.json"
UPDATED_FILE = BASE / "is_updated.json"
STATE_FILE   = BASE / "last_network_state.json"

SERIAL_PORT = "/dev/ttyUSB2"
BAUDRATE    = 115200
MODEM_USB_ID = "1e0e:9011"

# ---------------- LOGGING ----------------
LOG_BASE = Path("/home/recomputer/logs")
TODAY = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = LOG_BASE / TODAY / "Network"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "network.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  # keeps console output
    ]
)

log = logging.getLogger("network")


# ---------------- HELPERS ----------------
def load_json(path, default=None):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default

def write_json(path, data):
    path.write_text(json.dumps(data, indent=2))

def hash_dict(d):
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()

def modem_present():
    out = subprocess.run(["lsusb"], capture_output=True, text=True).stdout
    return MODEM_USB_ID in out

def sim_present(ser):
    resp = send_at(ser, "AT+CPIN?", 1)
    return "OK" in resp


def free_port(port):
    subprocess.run(["sudo", "fuser", "-k", port], check=False)
    time.sleep(1)

def wait_for_tty(port, timeout=40):
    log_info(f"[4G] Waiting for {port}")
    for _ in range(timeout):
        if os.path.exists(port):
            return
        time.sleep(1)
    raise RuntimeError("Serial port did not appear")

def send_at(ser, cmd, wait=1):
    log_info(f"[AT] {cmd}")
    ser.write((cmd + "\r").encode())
    time.sleep(wait)
    resp = ser.read(ser.in_waiting or 256).decode(errors="ignore")
    if resp.strip():
        log_info(resp.strip())
    return resp

# ---------------- LTE REGISTRATION WAIT ----------------
def wait_for_lte(ser, present, timeout=5):
    log_info("[4G] Waiting for LTE registration")
    # if present:
    #     send_at(ser,"AT+CRESET",45)
    for _ in range(timeout):
        send_at(ser, "AT+CREG", 1)
        send_at(ser, "AT+CGREG", 1)
        send_at(ser, "AT+CEREG", 1)
        send_at(ser, "AT+CNSMOD", 1)
        creg  = send_at(ser, "AT+CREG?", 1)
        cgreg = send_at(ser, "AT+CGREG?", 1)
        cereg = send_at(ser, "AT+CEREG?", 1)
        mode  = send_at(ser, "AT+CNSMOD?", 1)

        if ("CREG: 1" in creg or "CREG: 0,1" in creg):
            log_info("[4G] LTE registered")
            return

        time.sleep(2)

    raise RuntimeError("LTE registration timeout")

# ---------------- WIFI ----------------
def connect_wifi(ssid, password):
    if not ssid:
        log_info("[WiFi] No SSID configured")
        return

    # 1️⃣ Check current active Wi-Fi
    active = subprocess.run(
        ["nmcli", "-t", "-f", "ACTIVE,SSID", "dev", "wifi"],
        capture_output=True, text=True
    ).stdout

    for line in active.splitlines():
        if line.startswith("yes:") and line.split(":", 1)[1] == ssid:
            log_info(f"[WiFi] Already connected to {ssid}")
            return

    # 2️⃣ Check saved connections
    saved = subprocess.run(
        ["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"],
        capture_output=True, text=True
    ).stdout

    log_info(f"[INFO] Saved WIFI: {saved}")
    for line in saved.splitlines():
        name, ctype = line.split(":", 1)
        if name == ssid and ctype == "802-11-wireless":
            log_info(f"[WiFi] Bringing up saved connection {ssid}")
            subprocess.run(
                ["nmcli", "connection", "up", ssid],
                check=False
            )
            return

    # 3️⃣ New network → password required
    if not password:
        log_info(f"[WiFi] Password required for new network {ssid}")
        return

    log_info(f"[WiFi] Connecting to new network {ssid}")
    subprocess.run(
        ["nmcli", "dev", "wifi", "connect", ssid, "password", password],
        check=False
    )


# ---------------- ROUTING ----------------
def fix_routes():
    subprocess.run(["sudo", "ip", "route", "del", "default", "dev", "usb0"], check=False)
    subprocess.run([
        "sudo", "ip", "route", "add",
        "default", "via", "192.168.225.1",
        "dev", "usb0", "metric", "900"
    ], check=False)

# ---------------- 4G ----------------
def setup_4g(apn):
    if not apn:
        log.warning("APN not configured — skipping attach")
        subprocess.run(["sudo","ip","link","set","usb0","down"],check=False)
        time.sleep(2)
        return
    log_info("[4G] Starting CFUN-based attach (Jio-safe)")
    if not modem_present():
        subprocess.run(["sudo","gpioset","0","6=1"],check=False)
        time.sleep(8)

    free_port(SERIAL_PORT)
    wait_for_tty(SERIAL_PORT)

    with serial.Serial(SERIAL_PORT, BAUDRATE, timeout=2) as ser:
        send_at(ser, "AT")
        if not sim_present(ser):
            log_info("No Sim Inserted")
            subprocess.run(["sudo","ip","link","set","usb0","down"],check=False)
            time.sleep(2)
            return
        # 🔥 CFUN reset (THIS IS THE KEY)
        send_at(ser, "AT+CFUN=0", 2)
        time.sleep(15)

        send_at(ser, "AT+CFUN=1", 2)
        time.sleep(20)

        # Wait for LTE registration
        log_info("[4G] Waiting for network registration")

        send_at(ser, "AT+CREG", 1)
        send_at(ser, "AT+CGREG", 1)
        send_at(ser, "AT+CEREG", 1)
        send_at(ser, "AT+CNSMOD", 1)

        for _ in range(40):
            creg  = send_at(ser, "AT+CREG?", 1)
            cgreg = send_at(ser, "AT+CGREG?", 1)
            cereg = send_at(ser, "AT+CEREG?", 1)
            cns   = send_at(ser, "AT+CNSMOD?", 1)
            log_info(creg, cgreg, cereg, cns)

            if (
                ("CREG: 1" in creg or "CREG: 0,1" in creg) and
                ("CGREG: 1" in cgreg or "CGREG: 0,1" in cgreg) and
                ("CEREG: 1" in cereg or "CEREG: 0,1" in cereg)
            ):
                log_info("[4G] Registered on LTE")
                break

            time.sleep(2)
        else:
            raise RuntimeError("LTE registration failed")

        # Attach packet service (retry like you did manually)
        for _ in range(10):
            resp = send_at(ser, "AT+CGATT=1", 2)
            if "OK" in resp:
                break
            time.sleep(3)

        # Set APN
        send_at(ser, f'AT+CGDCONT=1,"IPV4V6","{apn}"', 2)

        # Activate PDP (IGNORE ERROR — Jio behavior)
        send_at(ser, "AT+CGACT=1,1", 2)

        # Poll for IP (THIS IS THE REAL SUCCESS SIGNAL)
        log_info("[4G] Waiting for IP from network")
        for _ in range(30):
            ipinfo = send_at(ser, "AT+CGPADDR=1", 2)
            if "+CGPADDR:" in ipinfo:
                log_info("[4G] IP assigned")
                break
            time.sleep(3)
        else:
            raise RuntimeError("No IP assigned by network")

    subprocess.run(["sudo", "dhclient", "usb0"], check=False)
    log_info("[4G] LTE READY (CFUN path)")

# ---------------- STATIC ETHERNET (DHCP + STATIC IP) ----------------
ETH_CONNECTION = "Wired connection 1"
ETH_CONNECTION2 = "Wired connection 2"

# ---------------- ETH IP CHECK ----------------
def eth0_has_ip(ip):
    """
    Returns True if eth0 already has the given IPv4 address
    """
    try:
        out = subprocess.run(
            ["ip", "-4", "addr", "show", "dev", "eth0"],
            capture_output=True,
            text=True,
            check=True
        ).stdout

        for line in out.splitlines():
            if line.strip().startswith("inet "):
                addr = line.split()[1].split("/")[0]
                if addr == ip:
                    return True
        return False

    except Exception as e:
        log.exception("[ETH] Failed to read eth0 addresses")
        return False


def apply_static_ethernet(static_cfg, iface, prev_cfg):
    """
    Apply DHCP or DHCP + secondary static IP on a given interface
    """
    iface = ETH_CONNECTION2 if iface=="eth1" else ETH_CONNECTION

    if static_cfg == prev_cfg:
        log_info(f"[{iface}] No config change")
        return static_cfg

    enabled = static_cfg.get("enabled", False)

    try:
        if not enabled:
            log_info(f"[{iface}] Restoring pure DHCP")

            subprocess.run([
                "sudo", "nmcli", "con", "mod", iface,
                "ipv4.addresses", "",
                "ipv4.method", "auto",
                "ipv4.ignore-auto-dns", "no",
                "ipv4.dns", ""
            ], check=False)

        else:
            ip = static_cfg.get("ip")
            subnet = static_cfg.get("subnet")

            if not ip or not subnet:
                log_warning(f"[{iface}] Static enabled but IP/subnet missing")
                return prev_cfg

            cidr = sum(bin(int(x)).count("1") for x in subnet.split("."))
            addr = f"{ip}/{cidr}"

            dns1 = static_cfg.get("dns_primary", "")
            dns2 = static_cfg.get("dns_secondary", "")
            dns = " ".join(d for d in [dns1, dns2] if d)

            log_info(f"[{iface}] Applying DHCP + static {addr}")

            subprocess.run([
                "sudo", "nmcli", "con", "mod", iface,
                "ipv4.method", "auto",
                "ipv4.addresses", addr
            ], check=True)

            subprocess.run([
                "sudo", "nmcli", "con", "mod", iface,
                "ipv4.ignore-auto-dns", "yes"
            ], check=True)

            if dns:
                subprocess.run([
                    "sudo", "nmcli", "con", "mod", iface,
                    "ipv4.dns", dns
                ], check=True)

        subprocess.run(["sudo", "nmcli", "con", "down", iface], check=False)
        subprocess.run(["sudo", "nmcli", "con", "up", iface], check=False)

        log_info(f"[{iface}] Network applied successfully")
        return static_cfg

    except Exception:
        log.exception(f"[{iface}] Failed to apply network config")
        return prev_cfg


def network_watcher_loop():
    log_info("[INFO] Network watcher started")

    prev_static = {}
    prev_static2 = {}

    while not STOP_EVENT.is_set():
        try:
            config_data = load_json(CONFIG_FILE, {})
            network = config_data.get("network", {})

            # WiFi & 4G (still unconditional — acceptable for now)
            connect_wifi(
                network.get("wifi", {}).get("ssid", ""),
                network.get("wifi", {}).get("password", "")
            )
            setup_4g(network.get("sim4g", {}).get("apn", ""))

            # ---- ETH0 ----
            prev_static = apply_static_ethernet(
                network.get("static", {}),
                "eth0",
                prev_static
            )

            # ---- ETH1 / STATIC2 ----
            static2 = network.get("static2", {})
            iface2 = static2.get("iface")

            if iface2:
                prev_static2 = apply_static_ethernet(
                    static2,
                    iface2,
                    prev_static2
                )

        except Exception as e:
            log_error(f"[NETWORK] Watcher error: {e}")

        time.sleep(10)


# ==================================================
# MAIN EXECUTION
# ==================================================

"""
What: Main entrypoint that initializes 4G (if configured), starts all background threads,
        and runs the main data collection loop.
Calls:
    - enable_4g_module(), connect_4g() when COMMUNICATION_MEDIA == "4G/LTE"
    - start_config_monitor(), start_tcp_server(), http_command_poller(),
    alarm_monitoring_loop(), offline_data_handler(), modbus_tcp_handler(),
    kafka_local_consumer_thread(), rs485_communication_handler(), system_status_monitor()
    - main_data_collection_loop()
Side effects: Launches multiple daemon threads; sets up hardware/network; runs indefinitely.
"""


def main():
    # Initial load
    if not UPDATED:
        update_global_config()

    log_info("=" * 60)
    log_info("INDEPENDENT DATA LOGGER SYSTEM STARTING")
    log_info("=" * 60)

    # 1. Start the Configuration Monitor (The Heart)
    # This thread stays alive forever and updates CONFIG_VERSION
    monitor_thread = threading.Thread(
        target=config_monitor_loop,
        daemon=False, # We join this on exit
        name="ConfigMonitor"
    )
    monitor_thread.start()

    # 2. Launch all functional threads
    # These threads are written to be self-healing and version-aware
    start_enabled_threads()

    log_info("=" * 60)
    log_info("ALL INDEPENDENT THREADS DISPATCHED")
    log_info("=" * 60)

    try:
        # Keep the main process alive
        while not STOP_EVENT.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        log_info("\n[INFO] Shutdown requested by user")
    except Exception as e:
        log_error(f"\n[CRITICAL] Main crashed: {e}")

    finally:
        log_info("[SHUTDOWN] Signaling all threads to stop...")
        STOP_EVENT.set()
        
        # Give threads time to close sockets/files
        time.sleep(2)
        
        log_info("[SHUTDOWN] System complete.")
        os._exit(0)


if __name__ == "__main__":
    main()
