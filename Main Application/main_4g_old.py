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
import time
import json
import subprocess
from datetime import datetime
import glob
from pathlib import Path
import serial
import sys
import logging
from logging.handlers import TimedRotatingFileHandler


def load_config():
    try:
        with open(CONFIG_FILE_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"[ERROR] Failed to load config: {e}")
        return None

# Setup comprehensive logging with daily rotation
def setup_logging():
    """Initialize logging with daily rotation and multiple handlers"""
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = Path(f"/home/pi/logs/{today}")
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
    "/home/pi/Downloads/Gateway-UI/Main Application"
)
CONFIG_FILE_PATH = f"{BASE_DIR}/config.json"
config = None


"""
What: Load the JSON configuration from CONFIG_FILE_PATH and return its dict.
Calls: json.load()
Required by: update_global_config(), start_config_monitor(), any place needing config.
Notes: Returns None on error; caller should handle fallbacks/logging.
Side effects: None.
"""


PREV_NETWORK_FILE = f"{BASE_DIR}/prev_network.json"
IS_UPDATED_FILE = f"{BASE_DIR}/is_updated.json"


def load_json_file(path):
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    except Exception as e:
        log_error(f"Error loading {path}: {e}")
    return None


def save_json_file(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log_error(f"Error saving {path}: {e}")




# === GLOBAL VARIABLE DECLARATIONS (at module level) ===
# Global configuration variables (shared across functions)
SERIAL_PORT = "/dev/ttyUSB2"
BAUDRATE    = 115200

MODEMS = {
    "simcom": {
        "usb_id": "1e0e:9011",
        "usb_id_2": "1e0e:9001",
        "at_port": "/dev/ttyUSB2",
        "mode": "ecm"
    },
    "quectel": {
        "usb_id": "2c7c:0125",
        "usb_id_2":"",
        "at_port": "/dev/ttyUSB2",
        "ppp_port": "/dev/ttyUSB3",
        "mode": "ppp"
    }
}

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

def get_network_config(config):
    return config.get("network", {})


def is_network_changed(new, old):
    return new != old


def is_update_requested():
    data = load_json_file(IS_UPDATED_FILE)
    return data and data.get("is_updated", False)



def detect_modem():
    out = subprocess.run(["lsusb"], capture_output=True, text=True).stdout
    print(out)
    for name, cfg in MODEMS.items():
        if (cfg["usb_id"].lower() in out.lower()) or (cfg["usb_id_2"].lower() in out.lower()) :
            log_info(f"[MODEM] Detected {name}")
            return name
    return None


def setup_quectel_ppp(apn):
    log_info("[4G] Starting Quectel PPP")

    # kill old sessions
    subprocess.run(["sudo", "poff", "-a"], check=False)
    subprocess.run(["sudo", "killall", "pppd"], check=False)
    time.sleep(2)


    peer_config = f"""
{MODEMS['quectel']['ppp_port']} 115200
connect "/usr/sbin/chat -v -f /etc/chatscripts/quectel-connect"
noauth
defaultroute
replacedefaultroute
usepeerdns
persist
noipdefault
nodetach
"""

    log_info("[4G] Creating PPP peer config")

    subprocess.run(
        ["sudo", "tee", "/etc/ppp/peers/quectel"],
        input=peer_config,
        text=True,
        check=True
    )
    # update chat script dynamically (important!)
    chat = f"""
ABORT "NO CARRIER"
ABORT "ERROR"
ABORT "NO DIALTONE"
ABORT "BUSY"
ABORT "NO ANSWER"
"" AT
OK ATE0
OK AT+CGDCONT=1,"IP","{apn}"
OK ATD*99#
CONNECT \\d\\c
"""

    log_info(f"[INFO] quectel chat {chat}")
    subprocess.run(
        ["sudo", "tee", "/etc/chatscripts/quectel-connect"],
        input=chat,
        text=True,
        check=True
    )

    # clean shutdown only if needed
    subprocess.run(["sudo", "poff", "-a"], check=False)
    time.sleep(2)

    # ensure port is free
    subprocess.run(["sudo", "fuser", "-k", MODEMS['quectel']['ppp_port']], check=False)
    time.sleep(2)

    # start PPP
    subprocess.run(["sudo", "pon", "quectel"], check=False)

    # wait for ppp0
    for _ in range(20):
        if os.path.exists("/sys/class/net/ppp0"):
            log_info("[4G] PPP interface up")
            return
        time.sleep(1)

    raise RuntimeError("PPP failed to come up")

def sim_present(ser):
    resp = send_at(ser, "AT+CPIN?", 1)
    return "OK" in resp

def setup_simcom_ecm(apn):
    if not apn:
        log_warn("APN not configured — skipping attach")
        subprocess.run(["sudo", "ip", "link", "set", "usb0", "down"], check=False)
        return

    log_info("[4G] Starting attach")

    free_port(SERIAL_PORT)
    wait_for_tty(SERIAL_PORT)

    with serial.Serial(SERIAL_PORT, BAUDRATE, timeout=2) as ser:
        init_modem(ser)

        if not sim_present(ser):
            log_warn("No SIM detected")
            subprocess.run(["sudo", "ip", "link", "set", "usb0", "down"], check=False)
            return

        wait_for_registration(ser)
        attach_packet_service(ser)
        set_apn(ser, apn)
        wait_for_ip(ser)
        
    subprocess.run(["sudo", "dhclient", "usb0"], check=False)
    time.sleep(3)

    subprocess.run(
    ["sudo", "ip", "route", "replace", "default", "dev", "usb0", "metric", "50"],
    check=False)    
    log_info("[4G] LTE READY")


# ---------------- CORE STEPS ----------------

def init_modem(ser):
    send_at(ser, "AT")
    send_at(ser, "AT+CFUN=0", 2)
    time.sleep(15)
    send_at(ser, "AT+CFUN=1", 2)
    time.sleep(20)


def wait_for_registration(ser, retries=40):
    log_info("[4G] Waiting for network registration")

    for _ in range(retries):
        resp = send_at(ser, "AT+CEREG?", 1)

        if any(x in resp for x in ["0,1", "0,5", "1"]):
            log_info("[4G] Registered")
            return

        time.sleep(2)

    raise RuntimeError("LTE registration failed")


def attach_packet_service(ser, retries=10):
    for _ in range(retries):
        if "OK" in send_at(ser, "AT+CGATT=1", 2):
            return
        time.sleep(3)

    raise RuntimeError("Packet attach failed")


def set_apn(ser, apn):
    send_at(ser, f'AT+CGDCONT=1,"IPV4V6","{apn}"', 2)
    send_at(ser, "AT+CGACT=1,1", 2)  # ignore failure (Jio behavior)


def wait_for_ip(ser, retries=30):
    log_info("[4G] Waiting for IP")

    for _ in range(retries):
        if "+CGPADDR:" in send_at(ser, "AT+CGPADDR=1", 2):
            log_info("[4G] IP assigned")
            return
        time.sleep(3)

    raise RuntimeError("No IP assigned")


# ---------------- UTIL ----------------

def free_port(port):
    subprocess.run(["sudo", "fuser", "-k", port], check=False)
    time.sleep(1)


def wait_for_tty(port, timeout=40):
    log_info(f"[4G] Waiting for {port}")
    for _ in range(timeout):
        if os.path.exists(port):
            return
        time.sleep(1)
    raise RuntimeError("Serial port not found")


def send_at(ser, cmd, timeout=2):
    log_info(f"[AT] {cmd}")
    ser.write((cmd + "\r").encode())

    end = time.time() + timeout
    buf = ""

    while time.time() < end:
        buf += ser.read(ser.in_waiting or 1).decode(errors="ignore")
        if "OK" in buf or "ERROR" in buf:
            break

    if buf.strip():
        log_info(buf.strip())

    return buf


# ---------------- 4G ----------------
def setup_4g(apn):
    if not apn:
        log_warn("APN not configured — skipping attach")
        subprocess.run(["sudo","ip","link","set","usb0","down"],check=False)
        time.sleep(2)
        return
    modem = detect_modem()
    if not modem:
        # subprocess.run(["sudo","4g_power_on.sh"],check=False)
        log_info("No Modem Found")
        time.sleep(8)

    if MODEMS[modem]["mode"] == "ppp":
        setup_quectel_ppp(apn)
    else:
        setup_simcom_ecm(apn)


def network_watcher():
    log_info("[INFO] Network watcher started")

    try:
        config_data = load_config()
        network = config_data.get("network", {})

        setup_4g(network.get("sim4g", {}).get("apn", ""))

    except Exception as e:
        log_error(f"[NETWORK] Watcher error: {e}")


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
    log_info("[SYSTEM] Checking network execution conditions")

    config_data = load_config()
    if not config_data:
        log_error("No config found")
        return

    current_network = get_network_config(config_data)
    prev_network = load_json_file(PREV_NETWORK_FILE)

    first_boot = prev_network is None
    update_flag = is_update_requested()

    log_info(f"[DEBUG] First boot: {first_boot}")
    log_info(f"[DEBUG] Update flag: {update_flag}")

    if first_boot:
        log_info("[ACTION] First boot → running network setup")
        network_watcher()
        save_json_file(PREV_NETWORK_FILE, current_network)

    elif update_flag and is_network_changed(current_network, prev_network):
        log_info("[ACTION] Config changed & update requested → running setup")
        network_watcher()
        save_json_file(PREV_NETWORK_FILE, current_network)

        # reset flag after applying
        save_json_file(IS_UPDATED_FILE, {"is_updated": False})

    else:
        log_info("[SKIP] No changes detected → skipping network setup")


if __name__ == "__main__":
    main()


# this main_4g.py program is executing the at commands and it is fine

# now i want to execute the program of sending at commands when the following conditions match
# 1. There is an is_updated.json file present in same base folder which contains true or false. 
# 2. Store the previous network details taken from config
# 3. For the first time after boot, execute this program
# 4. From next time, store the previous network details taken from the config and execute this program only when new network details and previous network details are different and is_updated.json file is true