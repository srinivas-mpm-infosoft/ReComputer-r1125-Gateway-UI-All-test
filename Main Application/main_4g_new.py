#!/usr/bin/env python3

import os
import time
import json
import subprocess
from datetime import datetime
from pathlib import Path
import serial
import logging
from logging.handlers import TimedRotatingFileHandler

# ================= CONFIG =================
BASE_DIR = "/home/pi/Downloads/Gateway-UI/Main Application"
CONFIG_FILE = f"{BASE_DIR}/config.json"
BAUDRATE = 115200

# ================= LOGGING =================
def setup_logging():
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = Path(f"/home/pi/logs/{today}")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))

    file_handler = TimedRotatingFileHandler(
        log_dir / "gateway.log", when="midnight", backupCount=7
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger

logger = setup_logging()

# ================= FILE =================
def load_config():
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return None

# ================= SERIAL =================

def kill_known_modem_users():
    logger.info("[FORCE] Killing known modem users")

    cmds = [
        ["sudo", "systemctl", "stop", "ModemManager"],
        ["sudo", "pkill", "-f", "main_4g_old"],
        ["sudo", "pkill", "-f", "minicom"]
    ]

    for cmd in cmds:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def force_free_port(port):
    logger.info(f"[FORCE] Releasing {port}")

    try:
        subprocess.run(
            ["sudo", "fuser", "-k", port],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(1)
    except:
        pass



def find_modem_port():
    candidate_ports = ["/dev/ttyUSB2", "/dev/ttyUSB3"]

    kill_known_modem_users()

    logger.info(f"[SCAN] Trying only: {candidate_ports}")

    for port in candidate_ports:
        if not os.path.exists(port):
            logger.warning(f"[SKIP] {port} not present")
            continue

        # 🔥 FORCE RELEASE PORT
        force_free_port(port)

        logger.info(f"[SCAN] Testing {port}")

        try:
            with serial.Serial(port, BAUDRATE, timeout=1) as ser:
                ser.reset_input_buffer()
                ser.reset_output_buffer()

                for attempt in range(5):
                    ser.write(b"AT\r")
                    logger.info(f"[TRY {attempt+1}] Sent AT on {port}")

                    start = time.time()
                    resp = ""

                    while time.time() - start < 2:
                        if ser.in_waiting:
                            chunk = ser.read(ser.in_waiting).decode(errors="ignore")
                            resp += chunk

                            if "OK" in resp:
                                logger.info(f"[MODEM] Found AT port: {port}")
                                return port

                    logger.info(f"[RESP] {port}: {resp.strip()}")

        except Exception as e:
            logger.error(f"[FAILED OPEN] {port}: {e}")
            continue

    return None

# ================= AT =================
def send_at(ser, cmd, timeout=3):
    logger.info(f"[AT] {cmd}")
    ser.reset_input_buffer()
    ser.write((cmd + "\r").encode())

    end = time.time() + timeout
    resp = ""

    while time.time() < end:
        if ser.in_waiting:
            chunk = ser.read(ser.in_waiting).decode(errors="ignore")
            resp += chunk

            if "OK" in resp or "ERROR" in resp:
                break

    logger.info(resp.strip())
    return resp

# ================= MODEM =================
def check_sim(ser):
    resp = send_at(ser, "AT+CPIN?", 2)
    if "READY" in resp:
        logger.info("[SIM] READY")
        return True
    else:
        logger.error("[SIM] NOT DETECTED")
        return False

def wait_for_network(ser):
    logger.info("[NET] Waiting for registration")

    for _ in range(30):
        resp = send_at(ser, "AT+CEREG?", 2)

        if any(x in resp for x in ["0,1", "0,5"]):
            logger.info("[NET] REGISTERED")
            return True

        time.sleep(2)

    return False

# ================= NETWORK =================
def bring_usb_up():
    logger.info("[NET] Bringing up usb0")

    subprocess.run(["sudo", "ip", "link", "set", "usb0", "up"], check=False)
    time.sleep(1)

    subprocess.run(["sudo", "dhclient", "usb0"], check=False)
    time.sleep(2)

    subprocess.run([
        "sudo", "ip", "route", "replace",
        "default", "dev", "usb0", "metric", "50"
    ], check=False)

    logger.info("[NET] usb0 READY")

# ================= MAIN =================
def run_4g(apn):
    port = find_modem_port()

    if not port:
        logger.error("No modem AT port found")
        return

    with serial.Serial(port, BAUDRATE, timeout=2) as ser:
        send_at(ser, "AT")

        if not check_sim(ser):
            subprocess.run(["sudo", "ip", "link", "set", "usb0", "down"], check=False)
            return

        if not wait_for_network(ser):
            logger.error("Network registration failed")
            return

    bring_usb_up()
    logger.info("4G READY")

# ================= ENTRY =================
def main():
    config = load_config()
    if not config:
        return

    apn = config.get("network", {}).get("sim4g", {}).get("apn", "")

    logger.info("Running 4G setup...")
    run_4g(apn)

if __name__ == "__main__":
    main()