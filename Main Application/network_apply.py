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
BASE = Path("/home/gateway/GATEWAY-COMPLETE/Demo application/Main Application")

CONFIG_FILE  = BASE / "config.json"
UPDATED_FILE = BASE / "is_updated.json"
STATE_FILE   = BASE / "last_network_state.json"

SERIAL_PORT = "/dev/ttyUSB2"
BAUDRATE    = 115200
MODEM_USB_ID = "1e0e:9011"

# ---------------- LOGGING ----------------
LOG_BASE = Path("/home/gateway/logs")
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
    log.info(f"[4G] Waiting for {port}")
    for _ in range(timeout):
        if os.path.exists(port):
            return
        time.sleep(1)
    raise RuntimeError("Serial port did not appear")

def send_at(ser, cmd, wait=1):
    log.info(f"[AT] {cmd}")
    ser.write((cmd + "\r").encode())
    time.sleep(wait)
    resp = ser.read(ser.in_waiting or 256).decode(errors="ignore")
    if resp.strip():
        log.info(resp.strip())
    return resp

# ---------------- LTE REGISTRATION WAIT ----------------
def wait_for_lte(ser, present, timeout=5):
    log.info("[4G] Waiting for LTE registration")
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
            log.info("[4G] LTE registered")
            return

        time.sleep(2)

    raise RuntimeError("LTE registration timeout")

# ---------------- WIFI ----------------
def connect_wifi(ssid, password):
    if not ssid:
        log.info("[WiFi] No SSID configured")
        return

    # 1️⃣ Check current active Wi-Fi
    active = subprocess.run(
        ["nmcli", "-t", "-f", "ACTIVE,SSID", "dev", "wifi"],
        capture_output=True, text=True
    ).stdout

    for line in active.splitlines():
        if line.startswith("yes:") and line.split(".", 1)[1] == ssid:
            log.info(f"[WiFi] Already connected to {ssid}")
            return

    # 2️⃣ Check saved connections
    saved = subprocess.run(
        ["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"],
        capture_output=True, text=True
    ).stdout

    for line in saved.splitlines():
        name, ctype = line.split(".", 1)
        if name == ssid and ctype == "802-11-wireless":
            log.info(f"[WiFi] Bringing up saved connection {ssid}")
            subprocess.run(
                ["nmcli", "connection", "up", ssid],
                check=False
            )
            return

    # 3️⃣ New network → password required
    if not password:
        log.info(f"[WiFi] Password required for new network {ssid}")
        return

    log.info(f"[WiFi] Connecting to new network {ssid}")
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
        log.warning("SIM present but APN not configured — skipping attach")
        return
    log.info("[4G] Starting CFUN-based attach (Jio-safe)")
    if not modem_present():
        subprocess.run(["sudo","4g_power_on.sh"],check=False)
        time.sleep(5)

    free_port(SERIAL_PORT)
    wait_for_tty(SERIAL_PORT)

    with serial.Serial(SERIAL_PORT, BAUDRATE, timeout=2) as ser:
        send_at(ser, "AT")
        if not sim_present(ser):
            log.info("No Sim Inserted")
            subprocess.run(["sudo","ip","link","set","usb0","down"],check=False)
            time.sleep(2)
            return
        # 🔥 CFUN reset (THIS IS THE KEY)
        send_at(ser, "AT+CFUN=0", 2)
        time.sleep(15)

        send_at(ser, "AT+CFUN=1", 2)
        time.sleep(20)

        # Wait for LTE registration
        log.info("[4G] Waiting for network registration")

        send_at(ser, "AT+CREG", 1)
        send_at(ser, "AT+CGREG", 1)
        send_at(ser, "AT+CEREG", 1)
        send_at(ser, "AT+CNSMOD", 1)

        for _ in range(40):
            creg  = send_at(ser, "AT+CREG?", 1)
            cgreg = send_at(ser, "AT+CGREG?", 1)
            cereg = send_at(ser, "AT+CEREG?", 1)
            cns   = send_at(ser, "AT+CNSMOD?", 1)
            log.info(creg, cgreg, cereg, cns)

            if (
                ("CREG: 1" in creg or "CREG: 0,1" in creg) and
                ("CGREG: 1" in cgreg or "CGREG: 0,1" in cgreg) and
                ("CEREG: 1" in cereg or "CEREG: 0,1" in cereg)
            ):
                log.info("[4G] Registered on LTE")
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
        log.info("[4G] Waiting for IP from network")
        for _ in range(30):
            ipinfo = send_at(ser, "AT+CGPADDR=1", 2)
            if "+CGPADDR:" in ipinfo:
                log.info("[4G] IP assigned")
                break
            time.sleep(3)
        else:
            raise RuntimeError("No IP assigned by network")

    subprocess.run(["sudo", "dhclient", "usb0"], check=False)
    log.info("[4G] LTE READY (CFUN path)")

# ---------------- STATIC ETHERNET (DHCP + STATIC IP) ----------------
ETH_CONNECTION = "Wired connection 1"

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


def apply_static_ethernet_dhcp(static_cfg, prev_cfg):
    """
    Apply DHCP + secondary static IP + forced DNS
    ONLY if config changed.
    """

    # enabled = static_cfg.get("enabled", False)
    # ip = static_cfg.get("ip", "")

    # # 🔒 HARD GUARD: IP already present on eth0
    # if enabled and ip and eth0_has_ip(ip):
    #     log.info(f"[ETH] Static IP {ip} already present on eth0 — skipping nmcli")
    #     return static_cfg

    if static_cfg == prev_cfg:
        log.info("[ETH] No static ethernet change")
        return static_cfg
    log.info("[ETH] Applying DHCP + static IP on Ethernet")

    enabled = static_cfg.get("enabled", False)

    try:
        if not enabled:
            # Remove static IP and restore DHCP DNS
            subprocess.run([
                "sudo", "nmcli", "con", "mod", ETH_CONNECTION,
                "ipv4.addresses", "",
                "ipv4.ignore-auto-dns", "no",
                "ipv4.dns", ""
            ], check=False)

        else:
            ip = static_cfg.get("ip", "")
            subnet = static_cfg.get("subnet", "")

            if not ip or not subnet:
                log.warning("[ETH] Static IP enabled but IP/subnet missing")
                return prev_cfg

            # subnet mask -> CIDR
            cidr = sum(bin(int(x)).count("1") for x in subnet.split("."))
            addr = f"{ip}/{cidr}"

            dns1 = static_cfg.get("dns_primary", "")
            dns2 = static_cfg.get("dns_secondary", "")
            gateway = static_cfg.get("gateway","")
            dns = " ".join(d for d in [dns1, dns2] if d)

            # DHCP + secondary IP
            subprocess.run([
                "sudo", "nmcli", "con", "mod", ETH_CONNECTION,
                "ipv4.method", "auto",
                "ipv4.addresses", addr,
                # "ipv4.gateway",gateway
            ], check=True)

            # Force DNS
            subprocess.run([
                "sudo", "nmcli", "con", "mod", ETH_CONNECTION,
                "ipv4.ignore-auto-dns", "yes"
            ], check=True)

            if dns:
                subprocess.run([
                    "sudo", "nmcli", "con", "mod", ETH_CONNECTION,
                    "ipv4.dns", dns
                ], check=True)

        # Restart ONLY ethernet
        subprocess.run(["sudo", "nmcli", "con", "down", ETH_CONNECTION], check=False)
        subprocess.run(["sudo", "nmcli", "con", "up", ETH_CONNECTION], check=False)

        log.info("[ETH] Static ethernet applied successfully")
        return static_cfg

    except Exception as e:
        log.exception("[ETH] Failed to apply static ethernet")
        return prev_cfg


# ---------------- MAIN ----------------
def main():
    log.info("[INFO] Network watcher started")
    first_time = True
    while True:
        try:
            updated = load_json(UPDATED_FILE, False)

            if not updated and not first_time:
                log.info("[INFO] Skipping run as nothing is changed")
                time.sleep(2)
                continue

            config = load_json(CONFIG_FILE, {})
            network = config.get("network", {})
            wifi = network.get("wifi", {})
            sim  = network.get("sim4g", {})
            static_ip =  network.get("static", {})

            state = {"wifi": wifi, "sim4g": sim}
            current_hash = hash_dict(state)
            last_hash = load_json(STATE_FILE, "")

            if first_time:
                log.info("Running for the first time")

                connect_wifi(
                    wifi.get("ssid", ""),
                    wifi.get("password", "")
                )

                setup_4g(sim.get("apn", ""))

                apply_static_ethernet_dhcp(
                    network.get("static", {}),
                    {}
                )

                first_time = False
                write_json(STATE_FILE, state)
                continue

            

            if current_hash == last_hash:
                log.info("[INFO] No network changes detected")
                time.sleep(2)
                continue

            log.info("[INFO] Applying network changes")

            connect_wifi(
                wifi.get("ssid", ""),
                wifi.get("password", "")
            )

            setup_4g(sim.get("apn", ""))

            prev_state = load_json(STATE_FILE, {})
            prev_static = prev_state.get("static", {})
            new_static = network.get("static", {})

            applied_static = apply_static_ethernet_dhcp(
                new_static,
                prev_static
            )

            state["static"] = applied_static
            write_json(STATE_FILE, state)

            log.info("[SUCCESS] Network applied")


        except Exception as e:
            log.exception(f"[ERROR] {e}")

        time.sleep(2)



if __name__ == "__main__":
    main()
