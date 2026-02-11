
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
try:
    import smbus
except ImportError:
    import smbus2 as smbus

import psutil
import serial
import requests
import mysql.connector
import minimalmodbus
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import multiprocessing

# === Load full configuration JSON ===

CONFIG_FILE_PATH = "config.json"
config = None

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
        print(f"[ERROR] Failed to load config: {e}")
        return None

# === GLOBAL VARIABLE DECLARATIONS (at module level) ===
# Global configuration variables (shared across functions)

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
MODBUS_TCP_ENABLED = None

# Digital Input Configuration (global)
DIGITAL_SAVE_LOG = None
DIGITAL_MODE = None
DIGITAL_CHANNELS = None
DIGITAL_COUNTS = None
DIGITAL_TIMES = None

# Analog Configuration (global)
ANALOG_POLLING_INTERVAL = None
ANALOG_POLLING_UNIT = None
ANALOG_SAVE_LOG = None
ANALOG_CHANNELS = None
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


# Modbus TCP Configuration (global)
MODBUS_TCP_MAC = None
MODBUS_TCP_IP = None
MODBUS_TCP_SUBNET = None
MODBUS_TCP_GATEWAY = None
MODBUS_TCP_INTERVAL = None
MODBUS_TCP_LOG = None
MODBUS_TCP_TABLE = None

# Ethernet Configuration (global)
RECV_IP = None
RECV_PORT = None
SEND_TARGETS = None

# RS485 Configuration (global)
RS485_PORT = None
RS485_BAUD_RATE = None
MODEM_PORT="/dev/ttyUSB2"

# Kafka Configuration (global)
KAFKA_BROKERS = None
KAFKA_TOPIC = None
KAFKA_CERT_FILES = None
kafka_topic =  None

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
MYSQL_CONFIG = None

# ADC I2C address (global)
ADC_I2C_ADDRESS = None

# Global data storage
data_map = None
analog_readings = None
digital_readings = None
modbus_readings = None

# Global I2C bus (initialize once)
bus = None

# ==================================================
# UTILITY FUNCTIONS
# ==================================================

"""
What: Create and configure minimalmodbus.Instrument objects for all enabled Modbus R1/R2/R3 entries.
Calls: minimalmodbus.Instrument()
Required by: main_data_collection_loop() to get instruments; read_modbus_registers() consumes the mapping created here.
Notes: Uses RS485_PORT, MODBUS_BAUD_RATE, MODBUS_DATA_BIT, MODBUS_PARITY, MODBUS_STOP_BIT, and MODBUS_Rx_CFG.
        Keys returned look like 'R1_slave_{id}' / 'R2_slave_{id}' / 'R3_slave_{id}'.
Side effects: Opens serial configuration on instruments.
"""
def setup_modbus_instruments():
    instruments = {}
    print("HELLO FROM INSTRUMENTS")

    # ---- Helper: create instrument safely ----
    def create_instrument(prefix, slave_id):
        try:
            inst = minimalmodbus.Instrument(RS485_PORT, slave_id)
            inst.serial.baudrate = MODBUS_BAUD_RATE
            inst.serial.bytesize = MODBUS_DATA_BIT
            inst.serial.parity   = MODBUS_PARITY
            inst.serial.stopbits = MODBUS_STOP_BIT
            inst.serial.timeout  = 1
            instruments[f"{prefix}_slave_{slave_id}"] = inst
        except Exception as e:
            print(f"Failed {prefix} slave {slave_id}: {e}")

    # ---- R1 ----
    if MODBUS_R1_CFG.get("enabled", False):
        slaves = MODBUS_R1_CFG.get("slaves", [])
        table  = MODBUS_R1_CFG.get("table", [])
        for idx, slave_info in enumerate(slaves):
            slave_id = slave_info.get("id")
            if idx < len(table) and table[idx].get("enabled", False):
                create_instrument("R1", slave_id)

    # ---- R2 ----
    if MODBUS_R2_CFG.get("enabled", False):
        slaves = MODBUS_R2_CFG.get("slaves", [])
        table  = MODBUS_R2_CFG.get("table", [])
        for idx, slave_info in enumerate(slaves):
            slave_id = slave_info.get("id")
            if idx < len(table) and table[idx].get("enabled", False):
                create_instrument("R2", slave_id)

    # ---- R3 ----
    if MODBUS_R3_CFG.get("enabled", False):
        for entry in MODBUS_R3_CFG.get("table", []):
            if entry.get("enabled", False):
                slave_id = entry.get("slave")
                create_instrument("R3", slave_id)
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
    global config, CONNECTION_TYPE, COM_PORT, FIRMWARE, SERIAL_NUMBER, COM_BAUD_RATE
    global MODBUS_ENABLED, ANALOG_ENABLED, DIGITAL_INPUT_ENABLED, MODBUS_TCP_ENABLED
    global DIGITAL_SAVE_LOG, DIGITAL_MODE, DIGITAL_CHANNELS, DIGITAL_COUNTS, DIGITAL_TIMES
    global ANALOG_POLLING_INTERVAL, ANALOG_POLLING_UNIT, ANALOG_SAVE_LOG, ANALOG_CHANNELS
    global EXTENSION_ADC, SCALING_CONFIG, MODBUS_BAUD_RATE, MODBUS_DATA_BIT
    global MODBUS_PARITY, MODBUS_STOP_BIT, MODBUS_R1_CFG, MODBUS_R2_CFG, MODBUS_R3_CFG
    global POLLING_INTERVALS, COMMUNICATION_MEDIA, SEND_FORMAT, FTP_CONFIG
    global WIRELESS_POLLING_INTERVAL, WIRELESS_POLLING_UNIT, APN, JSON_ENDPOINT
    global MODBUS_TCP_MAC, MODBUS_TCP_IP, MODBUS_TCP_SUBNET, MODBUS_TCP_GATEWAY
    global MODBUS_TCP_INTERVAL, MODBUS_TCP_LOG, MODBUS_TCP_TABLE, RECV_IP, RECV_PORT
    global SEND_TARGETS, RS485_PORT, RS485_BAUD_RATE, KAFKA_BROKERS, KAFKA_TOPIC
    global KAFKA_CERT_FILES, DIGITAL_IO_ALARMS, ANALOG_ALARMS, OFFLINE_ENABLED, MODBUS_ALARMS
    global OFFLINE_MODE, OFFLINE_FTP, OFFLINE_SCHEDULE, LTE_INTERFACE, MYSQL_CONFIG
    global CSV_FILENAME, LAST_SENT_FILE
    global ADC_I2C_ADDRESS, data_map
    global analog_readings, digital_readings, modbus_readings, bus, MODBUS_PARITY_MAP
    global UPDATED
    global WIFI_SSID, WIFI_PASSWORD, WIFI_IP_MODE, WIFI_IP, WIFI_SUBNET, WIFI_GATEWAY, WIFI_DNS1, WIFI_DNS2

    print("[INFO] 🔄 Updating global configuration variables...")

    # Load fresh config
    config = load_config()
    if config is None:
        print("[ERROR] Failed to load configuration")
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
    MODBUS_TCP_ENABLED = settings.get("modbusTcp", False)

    # Digital Input Configuration
    digital_input_cfg = io_settings.get("digitalInput", {})
    DIGITAL_SAVE_LOG = digital_input_cfg.get("saveLog", False)
    DIGITAL_MODE = digital_input_cfg.get("mode", "time")
    DIGITAL_CHANNELS = digital_input_cfg.get("channels", [False, True, True, True])
    DIGITAL_COUNTS = digital_input_cfg.get("counts", [1, 0, 0, 0])
    DIGITAL_TIMES = digital_input_cfg.get("times", [0, 5, 0, 0])

    # Analog Configuration
    analog_cfg = io_settings.get("analog", {})
    ANALOG_POLLING_INTERVAL = analog_cfg.get("pollingInterval", 30)
    ANALOG_POLLING_UNIT = analog_cfg.get("pollingIntervalUnit", "Sec")
    ANALOG_SAVE_LOG = analog_cfg.get("saveLog", True)
    ANALOG_CHANNELS = analog_cfg.get("channels", [])
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
    MODBUS_PARITY_MAP = {"None": "N", "Even": "E", "Odd": "O", "Mark": "M", "Space": "S"}
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
    JSON_ENDPOINT = wireless_cfg.get("jsonEndpoint", "https://github.com/srinivas2200030392")

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

    # Modbus TCP Configuration
    modbus_tcp_cfg = config.get("modbusTcp", {})
    MODBUS_TCP_MAC = modbus_tcp_cfg.get("mac", "")
    MODBUS_TCP_IP = modbus_tcp_cfg.get("ip", "")
    MODBUS_TCP_SUBNET = modbus_tcp_cfg.get("subnet", "")
    MODBUS_TCP_GATEWAY = modbus_tcp_cfg.get("gateway", "")
    MODBUS_TCP_INTERVAL = modbus_tcp_cfg.get("modbusTcpInterval", 3)
    MODBUS_TCP_LOG = modbus_tcp_cfg.get("log", True)
    MODBUS_TCP_TABLE = modbus_tcp_cfg.get("table", [])

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
    LAST_SENT_FILE = "/home/mpminfosoft/last_sent.txt"
    LTE_INTERFACE = config.get("lteInterface", "usb0")
    MYSQL_CONFIG = config.get("mysqlConfig", {
        "user": "mpmgateway",
        "password": "mpmgateway",
        "host": "localhost",
        "database": "voltage_reading",
    })
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
            print(f"[ERROR] Failed to initialize I2C bus: {e}")

    print("[INFO] ✅ Global configuration variables updated successfully")
    print(f"[INFO] Key settings - Analog: {ANALOG_ENABLED}, Digital: {DIGITAL_INPUT_ENABLED}")
    print(f"[INFO] Key settings - Modbus: {MODBUS_ENABLED}, ModbusTCP: {MODBUS_TCP_ENABLED}")
    
    UPDATED = True
    connect_wifi()
    return True

"""
What: Check is_updated.json for a text boolean ('true'/'false') within first two lines to trigger config reload.
Calls: Path.exists(), built-in file I/O
Required by: config_monitor_loop()
Notes: Returns True/False/None. Uses first two lines but substitutes second into first if present.
Side effects: None.
"""
def read_update_flag(file_path="is_updated.json"):
    try:
        update_file = Path(file_path)
        if not update_file.exists():
            return None

        with open(update_file, 'r') as f:
            lines = []
            for _ in range(1 ):  # read first 2 lines only
                line = f.readline()
                if not line:
                    break
                lines.append(line.strip().lower())

        print("First two lines:", lines)
        lines[0] = lines[1] if len(lines)>1 else lines[0]
        # interpret the first line only as boolean flag
        if lines and lines[0].startswith("true"):
            return True
        elif lines and lines[0].startswith("false"):
            return False
        else:
            return None

    except Exception as e:
        print(f"[ERROR] Failed to read update flag: {e}")
        return None

"""
What: Write 'false' to is_updated.json to acknowledge config reload completion.
Calls: built-in file I/O
Required by: config_monitor_loop() after successful update_global_config().
Notes: Prevents repeated reloads.
Side effects: Overwrites file content.
"""
def clear_update_flag(file_path="is_updated.json"):
    try:
        with open(file_path, 'w') as f:
            f.write("false")
        print(f"[INFO] Cleared update flag in {file_path}")
    except Exception as e:
        print(f"[ERROR] Failed to clear update flag: {e}")

"""
What: Background loop that watches is_updated.json and reloads global config when it becomes true.
Calls: read_update_flag(), update_global_config(), clear_update_flag()
Required by: start_config_monitor() (spawns as thread)
Notes: Sleeps 5s between checks; tracks last_flag_state to avoid duplicate logs.
Side effects: Mutates globals via update_global_config(); writes is_updated.json via clear_update_flag().
"""
def config_monitor_loop():
    print("[INFO] 📊 Starting configuration monitor...")
    last_flag_state = None
    
    while True:
        try:
            # Read update flag from is_updated.json
            update_flag = read_update_flag("is_updated.json")
            
            if update_flag is True and update_flag != last_flag_state:
                print("[INFO] 🚨 Configuration update flag detected: TRUE")
                
                # Update all global configuration variables
                if update_global_config():
                    # Clear the update flag after successful reload
                    clear_update_flag("is_updated.json")
                    print("[INFO] ✅ Configuration update completed")
                else:
                    print("[ERROR] ❌ Configuration update failed")
                    
            elif update_flag is False and update_flag != last_flag_state:
                print("[INFO] Configuration update flag: FALSE (no update needed)")
                
            elif update_flag is None:
                print("[WARN] Could not read update flag from is_updated.json")
            
            last_flag_state = update_flag
            
            # Wait 5 seconds before next check
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("[INFO] Configuration monitor stopped by user")
            break
        except Exception as e:
            print(f"[ERROR] Configuration monitor error: {e}")
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
            print("[CRITICAL] Initial configuration load failed. Exiting.")
            exit(1)
    
    # Start monitoring thread
    config_thread = threading.Thread(target=config_monitor_loop, daemon=True)
    config_thread.start()
    print("[INFO] 🚀 Configuration monitor thread started")
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
def read_analog_channel(channel_idx):
    try:
        if channel_idx < len(ANALOG_CHANNELS):
            channel_cfg = ANALOG_CHANNELS[channel_idx]
            if channel_cfg.get("enabled", False):
                # Configure ADC for the specific channel
                config_value = 0xC000 | (
                    channel_idx << 12
                )  # Basic config with channel selection
                bus.write_word_data(ADC_I2C_ADDRESS, 0x01, config_value)
                time.sleep(0.1)

                raw = bus.read_word_data(ADC_I2C_ADDRESS, 0x00)
                raw = ((raw << 8) & 0xFF00) | (raw >> 8)  # Byte swap
                if raw >= 0x8000:
                    raw -= 0x10000

                voltage = round(raw * 2.048 / 32768.0, 2)

                # Apply scaling if enabled
                if SCALING_CONFIG.get("enabled", False):
                    scaling_data = SCALING_CONFIG.get("data", [])
                    if channel_idx < len(scaling_data):
                        scale_cfg = scaling_data[channel_idx]
                        min_val = scale_cfg.get("min", 0)
                        max_val = scale_cfg.get("max", 10)
                        # Simple linear scaling
                        voltage = min_val + (voltage / 10.0) * (max_val - min_val)
                        voltage = round(voltage, 2)

                print(f"[INFO] Channel {channel_idx} voltage: {voltage:.2f} V")
                return voltage
        return None
    except Exception as e:
        print(f"[ERROR] ADC channel {channel_idx} read failed: {e}")
        # Simulated fallback reading to keep pipeline alive
        return round(random.uniform(1.0, 5.0), 2)

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
            voltage = read_analog_channel(idx)
            if voltage is not None:
                readings[f"analog_ch_{idx}"] = voltage

    # Read extension ADC if enabled
    if EXTENSION_ADC.get("enabled", False):
        ext_data = EXTENSION_ADC.get("data", [])
        for idx, _ext_channel in enumerate(ext_data):
            # Simulate reading extension ADC
            voltage = round(random.uniform(2.0, 8.0), 2)
            readings[f"ext_adc_ch_{idx}"] = voltage
            print(f"[INFO] Extension ADC Channel {idx}: {voltage:.2f} V")

    return readings

# ==================================================
# DIGITAL I/O FUNCTIONS
# ==================================================

"""
What: Sample enabled digital inputs and return states, also check digital alarms per channel.
Calls: check_digital_alarm()
Required by: main_data_collection_loop()
Notes: Currently simulates inputs randomly (0/1). Replace with GPIO reads as needed.
Side effects: May emit alarm printouts via check_digital_alarm().
"""
def read_digital_inputs():
    readings = {}
    for idx, enabled in enumerate(DIGITAL_CHANNELS):
        if enabled:
            # Simulate digital input reading
            state = random.choice([0, 1])
            readings[f"digital_ch_{idx}"] = state
            print(f"[INFO] Digital Channel {idx}: {state}")

            # Check alarm conditions
            check_digital_alarm(idx, state)

    return readings

"""
What: Evaluate a digital input state against configured alarm thresholds and report alarms.
Calls: None
Required by: read_digital_inputs()
Notes: Uses DIGITAL_IO_ALARMS config with up to 5 levels; prints [ALARM] lines when triggered.
Side effects: Print to console; could be extended to send notifications.
"""
def check_digital_alarm(channel, state):
    channel_key = f"Channel {channel + 1}"
    if channel_key in DIGITAL_IO_ALARMS:
        alarm_cfg = DIGITAL_IO_ALARMS[channel_key]
        if alarm_cfg.get("alert_enable") == "Enable":
            # Check alarm levels
            for level in range(5):
                level_key = f"level_enable_{level}"
                if alarm_cfg.get(level_key) == "Enable":
                    threshold = alarm_cfg.get(f"level_threshold_{level}", "0")
                    contact = alarm_cfg.get(f"level_contact_{level}", "")
                    message = alarm_cfg.get(f"level_message_{level}", "")

                    if state >= int(threshold):
                        print(
                            f"[ALARM] Digital Channel {channel}: {message} - Contact: {contact}"
                        )

# ==================================================
# WIFI
# ==================================================

def connect_wifi():
    global WIFI_SSID, WIFI_PASSWORD, WIFI_IP_MODE
    global WIFI_IP, WIFI_SUBNET, WIFI_GATEWAY, WIFI_DNS1, WIFI_DNS2

    if not WIFI_SSID:
        print("❌ No WiFi SSID configured.")
        return False

    try:
        # Get current active SSID
        result = subprocess.run(
            ["nmcli", "-t", "-f", "active,ssid", "dev", "wifi"],
            capture_output=True, text=True
        )
        current_ssid = None
        for line in result.stdout.strip().splitlines():
            if line.startswith("yes:"):
                current_ssid = line.split(":", 1)[1]
                break

        if current_ssid == WIFI_SSID:
            print(f"✅ Already connected to {WIFI_SSID}, skipping reconfigure.")
            return True

        # Not connected or different network → reconfigure
        print(f"🔄 Switching WiFi from {current_ssid} → {WIFI_SSID}")

        # Delete old connection for target SSID
        subprocess.run(["nmcli", "connection", "delete", WIFI_SSID],
                       check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Connect with password
        cmd = ["nmcli", "device", "wifi", "connect", WIFI_SSID, "password", WIFI_PASSWORD]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to connect WiFi: {result.stderr.strip()}")
            return False

        # Configure static IP if required
        if WIFI_IP_MODE.upper() == "STATIC":
            ip_config = f"{WIFI_IP}/{WIFI_SUBNET} {WIFI_GATEWAY}"
            subprocess.run([
                "nmcli", "connection", "modify", WIFI_SSID,
                "ipv4.addresses", ip_config,
                "ipv4.gateway", WIFI_GATEWAY,
                "ipv4.dns", ",".join(filter(None, [WIFI_DNS1, WIFI_DNS2])),
                "ipv4.method", "manual"
            ], check=True)
            subprocess.run(["nmcli", "connection", "up", WIFI_SSID], check=True)
        else:
            subprocess.run([
                "nmcli", "connection", "modify", WIFI_SSID,
                "ipv4.method", "auto"
            ], check=True)

        print(f"✅ Connected to WiFi {WIFI_SSID} ({WIFI_IP_MODE})")
        return True

    except Exception as e:
        print(f"❌ WiFi connection failed: {e}")
        return False

# ==================================================
# MODBUS FUNCTIONS
# ==================================================

def init_modbus_db():
    conn = mysql.connector.connect(
        host="localhost",
        user="mpmgateway",
        password="mpmgateway",
        database="energymeter_readings"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS modbus_readings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn

def ensure_columns(conn, readings):
    cur = conn.cursor()
    cur.execute("SHOW COLUMNS FROM modbus_readings")
    existing_cols = {row[0] for row in cur.fetchall()}

    # Exclude default ones
    existing_cols.discard("id")
    existing_cols.discard("ts")

    # Add missing columns
    for col in sorted(readings.keys()):
        if col not in existing_cols:
            print(f"[DB] Adding new column: {col}")
            cur.execute(f"ALTER TABLE modbus_readings ADD COLUMN `{col}` FLOAT DEFAULT -1")
    conn.commit()

def insert_readings(conn, readings):
    cur = conn.cursor()

    # Ensure schema up-to-date
    ensure_columns(conn, readings)

    # Fetch full list of columns (to insert -1 for missing ones)
    cur.execute("SHOW COLUMNS FROM modbus_readings")
    all_cols = [row[0] for row in cur.fetchall() if row[0] not in ("id", "ts")]

    row_values = []
    for col in sorted(all_cols):
        row_values.append(readings.get(col, -1))  # -1 for missing

    placeholders = ", ".join(["%s"] * len(row_values))
    colnames = ", ".join(f"`{c}`" for c in sorted(all_cols))

    sql = f"INSERT INTO modbus_readings ({colnames}) VALUES ({placeholders})"
    cur.execute(sql, row_values)
    conn.commit()


def check_modbus_alarms(readings):
    """
    Checks all MODBUS alarms (alerts and per-channel configs) and calls send_sms if triggered.
    """
    # --- ALERTS: list-of-dict conditions (generic) ---
    for alert in MODBUS_ALARMS.get("alerts", []):
        print("Hello1")
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
                            print("Hello")
                            send_sms(
                                contact,
                                text=f"{message}: value={value} ({cond} {threshold})",
                            )
                except Exception as e:
                    print(f"[WARN] SMS alarm check error: {e}")

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
                                    contact, text=f"{message}: value={v} (>= {threshold})"
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
def regs_to_float(regs, count, byte_order="big", word_order="little"):
    floats = []
    for i in range(0,count,2):
        b1 = regs[i].to_bytes(2, byte_order)
        b2 = regs[i+1].to_bytes(2, byte_order)
        raw_bytes = b1 + b2 if word_order == "big" else b2 + b1
        floats.append(struct.unpack(">f", raw_bytes)[0])
    leftover = regs[i:] if i < len(regs) else []
    return floats, leftover

"""
What: Iterate configured Modbus tables (R1/R2) and fetch registers for each enabled slave.
Calls: minimalmodbus.Instrument.read_registers()/read_input_registers(), regs_to_float()
Required by: main_data_collection_loop()
Notes: Uses MODBUS_R1_CFG/MODBUS_R2_CFG tables and per-reg conversion rules (Integer/Float/Hex).
        Adds entries in the returned dict with keys like 'R1_S{slave}_{start}'.
Side effects: Console logs; serial comms to Modbus slaves; catches and logs errors per register.
"""
def read_modbus_registers(instruments):
    readings = {}

    # ---- R1 ----
    if MODBUS_R1_CFG.get("enabled", False):
        table  = MODBUS_R1_CFG.get("table", [])
        slaves = MODBUS_R1_CFG.get("slaves", [])

        for idx, slave_info in enumerate(slaves):
            slave_id = slave_info.get("id", 1)
            instrument_key = f"R1_slave_{slave_id}"

            if idx < len(table) and table[idx].get("enabled", False):
                reg_cfg = table[idx]
                if instrument_key in instruments:
                    instrument = instruments[instrument_key]
                    try:
                        start = reg_cfg.get("start", 0)
                        length = reg_cfg.get("length", 1)
                        reg_type = reg_cfg.get("type", "Holding Register")
                        conversion = reg_cfg.get("conversion", "Integer")

                        # Read registers
                        regs = instrument.read_registers(start, length)

                        # Decode value
                        if conversion == "Float: Big Endian" and length >= 2:
                            value, leftover = regs_to_float(regs, length, byte_order="big", word_order="little")
                            if leftover:
                                print(f"[WARN] Leftover registers for R1 slave {slave_id} reg {start}: {leftover}")
                        elif conversion == "Integer":
                            value = regs[0] if length == 1 else regs
                        else:  # Raw Hex
                            value = hex(regs[0]) if length == 1 else [hex(r) for r in regs]

                        readings[f"R1_S{slave_id}_{start}"] = value
                        print(f"[INFO] Modbus R1 Slave {slave_id} Reg {start}: {value}")

                    except Exception as e:
                        print(f"[ERROR] Modbus R1 read failed for slave {slave_id} reg {start}: {e}")

    # ---- R2 ----
    if MODBUS_R2_CFG.get("enabled", False):
        table  = MODBUS_R2_CFG.get("table", [])
        slaves = MODBUS_R2_CFG.get("slaves", [])

        for idx, slave_info in enumerate(slaves):
            slave_id = slave_info.get("id", 1)
            instrument_key = f"R2_slave_{slave_id}"

            if idx < len(table) and table[idx].get("enabled", False):
                reg_cfg = table[idx]
                if instrument_key in instruments:
                    instrument = instruments[instrument_key]
                    try:
                        start = reg_cfg.get("start", 0)
                        length = reg_cfg.get("length", 1)
                        reg_type = reg_cfg.get("type", "Holding Register")
                        conversion = reg_cfg.get("conversion", "Integer")

                        # Read registers
                        regs = instrument.read_registers(start, length)

                        # Decode value
                        if conversion == "Float: Big Endian" and length >= 2:
                            value, leftover = regs_to_float(regs, length, byte_order="big", word_order="little")
                            if leftover:
                                print(f"[WARN] Leftover registers for R2 slave {slave_id} reg {start}: {leftover}")
                        elif conversion == "Integer":
                            value = regs[0] if length == 1 else regs
                        else:  # Raw Hex
                            value = hex(regs[0]) if length == 1 else [hex(r) for r in regs]

                        readings[f"R2_S{slave_id}_{start}"] = value
                        print(f"[INFO] Modbus R2 Slave {slave_id} Reg {start}: {value}")

                    except Exception as e:
                        print(f"[ERROR] Modbus R2 read failed for slave {slave_id} reg {start}: {e}")

    # ---- R3 ----
    if MODBUS_R3_CFG.get("enabled", False):
        table = MODBUS_R3_CFG.get("table", [])

        for reg_cfg in table:
            if reg_cfg.get("enabled", False):
                slave_id = reg_cfg.get("slave", 1)
                instrument_key = f"R3_slave_{slave_id}"

                if instrument_key in instruments:
                    instrument = instruments[instrument_key]
                    try:
                        start = reg_cfg.get("start", 0)
                        length = reg_cfg.get("length", 1)
                        reg_type = reg_cfg.get("type", "Holding Register")
                        conversion = reg_cfg.get("conversion", "Integer")

                        regs = instrument.read_registers(start, length)

                        if conversion == "Float: Big Endian" and length >= 2:
                            value, leftover = regs_to_float(regs, length, byte_order="big", word_order="little")
                            if leftover:
                                print(f"[WARN] Leftover registers for R3 slave {slave_id} reg {start}: {leftover}")
                        elif conversion == "Integer":
                            value = regs[0] if length == 1 else regs
                        else:  # Raw Hex
                            value = hex(regs[0]) if length == 1 else [hex(r) for r in regs]

                        readings[f"R3_S{slave_id}_{start}"] = value
                        print(f"[INFO] Modbus R3 Slave {slave_id} Reg {start}: {value}")

                    except Exception as e:
                        print(f"[ERROR] Modbus R3 read failed for slave {slave_id} reg {start}: {e}")
    check_modbus_alarms(readings)

    # Insert into DB
    conn = init_modbus_db()
    insert_readings(conn, readings)
    conn.close()

    return readings

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
            CREATE TABLE IF NOT EXISTS sensor_readings (
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
                    "INSERT INTO sensor_readings (timestamp, sensor_type, sensor_id, value) VALUES (%s, %s, %s, %s)",
                    (timestamp, "mixed", sensor_id, float(value)),
                )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"[INFO] Inserted {len(readings)} readings into MySQL")

    except Exception as e:
        print(f"[ERROR] MySQL insert failed: {e}")

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
            print(f"[INFO] SD card found at {part.mountpoint}")
            return os.path.join(part.mountpoint, CSV_FILENAME)
    print("[WARN] SD card not inserted, using internal path")
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

        print(f"[INFO] Logged {len(readings_data)} entries to CSV")

    except Exception as e:
        print(f"[ERROR] Failed to write CSV: {e}")

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
        #subprocess.run(["gpioset", "0", "6=0"], check=True)
        print("[INFO] 4G module powered ON (GPIO6=0)")
        time.sleep(5)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to power ON 4G module: {e}")

"""
What: Request DHCP lease on LTE_INTERFACE using dhclient.
Calls: subprocess.run()
Required by: main() when COMMUNICATION_MEDIA == "4G/LTE"
Notes: Assumes modem provides a network interface and DHCP server; may need APN/ppp for some modems.
Side effects: Network config; interface state changes.
"""
def connect_4g():
    try:
        subprocess.run(["sudo", "dhclient", "-v", "-e", "IF_METRIC=600", LTE_INTERFACE], check=True)
        #subprocess.run(["dhclient", "-v", "-e", "IF_METRIC=600", LTE_INTERFACE], check=True)
        print(f"[INFO] Connected via 4G LTE on {LTE_INTERFACE}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 4G LTE network setup failed: {e}")

"""
What: POST a file (CSV) to JSON_ENDPOINT over HTTP.
Calls: requests.post()
Required by: main_data_collection_loop() when sending buffered data.
Notes: Expects JSON_ENDPOINT to accept multipart/form-data; adjust to application/json if needed.
Side effects: Network egress; prints error/status.
"""
def send_data_via_4g(filepath):
    try:
        with open(filepath, "rb") as f:
            files = {"file": (os.path.basename(filepath), f)}
            response = requests.post(JSON_ENDPOINT, files=files, timeout=10)
            if response.status_code in (200, 201):
                print("[INFO] Data uploaded via 4G successfully")
            else:
                print(
                    f"[ERROR] Upload failed: HTTP {response.status_code} - {response.text}"
                )
    except Exception as e:
        print(f"[ERROR] 4G upload failed: {e}")

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
        print(f"[INFO] Incoming TCP connection from {addr}")
        try:
            with open("received_data.txt", "ab") as f:
                while True:
                    data = client_socket.recv(4096)
                    if not data:
                        break
                    f.write(data)
                    try:
                        decoded = data.decode(errors="ignore")
                        #kafka_publish("tcp", {"ts": datetime.now(timezone.utc).isoformat(), "data": decoded})
                    except Exception:
                        pass
            print(f"[INFO] Data received and saved from {addr}")
        except Exception as e:
            print(f"[ERROR] TCP server error: {e}")
        finally:
            client_socket.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((RECV_IP, RECV_PORT))
    server.listen(5)
    print(f"[INFO] TCP Server listening on {RECV_IP}:{RECV_PORT}")

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
            print(f"Killing process {pid} holding {port}")
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
    print("CREG response:", resp.strip())
    return "+CREG: 0,1" in resp or "+CREG: 0,5" in resp


def is_attached(ser):
    resp = send_at(ser, "AT+CGATT?")
    print("CGATT response:", resp.strip())
    return "+CGATT: 1" in resp


def send_sms(number, text):
    # Free the port before opening
    free_port(MODEM_PORT)

    with serial.Serial(MODEM_PORT, RS485_BAUD_RATE, timeout=5) as ser:
        if "OK" not in send_at(ser, "AT"):
            print("Modem not responding.")
            return

        if not is_registered(ser):
            print("❌ SIM not registered on network.")
            return
        if not is_attached(ser):
            print("❌ SIM not attached to packet service.")
            return

        print(send_at(ser, "AT+CMGF=1"))

        ser.write(f'AT+CMGS="{number}"\r'.encode())
        time.sleep(0.5)

        ser.write(text.encode() + b"\x1A")
        time.sleep(5)

        response = ser.read_all().decode(errors="ignore")
        print("Final response:", response)

        if "OK" in response and "+CMGS" in response:
            print("✅ SMS sent successfully!")
        else:
            print("❌ SMS failed.")
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
def main_data_collection_loop():
    global data_map, analog_readings, digital_readings, modbus_readings

    # Setup modbus instruments
    modbus_instruments = setup_modbus_instruments()
    # Calculate polling intervals in seconds
    analog_interval_sec = convert_polling_interval_to_seconds(
        ANALOG_POLLING_INTERVAL, ANALOG_POLLING_UNIT
    )
    wireless_interval_sec = convert_polling_interval_to_seconds(
        WIRELESS_POLLING_INTERVAL, WIRELESS_POLLING_UNIT
    )

    print(f"[INFO] Starting data collection loop")
    print(f"[INFO] Analog polling interval: {analog_interval_sec} seconds")
    print(f"[INFO] Wireless polling interval: {wireless_interval_sec} seconds")

    last_analog_read = 0
    last_wireless_send = 0
    last_modbus_r1_read = 0
    last_modbus_r2_read = 0

    # Get polling intervals for modbus
    r1_interval = POLLING_INTERVALS.get("r1", {})
    r1_interval_sec = convert_polling_interval_to_seconds(
        r1_interval.get("interval", 5), r1_interval.get("unit", "Sec")
    )
    r2_interval = POLLING_INTERVALS.get("r2", {})
    r2_interval_sec = convert_polling_interval_to_seconds(
        r2_interval.get("interval", 3), r2_interval.get("unit", "Sec")
    )

    while True:
        try:
            current_time = time.time()
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            all_readings = {}

            # Read analog channels if enabled and interval has passed
            if (
                ANALOG_ENABLED
                and (current_time - last_analog_read) >= analog_interval_sec
            ):
                analog_readings = read_all_analog_channels()
                all_readings.update(analog_readings)
                last_analog_read = current_time
                print(f"[INFO] Read {len(analog_readings)} analog channels")

            # Read digital inputs if enabled
            if DIGITAL_INPUT_ENABLED:
                digital_readings = read_digital_inputs()
                all_readings.update(digital_readings)

            # Read modbus registers if enabled and intervals have passed
            if MODBUS_ENABLED and modbus_instruments:
                if (current_time - last_modbus_r1_read) >= r1_interval_sec:
                    r1_readings = read_modbus_registers(modbus_instruments)
                    all_readings.update(r1_readings)
                    last_modbus_r1_read = current_time

                if (current_time - last_modbus_r2_read) >= r2_interval_sec:
                    r2_readings = read_modbus_registers(modbus_instruments)
                    all_readings.update(r2_readings)
                    last_modbus_r2_read = current_time

            # Store readings if we have any
            if all_readings:
                data_map[timestamp] = all_readings

                # Insert into MySQL
                insert_readings_mysql(timestamp, all_readings)
                #kafka_publish("aggregate", {"ts": timestamp, "readings": all_readings})
                print(f"[INFO] Collected {len(all_readings)} readings at {timestamp}")

            # Send data if we have enough readings or time interval has passed
            if (
                len(data_map) >= 5
                or (current_time - last_wireless_send) >= wireless_interval_sec
            ):
                if data_map:
                    csv_path = get_csv_path()
                    write_to_csv(csv_path, data_map)

                    # Prepare data for sending
                    temp_file = "current_readings.csv"
                    write_to_csv(temp_file, data_map)

                    # Send via configured method
                    if COMMUNICATION_MEDIA == "4G/LTE":
                        try:
                            send_data_via_4g(temp_file)
                            last_wireless_send = current_time
                        except Exception as e:
                            print(f"[ERROR] 4G send failed: {e}")

                    # Send to TCP targets if configured
                    if len(data_map) >= 10:
                        payload = json.dumps(data_map, separators=(",", ":")).encode("utf-8")

                        for target in SEND_TARGETS:
                            try:
                                target_ip = target.get("ip", "")
                                target_port = int(target.get("port", 12345))

                                if not target_ip:
                                    continue

                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                    sock.settimeout(3)  # 3s timeout so it won't hang forever
                                    sock.connect((target_ip, target_port))
                                    sock.sendall(payload)
                                    print(f"[INFO] Sent {len(payload)} bytes to {target_ip}:{target_port}")

                            except Exception as e:
                                print(f"[ERROR] TCP send failed to {target_ip}:{target_port} -> {e}")

                    data_map.clear()

            # Sleep for a short interval to prevent excessive CPU usage
            time.sleep(1)

        except KeyboardInterrupt:
            print("[INFO] Data collection stopped by user")
            break
        except Exception as e:
            print(f"[CRITICAL] Unexpected error in main loop: {e}")
            time.sleep(2)

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
            print("[INFO] Alarm monitoring stopped by user")
            break
        except Exception as e:
            print(f"[ERROR] Alarm monitoring error: {e}")
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
                print(
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
                        print("[INFO] Scheduled offline data transfer")
                        # Transfer data via FTP
                        transfer_offline_data()

                time.sleep(60)  # Check every minute
            else:
                time.sleep(300)  # Check every 5 minutes if disabled

        except KeyboardInterrupt:
            print("[INFO] Offline data handler stopped by user")
            break
        except Exception as e:
            print(f"[ERROR] Offline data handler error: {e}")
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
            print(f"[INFO] Would transfer data to FTP server {server}:{port}/{folder}")
            # Implement actual FTP transfer here
            # import ftplib
            # ftp = ftplib.FTP()
            # ftp.connect(server, port)
            # ftp.login(username, password)
            # ... transfer files

    except Exception as e:
        print(f"[ERROR] FTP transfer failed: {e}")

"""
What: Periodically iterate configured Modbus TCP entries and (stub) read data.
Calls: (Future) pymodbus or similar
Required by: main() as background thread when MODBUS_TCP_ENABLED True
Notes: Currently logs intended actions; implement actual TCP reads per device.
Side effects: None besides logs (for now).
"""
def modbus_tcp_handler():
    if not MODBUS_TCP_ENABLED:
        return

    while True:
        try:
            for entry in MODBUS_TCP_TABLE:
                if entry.get("enabled", False):
                    slave_id = entry.get("slave", 1)
                    slave_ip = entry.get("slaveIp", "")
                    port = entry.get("socket", 502)
                    start_addr = entry.get("start", 1)
                    reg_type = entry.get("type", "Input Register")
                    length = entry.get("len", 1)

                    if slave_ip:
                        print(
                            f"[INFO] Would read Modbus TCP from {slave_ip}:{port} slave {slave_id} addr {start_addr}"
                        )
                        # Implement Modbus TCP communication here
                        # You would use pymodbus or similar library

            time.sleep(MODBUS_TCP_INTERVAL)

        except KeyboardInterrupt:
            print("[INFO] Modbus TCP handler stopped by user")
            break
        except Exception as e:
            print(f"[ERROR] Modbus TCP handler error: {e}")
            time.sleep(MODBUS_TCP_INTERVAL)

"""
What: Read last processed timestamp for polling deltas from a file.
Calls: built-in file I/O
Required by: poll_new_data()
Notes: File path fixed to /home/mpminfosoft/last_kafka_timestamp.txt.
Side effects: None.
"""
def get_last_timestamp():
    timestamp_file = "/home/mpminfosoft/last_kafka_timestamp.txt"
    try:
        if os.path.exists(timestamp_file):
            with open(timestamp_file, "r") as f:
                return f.read().strip()
        return None
    except Exception as e:
        print(f"[ERROR] Failed to read last timestamp: {e}")
        return None

"""
What: Persist last processed timestamp to a file for incremental polling.
Calls: built-in file I/O
Required by: poll_new_data()
Notes: Writes to /home/mpminfosoft/last_kafka_timestamp.txt.
Side effects: Overwrites file.
"""
def set_last_timestamp(timestamp):
    timestamp_file = "/home/mpminfosoft/last_kafka_timestamp.txt"
    try:
        with open(timestamp_file, "w") as f:
            f.write(str(timestamp))
    except Exception as e:
        print(f"[ERROR] Failed to save timestamp: {e}")

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
                print(f"[INFO] New records received: {len(data)} items")
                # Process the new data
                for record in data:
                    print(f"[DATA] Record: {record}")
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
                print("[INFO] No new records found.")
        else:
            print(f"[WARN] Poll failed: HTTP {response.status_code}")

    except Exception as e:
        print(f"[ERROR] Polling error: {e}")

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
        print(f"[INFO] Stored external data record")

    except Exception as e:
        print(f"[ERROR] Failed to store external data: {e}")

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
        print(f"[INFO] Processing config update: {config_data}")

        if "pollingInterval" in config_data:
            global WIRELESS_POLLING_INTERVAL
            WIRELESS_POLLING_INTERVAL = config_data["pollingInterval"]
            print(f"[INFO] Updated polling interval to {WIRELESS_POLLING_INTERVAL}")

    except Exception as e:
        print(f"[ERROR] Config update processing failed: {e}")

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
        print(val)
        return val
    except json.JSONDecodeError:
        print(f"[WARN] NON-JSON message skipped: {v}")
        return None

def ensure_topic_exists(topic_name):
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        existing_topics = admin.list_topics()
        if topic_name not in existing_topics:
            print(f"[INFO] Topic '{topic_name}' not found, creating...")
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin.create_topics(new_topics=[topic], validate_only=False)
            print(f"[INFO] Topic '{topic_name}' created.")
        else:
            print(f"[INFO] Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"[ERROR] Could not ensure topic exists: {e}")

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

    print(f"[INFO] Kafka consumer started on topic {topic} (batch {batch_max}/{batch_sec}s)")

    while True:
        try:
            for msg in consumer:
                record = {"key": msg.key, "value": msg.value, "ts": time.time()}
                batch.append(record)
                print(batch)
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
            print("[INFO] Kafka consumer stopping...")
            break
        except Exception as e:
            print(f"[ERROR] Kafka consumer error: {e}")
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
        print(f"[ERROR] Consumer thread crashed: {e}")
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
    compression = (config.get("kafka", {}).get("compression", "gzip") or "gzip")
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

    print(f"[INFO] Kafka producer initialized on {kafka_topic}")

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
        print("[INFO] Initializing Kafka producer lazily")
        init_local_kafka_producer()
    try:
        kafka_producer.send(KAFKA_TOPIC, key=source_key, value=payload).get(timeout=10)
        kafka_producer.flush()
    except Exception as e:
        now = time.time()
        if now - kafka_last_err > 10:
            print(f"[WARN] Kafka publish failed: {e}")
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
                print(f"[INFO] Received commands: {commands}")

                for command in commands:
                    process_remote_command(command)
            else:
                print(f"[WARN] Command polling failed: HTTP {response.status_code}")

        except Exception as e:
            print(f"[ERROR] Command polling error: {e}")

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
            print(f"[INFO] Config update command received: {cmd_data}")
            # Update configuration dynamically

        elif cmd_type == "restart":
            print("[INFO] Restart command received")
            # Implement restart logic

        elif cmd_type == "data_request":
            print(f"[INFO] Data request command: {cmd_data}")
            # Send specific data

        else:
            print(f"[WARN] Unknown command type: {cmd_type}")

    except Exception as e:
        print(f"[ERROR] Command processing error: {e}")

"""
What: Handle RS485 communication: periodically send latest data, and read/process inbound lines.
Calls: process_rs485_data()
Required by: main() as background thread
Notes: Writes a single-line JSON-framed message prefixed with 'RS485_DATA:'; replace with protocol as needed.
Side effects: Serial I/O; DB/Kafka via process_rs485_data().
"""
def rs485_communication_handler():
    try:
        ser = serial.Serial(RS485_PORT, RS485_BAUD_RATE, timeout=1)
        print(
            f"[INFO] RS485 communication started on {RS485_PORT} at {RS485_BAUD_RATE} baud"
        )

        while True:
            # Send any queued data
            if data_map:
                latest_timestamp = max(data_map.keys())
                latest_data = data_map[latest_timestamp]
                message = f"RS485_DATA:{json.dumps(latest_data)}\n"
                ser.write(message.encode())
                print(f"[INFO] Sent RS485 data: {len(message)} bytes")

            # Check for incoming data
            if ser.in_waiting:
                line = ser.readline().decode(errors="ignore").strip()
                if line:
                    print(f"[RS485 IN] {line}")
                    process_rs485_data(line)

            time.sleep(2)

    except Exception as e:
        print(f"[ERROR] RS485 communication error: {e}")

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
            print(f"[INFO] Received RS485 data: {parsed_data}")

            # Store or process the received data
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            insert_readings_mysql(
                timestamp, {f"rs485_{k}": v for k, v in parsed_data.items()}
            )
            #kafka_publish("rs485", {"ts": datetime.now(timezone.utc).isoformat(), "data": parsed_data})
    except Exception as e:
        print(f"[ERROR] RS485 data processing error: {e}")

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

            print(
                f"[STATUS] CPU: {cpu_percent}%, Memory: {memory.percent}%, Disk: {disk_usage.percent}%"
            )

            # Check network interfaces
            net_stats = psutil.net_if_stats()
            for interface, stats in net_stats.items():
                if interface in [LTE_INTERFACE, "eth0", "wlan0"]:
                    status = "UP" if stats.isup else "DOWN"
                    print(f"[STATUS] {interface}: {status}")

            # Log system status
            status_data = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": disk_usage.percent,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }

            # Insert system status into database
            try:
                conn = mysql.connector.connect(**MYSQL_CONFIG)
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
                print(f"[ERROR] System status database insert failed: {e}")

            time.sleep(60)  # Monitor every minute

        except KeyboardInterrupt:
            print("[INFO] System status monitor stopped by user")
            break
        except Exception as e:
            print(f"[ERROR] System status monitor error: {e}")
            time.sleep(60)


# ---- Priority Helper ----
def set_priority(rt_priority: int, nice_value: int):
    """
    Try to set real-time scheduling first (needs root).
    Fallback to nice if permission is denied.
    """
    try:
        # Real-time scheduling (requires sudo)
        param = os.sched_param(rt_priority)
        os.sched_setscheduler(0, os.SCHED_FIFO, param)
        print(f"[INIT] PID={os.getpid()} running with RT priority {rt_priority}")
    except PermissionError:
        # Fallback: adjust nice value (works without sudo for positive nice)
        try:
            os.nice(nice_value)
            print(f"[INIT] PID={os.getpid()} running with nice={nice_value} (fallback)")
        except Exception as e:
            print(f"[ERROR] Failed to set nice={nice_value}: {e}")
    except Exception as e:
        print(f"[ERROR] Priority set failed: {e}")


# ---- Worker Wrappers ----
def run_rs485():
    set_priority(rt_priority=80, nice_value=-10)  # Highest priority
    rs485_communication_handler()

def run_modbus_tcp():
    set_priority(rt_priority=70, nice_value=-5)
    modbus_tcp_handler()

def run_tcp_server():
    set_priority(rt_priority=60, nice_value=0)
    start_tcp_server()

def run_http_poller():
    set_priority(rt_priority=40, nice_value=5)
    http_command_poller()

def run_offline_handler():
    set_priority(rt_priority=30, nice_value=10)
    offline_data_handler()

def run_status_monitor():
    set_priority(rt_priority=20, nice_value=15)
    system_status_monitor()

def run_config_monitor():
    set_priority(rt_priority=10, nice_value=19)
    start_config_monitor()

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
# ---- Main ----
def main():
    if not UPDATED:
        update_global_config()
    print("=" * 60)
    print("ENHANCED DATA LOGGER SYSTEM STARTING (MULTIPROCESS MODE)")
    print("=" * 60)

    # Print configuration summary
    print(f"[CONFIG] Communication Media: {COMMUNICATION_MEDIA}")
    print(f"[CONFIG] Send Format: {SEND_FORMAT}")
    print(f"[CONFIG] Analog Enabled: {ANALOG_ENABLED}")
    print(f"[CONFIG] Digital Input Enabled: {DIGITAL_INPUT_ENABLED}")
    print(f"[CONFIG] Modbus Enabled: {MODBUS_ENABLED}")
    print(f"[CONFIG] Modbus TCP Enabled: {MODBUS_TCP_ENABLED}")
    print(f"[CONFIG] Offline Data Enabled: {OFFLINE_ENABLED}")
    print(f"[CONFIG] APN: {APN}")
    print(f"[CONFIG] JSON Endpoint: {JSON_ENDPOINT}")
    print(f"[CONFIG] RS485 Port: {RS485_PORT} @ {RS485_BAUD_RATE} baud")

    # Initialize 4G module
    print("[INIT] Initializing 4G module...")
    enable_4g_module()
    connect_4g()

    workers = []
    workers.append(("Config Monitor", run_config_monitor))
    workers.append(("TCP Server", run_tcp_server))
    workers.append(("HTTP Poller", run_http_poller))
    workers.append(("Offline Handler", run_offline_handler))
    if MODBUS_TCP_ENABLED:
        workers.append(("Modbus TCP", run_modbus_tcp))
    workers.append(("RS485", run_rs485))
    workers.append(("System Monitor", run_status_monitor))

    processes = []
    for name, func in workers:
        p = multiprocessing.Process(target=func, name=name, daemon=True)
        p.start()
        processes.append(p)
        print(f"[PROCESS] {name} started (PID={p.pid})")

    print("=" * 60)
    print("ALL BACKGROUND PROCESSES STARTED")
    print("STARTING MAIN DATA COLLECTION LOOP")
    print("=" * 60)

    try:
        main_data_collection_loop()
    except KeyboardInterrupt:
        print("\n[INFO] Shutdown requested by user")
    except Exception as e:
        print(f"\n[CRITICAL] Main loop crashed: {e}")
    finally:
        for p in processes:
            p.terminate()
        print("[INFO] All processes terminated")

    print("\n" + "=" * 60)
    print("DATA LOGGER SYSTEM SHUTTING DOWN")
    print("=" * 60)

if __name__ == "__main__":
    main()
