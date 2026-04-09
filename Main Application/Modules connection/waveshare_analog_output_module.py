from pymodbus.client import ModbusSerialClient
import time

PORT = "/dev/ttyACM1"
SLAVE_ID = 3
BASE_ADDR = 0

client = ModbusSerialClient(
    port=PORT,
    baudrate=9600,
    parity='N',
    stopbits=1,
    bytesize=8,
    timeout=1
)

if not client.connect():
    print("Unable to connect")
    exit()

print("Connected (Percentage Control Mode)")


# -----------------------------
# % → DAC
# -----------------------------
def percent_to_dac(percent):
    if not (0 <= percent <= 100):
        raise ValueError("Percentage must be 0–100")
    return int((percent / 100.0) * 20000)


# -----------------------------
# DAC → %
# -----------------------------
def dac_to_percent(dac):
    return (dac / 20000.0) * 100


# -----------------------------
# WRITE SINGLE CHANNEL
# -----------------------------
def set_channel_percent(channel, percent):
    dac_value = percent_to_dac(percent)

    result = client.write_registers(
        address=BASE_ADDR + channel,
        values=[dac_value],
        slave=SLAVE_ID
    )

    if result.isError():
        print(f"[ERROR] CH{channel+1} write failed")
    else:
        print(f"[OK] CH{channel+1} → {percent:.1f}% (DAC={dac_value})")


# -----------------------------
# WRITE ALL CHANNELS
# -----------------------------
def set_all_percent(percent_list):
    if len(percent_list) != 8:
        raise ValueError("Need 8 values")

    values = [percent_to_dac(p) for p in percent_list]

    result = client.write_registers(
        address=BASE_ADDR,
        values=values,
        slave=SLAVE_ID
    )

    if result.isError():
        print("[ERROR] Bulk write failed")
    else:
        print("[OK] All channels updated")


# -----------------------------
# READ BACK
# -----------------------------
def read_all_percent():
    result = client.read_holding_registers(
        address=BASE_ADDR,
        count=8,
        slave=SLAVE_ID
    )

    if result.isError():
        print("[ERROR] Read failed")
        return

    print("\n--- Channel Status ---")
    for i, val in enumerate(result.registers):
        percent = dac_to_percent(val)
        print(f"CH{i+1}: {percent:.1f}% (DAC={val})")


# -----------------------------
# TEST
# -----------------------------

# Ramp test
for i in range(8):
    percent = (i + 1) * 12.5   # 12.5% → 100%
    print(f"\nSetting CH{i+1} to {percent}%")
    set_channel_percent(i, percent)
    time.sleep(0.5)

time.sleep(2)

# Bulk test
print("\nSetting all channels")
set_all_percent([0, 10, 20, 40, 60, 80, 90, 100])

time.sleep(2)

# Read back
read_all_percent()

client.close()