from pymodbus.client import ModbusSerialClient
import time

# CONFIGURATION
PORT = "/dev/ttyACM1"
BAUDRATE = 9600
SLAVE_ID = 1   # change if your device address is different

# Create client
client = ModbusSerialClient(
    port=PORT,
    baudrate=BAUDRATE,
    parity='N',
    stopbits=1,
    bytesize=8,
    timeout=1
)

if not client.connect():
    print("❌ Unable to connect")
    exit()

print("✅ Connected to relay")

# -------------------------------
# FUNCTION: Turn ON relay
# -------------------------------
def relay_on(channel):
    # channels start from 0 (0–7)
    result = client.write_coil(channel, True)
    print(f"Relay {channel} ON →", result)

# -------------------------------
# FUNCTION: Turn OFF relay
# -------------------------------
def relay_off(channel):
    result = client.write_coil(channel, False)
    print(f"Relay {channel} OFF →", result)

# -------------------------------
# FUNCTION: Read all relay states
# -------------------------------
def read_relays():
    result = client.read_coils(address=0, count=8)
    if result.isError():
        print("❌ Read error:", result)
    else:
        print("Relay states:", result.bits)

# -------------------------------
# TEST SEQUENCE
# -------------------------------
try:
    print("\n--- TEST START ---")

    read_relays()

    print("\nTurning ON relay 0")
    relay_on(0)
    time.sleep(1)

    read_relays()

    print("\nTurning OFF relay 0")
    relay_off(0)
    time.sleep(1)

    read_relays()

    print("\nTurning ALL ON")
    for i in range(8):
        relay_on(i)
        time.sleep(1)
    time.sleep(1)

    read_relays()

    print("\nTurning ALL OFF")
    for i in range(8):
        relay_off(i)
        time.sleep(1)

    read_relays()

finally:
    client.close()
    print("\n🔌 Connection closed")