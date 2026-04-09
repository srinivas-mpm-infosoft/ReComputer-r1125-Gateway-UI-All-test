from pymodbus.client import ModbusSerialClient
import time

# Serial settings (change according to your system)
PORT = "/dev/ttyACM1"     # or /dev/ttyAMA0 etc
SLAVE_ID = 1              # device address

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

print("Connected to device")

# -----------------------------
# READ DIGITAL INPUTS
# -----------------------------
def read_inputs():
    result = client.read_discrete_inputs(address=0, count=8)
    
    if result.isError():
        print("Error reading inputs")
        return
    
    for i, state in enumerate(result.bits):
        print(f"Input {i+1}: {state}")


# -----------------------------
# READ OUTPUT STATUS
# -----------------------------
def read_outputs():
    result = client.read_coils(address=0, count=8)

    if result.isError():
        print("Error reading outputs")
        return
    
    for i, state in enumerate(result.bits):
        print(f"Output {i+1}: {state}")


# -----------------------------
# WRITE SINGLE OUTPUT
# -----------------------------
def set_output(channel, state):
    # channel: 0-7
    result = client.write_coil(channel, state)

    if result.isError():
        print("Write failed")
    else:
        print(f"Output {channel+1} set to {state}")


# -----------------------------
# WRITE MULTIPLE OUTPUTS
# -----------------------------
def set_all_outputs(states):
    # states example: [True, False, True, False, False, False, False, False]
    result = client.write_coils(address=0, values=states)

    if result.isError():
        print("Write failed")
    else:
        print("Outputs updated")


# -----------------------------
# Example usage
# -----------------------------
read_inputs()
read_outputs()

for i in range(8):
	print("\nTurning ON output 1")
	set_output(i, True)
	time.sleep(0.5)

time.sleep(2)
for i in range(8):
	print("Turning OFF output 1")
	set_output(i, False)
	time.sleep(0.5)

client.close()
