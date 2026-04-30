 # Minimal Siemens PLC Read/Write using python-snap7
# Install:
# pip install python-snap7

import snap7
#from snap7.util import get_bool, set_bool, get_dint, set_dint, set_real, get_real, get_int, set_int
from snap7.util import *
from snap7.type import Areas

# ----------------------------
# PLC Connection Details
# ----------------------------
PLC_IP = "192.168.0.1"   # Change to your PLC IP
RACK = 0                # Usually 0 for S7-1200 / S7-1500
SLOT = 1                # Usually 1 for S7-1200 / S7-1500

client = snap7.client.Client()

try:
    # Connect to PLC
    client.connect(PLC_IP, RACK, SLOT)

    if client.get_connected():
        print("Connected to Siemens PLC")

    # ----------------------------
    # READ Example
    # Read 1 byte from DB1 starting at byte 0
    # ----------------------------
    db_number = 1
    start = 0
    size = 4

    data = client.db_read(db_number, start, size)

   # Read bit 0 of byte 0
    bit_value = get_dint(data, 0) 
    print("Value", bit_value)

    # ----------------------------
    # WRITE Example
    # Write 800 to address
    # ----------------------------
    set_dint(data, 0, 800)

    client.db_write(db_number, start, data)

    print("Wrote 800 to address")


    
    # Read bit 0 of byte 0
    bit_value = get_dint(data, 0)
    print("Value", bit_value)

except Exception as e:
    print("Error:", e)

finally:
    client.disconnect()
    client.destroy()
    print("Disconnected")
