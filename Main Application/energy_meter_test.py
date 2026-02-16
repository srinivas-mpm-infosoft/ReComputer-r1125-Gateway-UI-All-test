import struct
import time
import minimalmodbus
import serial

def regs_to_float(reg1, reg2, byte_order='big', word_order='little'):
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

# --- Modbus Instrument Configuration ---
# port = "/dev/energy_meter_usb"  # Change to your serial port, e.g., 'COM3' on Windows
port = "/dev/ttyACM1"  # Change to your serial port, e.g., 'COM3' on Windows
slave_id = 2           # Change to your Modbus slave ID

instrument = minimalmodbus.Instrument(port, slave_id)
instrument.serial.baudrate = 9600
instrument.serial.bytesize = 8
instrument.serial.parity = serial.PARITY_NONE
instrument.serial.stopbits = 2
instrument.serial.timeout = 1  # seconds

def read_floats(address, count):
    regs = instrument.read_registers(address, count)
    print(f"Raw registers at {address}: {regs}")
    floats = []
    for i in range(0, count, 2):
        f = regs_to_float(regs[i], regs[i+1], byte_order='big', word_order='little')
        floats.append(f)
    return floats

while True:
    try:
        currents = read_floats(2998, 6)
        print(f"Current A: {currents[0]:.3f} A")
        print(f"Current B: {currents[1]:.3f} A")
        print(f"Current C: {currents[2]:.3f} A")
    except Exception as e:
        print(f"Error reading currents: {e}")

    try:
        voltages_ll = read_floats(3018, 6)
        print(f"Voltage A-B: {voltages_ll[0]:.3f} V")
        print(f"Voltage B-C: {voltages_ll[1]:.3f} V")
        print(f"Voltage C-A: {voltages_ll[2]:.3f} V")
    except Exception as e:
        print(f"Error reading L-L voltages: {e}")

    try:
        voltages_ln = read_floats(3026, 6)
        print(f"Voltage A-N: {voltages_ln[0]:.3f} V")
        print(f"Voltage B-N: {voltages_ln[1]:.3f} V")
        print(f"Voltage C-N: {voltages_ln[2]:.3f} V")
    except Exception as e:
        print(f"Error reading L-N voltages: {e}")

    try:
        pf_regs = instrument.read_registers(3082, 2)
        print(f"Raw power factor registers: {pf_regs}")
        power_factor = regs_to_float(pf_regs[0], pf_regs[1], byte_order='big', word_order='little')
        print(f"Power Factor: {power_factor:.3f}")
    except Exception as e:
        print(f"Error reading power factor: {e}")

    try:
        freq = instrument.read_registers(3108, 2)
        print(f"Raw Frequency registers: {freq}")
        frequency = regs_to_float(freq[0], freq[1], byte_order='big', word_order='little')
        print(f"Frequenct: {frequency:.3f}")
    except Exception as e:
        print(f"Error reading power factor: {e}")

    print("=======================")
    time.sleep(5)