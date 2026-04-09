import serial
import time

# --- Configuration ---
# PORT = "/dev/serial/by-id/usb-1a86_USB_Single_Serial_5A99022512-if00"

PORT = "/dev/ttyACM1"
# BAUDRATE = 9600
# TIMEOUT  = 1        # seconds

# def crc16(data: bytes):
#     """Calculate Modbus CRC16 checksum."""
#     crc = 0xFFFF
#     for byte in data:
#         crc ^= byte
#         for _ in range(8):
#             if crc & 0x0001:
#                 crc = (crc >> 1) ^ 0xA001
#             else:
#                 crc >>= 1
#     # Return as little-endian 2 bytes
#     return crc.to_bytes(2, byteorder='little')

# def read_device_id(ser: serial.Serial):
#     """
#     Send Read Device Address command using broadcast address (0x00).
#     Command: 00 03 40 00 00 01 + CRC
#     Expected response: 00 03 02 00 <ID> <CRC>
#     """
#     payload = bytes([0x00, 0x03, 0x40, 0x00, 0x00, 0x01])
#     command = payload + crc16(payload)

#     print(f"Sending : {command.hex(' ').upper()}")

#     ser.reset_input_buffer()
#     ser.write(command)
#     time.sleep(0.1)  # Wait for response

#     response = ser.read(7)  # Expect 7 bytes back
#     print(f"Received: {response.hex(' ').upper()}")

#     if len(response) < 7:
#         print("Error: No response or incomplete response received.")
#         return None

#     # Validate CRC
#     received_crc = response[-2:]
#     calculated_crc = crc16(response[:-2])
#     if received_crc != calculated_crc:
#         print(f"Error: CRC mismatch. Got {received_crc.hex()}, expected {calculated_crc.hex()}")
#         return None

#     # Device ID is in bytes 4-5 (16-bit value, big-endian)
#     device_id = int.from_bytes(response[3:5], byteorder='big')
#     return device_id

# def main():
#     try:
#         with serial.Serial(
#             port=PORT,
#             baudrate=BAUDRATE,
#             bytesize=serial.EIGHTBITS,
#             parity=serial.PARITY_NONE,
#             stopbits=serial.STOPBITS_ONE,
#             timeout=TIMEOUT
#         ) as ser:
#             print(f"Connected to {PORT} at {BAUDRATE} baud\n")

#             device_id = read_device_id(ser)

#             if device_id is not None:
#                 print(f"\n✓ Device ID: {device_id} (0x{device_id:02X})")
#             else:
#                 print("\n✗ Failed to read Device ID.")

#     except serial.SerialException as e:
#         print(f"Serial error: {e}")

# if __name__ == "__main__":
#     main()


BAUDRATE    = 9600
TIMEOUT     = 1        # seconds
NEW_ID      = 3        # Target Device ID (1–255)

def crc16(data: bytes):
    """Calculate Modbus CRC16 checksum."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc.to_bytes(2, byteorder='little')

def read_device_id(ser: serial.Serial):
    """
    Read current Device ID using broadcast address.
    Command: 00 03 40 00 00 01 + CRC
    """
    payload = bytes([0x00, 0x03, 0x40, 0x00, 0x00, 0x01])
    command = payload + crc16(payload)

    print(f"  TX: {command.hex(' ').upper()}")
    ser.reset_input_buffer()
    ser.write(command)
    time.sleep(0.1)

    response = ser.read(7)
    print(f"  RX: {response.hex(' ').upper()}")

    if len(response) < 7:
        print("  Error: No or incomplete response.")
        return None

    if crc16(response[:-2]) != response[-2:]:
        print("  Error: CRC mismatch.")
        return None

    return int.from_bytes(response[3:5], byteorder='big')

def set_device_id(ser: serial.Serial, new_id: int):
    """
    Set new Device ID using broadcast address.
    Command: 00 06 40 00 00 <new_id> + CRC
    Expected echo response matches sent command.
    """
    payload = bytes([0x00, 0x06, 0x40, 0x00, 0x00, new_id])
    command = payload + crc16(payload)

    print(f"  TX: {command.hex(' ').upper()}")
    ser.reset_input_buffer()
    ser.write(command)
    time.sleep(0.1)

    response = ser.read(8)
    print(f"  RX: {response.hex(' ').upper()}")

    if len(response) < 8:
        print("  Error: No or incomplete response.")
        return False

    if crc16(response[:-2]) != response[-2:]:
        print("  Error: CRC mismatch.")
        return False

    # Response should echo back the same command
    if response != command:
        print("  Error: Response does not match sent command.")
        return False

    return True

def main():
    try:
        with serial.Serial(
            port=PORT,
            baudrate=BAUDRATE,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=TIMEOUT
        ) as ser:
            print(f"Connected to {PORT} at {BAUDRATE} baud\n")

            # Step 1 — Read current Device ID

            print("[ Step 1 ] Reading current Device ID...")
            current_id = read_device_id(ser)
            if current_id is None:
                print("✗ Could not read current Device ID. Aborting.")
                return
            print(f"  → Current Device ID: {current_id} (0x{current_id:02X})\n")

            # Step 2 — Check if change is needed
            
            if current_id == NEW_ID:
                print(f"✓ Device ID is already {NEW_ID}. No change needed.")
                return

            # Step 3 — Write new Device ID

            # print(f"[ Step 2 ] Setting Device ID to {NEW_ID} (0x{NEW_ID:02X})...")
            # success = set_device_id(ser, NEW_ID)
            # if not success:
            #     print("✗ Failed to set new Device ID. Aborting.")
            #     return
            # print(f"  → Write command acknowledged.\n")

            # Step 4 — Power cycle reminder

            # print("[ Step 3 ] Please POWER CYCLE the module now, then press Enter to verify...")
            # input()

            # Step 5 — Verify new Device ID

            # print("[ Step 4 ] Verifying new Device ID...")
            # verified_id = read_device_id(ser)
            # if verified_id is None:
            #     print("✗ Could not verify new Device ID.")
            #     return

            # if verified_id == NEW_ID:
            #     print(f"\n✓ Success! Device ID changed: {current_id} → {verified_id} (0x{verified_id:02X})")
            # else:
            #     print(f"\n✗ Unexpected Device ID: {verified_id}. Expected {NEW_ID}.")

    except serial.SerialException as e:
        print(f"Serial error: {e}")

if __name__ == "__main__":
    main()