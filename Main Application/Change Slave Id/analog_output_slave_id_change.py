import serial
import time

PORT = "/dev/ttyACM1"
BAUDRATE = 9600
TIMEOUT = 1

NEW_ID = 3   # 🔥 CHANGE THIS

# ─────────────────────────────────────────
# CRC16 (Modbus)
# ─────────────────────────────────────────
def crc16(data: bytes):
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc.to_bytes(2, byteorder='little')

# ─────────────────────────────────────────
# READ DEVICE ID
# ─────────────────────────────────────────
def read_id(ser):
    payload = bytes([0x00, 0x03, 0x40, 0x00, 0x00, 0x01])
    cmd = payload + crc16(payload)

    print(f"[READ] TX: {cmd.hex(' ').upper()}")

    ser.reset_input_buffer()
    ser.write(cmd)
    time.sleep(0.1)

    resp = ser.read(7)
    print(f"[READ] RX: {resp.hex(' ').upper()}")

    if len(resp) < 7:
        print("❌ No response")
        return None

    if crc16(resp[:-2]) != resp[-2:]:
        print("❌ CRC error")
        return None

    return int.from_bytes(resp[3:5], byteorder='big')


# ─────────────────────────────────────────
# SET DEVICE ID
# ─────────────────────────────────────────
def set_id(ser, new_id):
    payload = bytes([0x00, 0x06, 0x40, 0x00, 0x00, new_id])
    cmd = payload + crc16(payload)

    print(f"[WRITE] TX: {cmd.hex(' ').upper()}")

    ser.reset_input_buffer()
    ser.write(cmd)
    time.sleep(0.1)

    resp = ser.read(8)
    print(f"[WRITE] RX: {resp.hex(' ').upper()}")

    if len(resp) < 8:
        print("❌ No response")
        return False

    if crc16(resp[:-2]) != resp[-2:]:
        print("❌ CRC error")
        return False

    if resp != cmd:
        print("❌ Echo mismatch")
        return False

    return True


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
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

            print(f"\nConnected → {PORT} @ {BAUDRATE}\n")

            # Step 1: Read current ID
            current = read_id(ser)
            if current is None:
                print("❌ Failed to read ID")
                return

            print(f"Current ID: {current}")

            if current == NEW_ID:
                print("✅ Already set")
                return

            # Step 2: Write new ID
            print(f"\nSetting new ID → {NEW_ID}")
            if not set_id(ser, NEW_ID):
                print("❌ Failed to set ID")

                return

            print("\n⚠️ IMPORTANT: Power cycle the module now!")
            input("Press ENTER after power cycle...")

            # Step 3: Verify
            new_val = read_id(ser)

            if new_val == NEW_ID:
                print(f"\n✅ SUCCESS: {current} → {new_val}")
            else:
                print(f"\n❌ FAILED: got {new_val}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()