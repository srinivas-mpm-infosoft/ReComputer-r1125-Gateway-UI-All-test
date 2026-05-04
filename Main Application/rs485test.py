import serial
import time

PORT = "/dev/ttyACM0"
BAUDRATE = 9600      # change if your device expects something else
MESSAGE = "temperature reading is 35 deg.\r\n"

try:
    ser = serial.Serial(
        port=PORT,
        baudrate=BAUDRATE,
        # bytesize=serial.EIGHTBITS,
        # parity=serial.PARITY_NONE,
        # stopbits=serial.STOPBITS_ONE,
        timeout=1
    )

    time.sleep(0.5)  # give UART time to settle

    while 1:
        ser.write(MESSAGE.encode("ascii"))
        ser.flush()

        print("Message sent")
        time.sleep(0.5)

except serial.SerialException as e:
    print("Serial error:", e)

finally:
    if 'ser' in locals() and ser.is_open:
        ser.close()
