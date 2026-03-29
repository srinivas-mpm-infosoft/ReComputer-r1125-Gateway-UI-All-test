import subprocess
import time

# Define your output pins
OUTPUT_PINS = {
    "GPIO 1": 24,
    "GPIO 2": 25,
    "GPIO 3": 26,
    "GPIO 4": 6
}

def get_gpio_status(pin):
    try:
        result = subprocess.check_output(
            ["raspi-gpio", "get", str(pin)],
            universal_newlines=True
        )
        return result.strip()
    except Exception as e:
        return f"Error reading GPIO {pin}: {e}"

def print_outputs():
    print("\n===== GPIO OUTPUT STATUS =====")
    for name, pin in OUTPUT_PINS.items():
        status = get_gpio_status(pin)
        print(f"{name} (Pin {pin}): {status}")
    print("================================\n")

if __name__ == "__main__":
    while True:
        print_outputs()
        time.sleep(5)