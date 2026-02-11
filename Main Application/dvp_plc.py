from pymodbus.client import ModbusTcpClient
import time

'''client = ModbusClient(host ="192.168.1.5", port =502, auto_open=True)
value = 2000
addr = 100

ok = client.write_single_register(addr, value)
print("Write success:", ok)'''

print("Hello")
PLC_IP = "192.168.1.5"
PLC_PORT = 502
UNIT_ID = 1
AO1_REGISTER_ADDR = 100
AI1_REGISTER_ADDR = 6


def write_mv(value):
    client = ModbusTcpClient(PLC_IP, port =PLC_PORT)
    if not client.connect():
        print("Could not connect to PLC")
        return None
    response = client.write_register(AO1_REGISTER_ADDR, value)
    if response.isError():
        print("Write failed:", response)
    else:
        print(f"Sent {value} to D{AO1_REGISTER_ADDR}")

    client.close()

def read_pv():
    client = ModbusTcpClient(PLC_IP, port =PLC_PORT)
    if not client.connect():
        print("Could not connect to PLC")
        return None
    
    response = client.read_holding_registers(AI1_REGISTER_ADDR, count=1)

    if response.isError():
        print("Read failed:", response)
        return None
    

    client.close()

    value2 = response.registers[0]
    return value2

if __name__ =="__main__":
    write_mv(1000)

    raw_ai0 = read_pv()
    print("AI0 raw:", raw_ai0)
    time.sleep(1)