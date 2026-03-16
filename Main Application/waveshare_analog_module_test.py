#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import minimalmodbus
import serial

PORT = "/dev/ttyAMA4"
SLAVE_ID = 1

instrument = minimalmodbus.Instrument(PORT, SLAVE_ID)

# Serial settings
instrument.serial.baudrate = 9600
instrument.serial.bytesize = 8
instrument.serial.parity   = serial.PARITY_NONE
instrument.serial.stopbits = 1
instrument.serial.timeout  = 1

# Modbus RTU mode
instrument.mode = minimalmodbus.MODE_RTU

# Optional but recommended
instrument.clear_buffers_before_each_transaction = True
instrument.close_port_after_each_call = False

try:
    while True:
        # Read 8 input registers starting at address 0x0000
        values = instrument.read_registers(
            registeraddress=0,
            number_of_registers=8,
            functioncode=3
        )

        # Raw values (µA or mV depending on mode)
        print("Raw:", values)

        # Convert to mA if in current mode
        currents_mA = [v / 1000.0 for v in values]
        print("Current (mA):", currents_mA)

        print("-" * 40)
        time.sleep(1)

except Exception as e:
    print("Modbus error:", e)
