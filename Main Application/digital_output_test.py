# #!/usr/bin/python3
# # -*- coding: utf-8 -*-

# import time
# import logging
# import RPi.GPIO as GPIO

# IN1 = 23
# IN2 = 24

# OUT1 = 17
# OUT2 = 27

# HIGH_TIME = 5   # seconds output stays HIGH
# LOW_TIME = 3    # seconds output stays LOW

# GPIO.setmode(GPIO.BCM)
# GPIO.setwarnings(False)

# GPIO.setup(OUT1, GPIO.OUT, initial=GPIO.LOW)
# GPIO.setup(OUT2, GPIO.OUT, initial=GPIO.LOW)

# GPIO.setup(IN1, GPIO.IN)
# GPIO.setup(IN2, GPIO.IN)

# try:
#     while True:
#         # 🔴 HIGH phase
#         GPIO.output(OUT1, GPIO.HIGH)
#         GPIO.output(OUT2, GPIO.HIGH)

#         print("OUT1=1 OUT2=1")
#         print(f"IN1={GPIO.input(IN1)} IN2={GPIO.input(IN2)}")
#         time.sleep(HIGH_TIME)

#         # ⚫ LOW phase
#         GPIO.output(OUT1, GPIO.LOW)
#         GPIO.output(OUT2, GPIO.LOW)

#         print("OUT1=0 OUT2=0")
#         print(f"IN1={GPIO.input(IN1)} IN2={GPIO.input(IN2)}")
#         time.sleep(LOW_TIME)

# except KeyboardInterrupt:
#     print("\nExiting cleanly")

# finally:
#     GPIO.cleanup()

#!/usr/bin/python
# -*- coding:utf-8 -*-
import serial
import os
import sys
import logging


    
import RPi.GPIO 
import time


IN1 = 23;
IN2 = 24;

OUT1 = 17;
OUT2 = 27;


GPIO = RPi.GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)


GPIO.setup(OUT1, GPIO.OUT)
GPIO.setup(OUT2, GPIO.OUT)


GPIO.setup(IN1, GPIO.IN)
GPIO.setup(IN2, GPIO.IN)


try:
    i = 0
    while(1):
        GPIO.output(OUT1, i%2)
        GPIO.output(OUT2, (i+1)%2)
        print("OUT1 %d       OUT2  %d"%((i%2),(i+1)%2))
        print("IN1  %d       IN2   %d\r\n\r\n"%(GPIO.input(IN1),GPIO.input(IN2)))
        time.sleep(1)
        i=i+1;
        if(i>=100):
            i = 0;
        
        
except KeyboardInterrupt:    
    logging.info("ctrl + c:")
    exit()