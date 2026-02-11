import _ssl
import ssl
import sys
from pathlib import Path

print("ssl.py      :", ssl.__file__)
print("_ssl module :", _ssl.__file__)

p = Path(_ssl.__file__).resolve()

if not str(p).startswith("/usr/lib/python3.13/"):
    sys.exit(f"FATAL: _ssl loaded from non-system location: {p}")

print("OK: system _ssl in use")
