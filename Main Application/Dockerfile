FROM python:3.11-bullseye

WORKDIR /app

# --- Install system dependencies and build tools ---
RUN apt-get update && \
    apt-get install -y \
        supervisor \
        gcc g++ \
        libffi-dev \
        build-essential \
        python3-smbus \
        i2c-tools \
        gpiod \
        sudo \
        isc-dhcp-client \
        iproute2 \
        nano \
        vim \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install Python dependencies + Cython
RUN pip install --no-cache-dir -r requirements.txt cython

# === Cythonize alltest.py ===
RUN mkdir -p build && \
    cython --embed -3 -o build/alltest.c alltest_in_python_threads.py && \
    gcc -Os -I/usr/local/include/python3.11 -o alltest build/alltest.c \
        -lpython3.11 -lpthread -lm -lutil -ldl && \
    rm -rf build && \
    rm -f alltest_in_python_threads.py  # remove Python source

# (optional) also cythonize main_windows.py if you want to hide that too
# RUN mkdir -p build && \
#     cython --embed -3 -o build/main.c main_windows.py && \
#     gcc -Os -I/usr/local/include/python3.11 -o main build/main.c \
#         -lpython3.11 -lpthread -lm -lutil -ldl && \
#     rm -rf build && \
#     rm -f main_windows.py && \
#     rm -rf build

# Copy supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose FastAPI port
EXPOSE 8000

# Run supervisor

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]