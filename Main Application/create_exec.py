import os
import subprocess
import shutil
from pathlib import Path


# ============================================
# PYTHON FILES TO BUILD AS EXECUTABLES
# ============================================

FILES = [
    # "combined_innodose_generate.py",
    #"combined_innodose_read_plc.py",
    # "merge_data_pc.py",
    #"plc1_plc_to_local_generate.py", 
    # "plc1_plc_to_local_read_plc.py",
    # "process_experiment_data.py",
    # "save_calculations.py",
    # "merge_etl_plc.py"
    # "main_linux_gateway_ui.py",
    # "main_program.py",
    #"main_linux_innodose_sql_alchemy.py",
    # "main_linux_no_db.py",
    # "inodose_service_scheduler.py",
    #"alerts_grafana.py",
    "network_apply.py",
    # "smb_final.py"
    # "ssl_test.py"
]


# ============================================
# DATA FILES BUNDLED INTO EACH EXECUTABLE
# ============================================
RESOURCE_ITEMS = [
    ("static", "static"),
]


# ============================================
# HIDDEN IMPORTS REQUIRED FOR FREEZE
# ============================================
HIDDEN_IMPORTS = [
    # Core libs
    "numpy",
    "pandas",
    "asyncio",
    "pymodbus",
    "pymodbus.client",

    # Flask stack
    "flask",
    "flask_sqlalchemy",
    "flask_cors",
    "pysmb",
    "pymysql"
]


# ============================================
# INSTALL PYINSTALLER IF MISSING
# ============================================
def ensure_pyinstaller():
    try:
        subprocess.run(
            ["pyinstaller", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        print("Installing PyInstaller…")
        subprocess.run(["pip", "install", "pyinstaller"], check=True)


# ============================================
# DATA PACKAGING (ABSOLUTE PATH FIX)
# ============================================
def add_data_params():
    params = []

    for src, dest in RESOURCE_ITEMS:
        p = Path(src).resolve()
        if not p.exists():
            print(f"⚠️ Missing resource → {src}")
            continue

        params += ["--add-data", f"{p}:{dest}"]

    return params


# ============================================
# BUILD ONE EXECUTABLE
# ============================================
def build_executable(pyfile):
    print(f"\n📌 Building → {pyfile}")

    cmd = [
        "pyinstaller",
        "--onefile",
        "--clean",
        "--noconfirm",
        "--distpath", "dist",
        "--workpath", "build_tmp",
        "--log-level", "INFO",
    ]

    for h in HIDDEN_IMPORTS:
        cmd += ["--hidden-import", h]

    cmd += add_data_params()
    cmd.append(pyfile)

    subprocess.run(cmd, check=True)


# ============================================
# CLEAN BUILD ARTIFACTS (FULL CLEAN)
# ============================================
def clean_all():
    folders = ["build_tmp", "build", "__pycache__"]
    for f in folders:
        if os.path.exists(f):
            shutil.rmtree(f)

    os.system("find . -name '*.pyc' -delete")

    pyinstaller_cache = Path.home() / ".cache" / "pyinstaller"
    if pyinstaller_cache.exists():
        shutil.rmtree(pyinstaller_cache)


# ============================================
# MAIN ENTRY
# ============================================
def main():
    ensure_pyinstaller()
    clean_all()

    for f in FILES:
        if not os.path.exists(f):
            print(f"❌ Script not found: {f}")
            return

    for f in FILES:
        build_executable(f)

    print("\n🎉 ALL EXECUTABLES BUILT SUCCESSFULLY!")
    print("📌 Output folder → dist/")


if __name__ == "__main__":
    main()
