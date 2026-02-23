# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['main_linux_gateway_ui.py'],
    pathex=[],
    binaries=[],
    datas=[('/home/recomputer/Gateway-UI/Main Application/static', 'static')],
    hiddenimports=['numpy', 'pandas', 'asyncio', 'pymodbus', 'pymodbus.client', 'flask', 'flask_sqlalchemy', 'flask_cors', 'pysmb', 'pymysql'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='main_linux_gateway_ui',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
