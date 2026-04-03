# -*- mode: python ; coding: utf-8 -*-
import os
import sys
from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# Project details
version = '7.0.0'
app_name = f'Datamixer_Enterprise_V7'

# Define all data files to bundle
datas = [
    ('NanumBarunGothic.ttf', '.'),
    ('splash_bg.png', '.'),
]

# Collect additional data for heavy modules
datas += collect_data_files('plotly')
datas += collect_data_files('qdarktheme')

a = Analysis(
    ['main.py'],
    pathex=[os.getcwd()],
    binaries=[],
    datas=datas,
    hiddenimports=[
        'pandas', 'numpy', 'matplotlib.pyplot', 'seaborn', 'PySide6.QtWebEngineWidgets',
        'PySide6.QtWebEngineCore', 'qdarktheme', 'openpyxl', 'qtconsole', 'ipykernel',
        'plotly', 'pyarrow', 'polars', 'scipy', 'darkdetect', 'jupyterlab',
        'src.app', 'src.engine', 'src.repl', 'src.session', 'src.theme', 'src.utils',
        'src.viz_manager', 'src.worker', 'src.models', 'src.settings', 'src.jupyter_manager'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # Icon explicitly excluded as requested
)
