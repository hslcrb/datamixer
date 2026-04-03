import os
import sys
import concurrent.futures
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from PySide6.QtGui import QFont, QFontDatabase
from PySide6.QtWidgets import QApplication

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# --- Constants ---
FONT_PATH = resource_path("NanumBarunGothic.ttf")
SPLASH_PATH = resource_path("splash_bg.png")
ENCODING_CANDIDATES = [
    'utf-8', 'cp949', 'euc-kr', 'utf-8-sig', 'utf-16', 
    'cp1252', 'iso-8859-1', 'latin1', 'utf-16-le', 'utf-16-be'
]

def try_decode(chunk, encoding):
    try:
        chunk.decode(encoding)
        return encoding
    except:
        return None

def detect_encoding_parallel(file_path):
    try:
        with open(file_path, 'rb') as f:
            chunk = f.read(1024 * 100) 
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(lambda enc: try_decode(chunk, enc), ENCODING_CANDIDATES))
        for res in results:
            if res: return res
    except Exception as e:
        print(f"Encoding detection failed: {e}")
    return 'utf-8'

def setup_fonts():
    """Register the NanumBarunGothic font for both PySide6 and Matplotlib."""
    if os.path.exists(FONT_PATH):
        font_id = QFontDatabase.addApplicationFont(FONT_PATH)
        if font_id != -1:
            font_families = QFontDatabase.applicationFontFamilies(font_id)
            if font_families:
                font_family = font_families[0]
                QApplication.setFont(QFont(font_family, 10))
                print(f"PySide6 Font Registered: {font_family}")
        
        fe = fm.FontEntry(fname=FONT_PATH, name='NanumBarunGothic')
        fm.fontManager.ttflist.insert(0, fe)
        plt.rcParams['font.family'] = fe.name
        plt.rcParams['axes.unicode_minus'] = False 
        print(f"Matplotlib Font Registered: {fe.name}")
    else:
        print(f"Warning: {FONT_PATH} not found. Path: {os.path.abspath(FONT_PATH)}")
