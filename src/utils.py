import os
import concurrent.futures
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from PySide6.QtGui import QFont, QFontDatabase
from PySide6.QtWidgets import QApplication

# --- Constants ---
FONT_PATH = "NanumBarunGothic.ttf"
ENCODING_CANDIDATES = [
    'utf-8', 'cp949', 'euc-kr', 'utf-8-sig', 'utf-16', 
    'cp1252', 'iso-8859-1', 'latin1', 'utf-16-le', 'utf-16-be'
]

def try_decode(chunk, encoding):
    """Helper to try decoding a chunk with a specific encoding."""
    try:
        chunk.decode(encoding)
        return encoding
    except:
        return None

def detect_encoding_parallel(file_path):
    """Detect encoding of a file by trying candidates in parallel."""
    try:
        with open(file_path, 'rb') as f:
            chunk = f.read(1024 * 100) # Read first 100KB
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Map try_decode across candidates
            results = list(executor.map(lambda enc: try_decode(chunk, enc), ENCODING_CANDIDATES))
            
        # Return the first successful encoding
        for res in results:
            if res:
                return res
    except Exception as e:
        print(f"Encoding detection failed: {e}")
    return 'utf-8' # Default fallback

def setup_fonts():
    """Register the NanumBarunGothic font for both PySide6 and Matplotlib."""
    if os.path.exists(FONT_PATH):
        # 1. Register for PySide6
        font_id = QFontDatabase.addApplicationFont(FONT_PATH)
        if font_id != -1:
            font_family = QFontDatabase.applicationFontFamilies(font_id)[0]
            QApplication.setFont(QFont(font_family, 10))
            print(f"PySide6 Font Registered: {font_family}")
        
        # 2. Register for Matplotlib
        fe = fm.FontEntry(fname=FONT_PATH, name='NanumBarunGothic')
        fm.fontManager.ttflist.insert(0, fe)
        plt.rcParams['font.family'] = fe.name
        plt.rcParams['axes.unicode_minus'] = False  # Fix minus sign display
        print(f"Matplotlib Font Registered: {fe.name}")
    else:
        print(f"Warning: {FONT_PATH} not found. Using default fonts.")
