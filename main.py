import sys
import os
import time
from PySide6.QtWidgets import QApplication, QSplashScreen, QProgressBar, QVBoxLayout, QLabel, QWidget
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QPixmap, QFont

from src.utils import setup_fonts
from src.app import DataExplorerApp

class PremiumSplash(QSplashScreen):
    """Premium Enterprise Launcher with Cinematic Clipping Mask."""
    def __init__(self):
        # Cinematic Wide Ratio (e.g. 21:9 or 16:9 widescreen)
        w, h = 900, 400
        bg_path = "splash_bg.png"
        if os.path.exists(bg_path):
            pixmap = QPixmap(bg_path).scaled(w, h, Qt.KeepAspectRatioByExpanding, Qt.SmoothTransformation)
        else:
            pixmap = QPixmap(w, h); pixmap.fill(Qt.transparent)

        # Create rounded clipping mask for premium feel
        from PySide6.QtGui import QRegion, QPainter, QPainterPath
        from PySide6.QtCore import QRectF, QPoint
        
        # We wrap the pixmap in a rounded rect
        rounded_pixmap = QPixmap(w, h)
        rounded_pixmap.fill(Qt.transparent)
        
        painter = QPainter(rounded_pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        path = QPainterPath()
        path.addRoundedRect(QRectF(0, 0, w, h), 20, 20)
        painter.setClipPath(path)
        painter.drawPixmap(0, 0, pixmap)
        painter.end()
        
        super().__init__(rounded_pixmap)
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
        self.setMask(rounded_pixmap.mask()) # Precise clipping
        
        # Add a custom widget for the progress bar overlay
        self.container = QWidget(self)
        self.container.setGeometry(40, h - 90, w - 80, 70)
        self.layout = QVBoxLayout(self.container)
        
        self.status_label = QLabel("Datamixer Core V7 Initializing...")
        self.status_label.setStyleSheet("color: #c0caf5; font-weight: bold; font-family: 'Segoe UI', sans-serif; font-size: 10pt; background: transparent;")
        self.layout.addWidget(self.status_label)
        
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(4)
        self.progress.setStyleSheet("""
            QProgressBar { border: none; background-color: rgba(26, 27, 38, 150); border-radius: 2px; }
            QProgressBar::chunk { background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #7aa2f7, stop:1 #bb9af7); border-radius: 2px; }
        """)
        self.layout.addWidget(self.progress)

    def update_msg(self, text, val):
        self.status_label.setText(text)
        self.progress.setValue(val)
        QApplication.processEvents() # Force UI refresh

def main():
    app = QApplication(sys.argv)
    
    # 1. Immediate Splash Launch
    splash = PremiumSplash()
    splash.show()
    
    # 2. Sequential Phased Initialization
    splash.update_msg("Enterprise Fonts & Asset Mapping...", 20)
    setup_fonts()
    time.sleep(0.5) # Smooth visual feedback
    
    splash.update_msg("Jupyter Lab Kernel Orchestration Starting...", 45)
    # The server starts in background in DataExplorerApp.__init__, 
    # but we pre-warm it if needed.
    
    splash.update_msg("GUI Parallel Engine Handlers Loading...", 70)
    window = DataExplorerApp()
    
    splash.update_msg("Intelligence Hub Core V7 Online. Ready.", 100)
    QTimer.singleShot(800, lambda: (window.show(), splash.finish(window)))
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
