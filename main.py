import sys
import os
import time
from PySide6.QtWidgets import QApplication, QSplashScreen, QProgressBar, QVBoxLayout, QLabel, QWidget
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QPixmap, QFont

from src.utils import setup_fonts
from src.app import DataExplorerApp

class PremiumSplash(QSplashScreen):
    """Premium Enterprise Launcher with Blue Progress Bar."""
    def __init__(self):
        # Use our generated background
        bg_path = "splash_bg.png"
        if not os.path.exists(bg_path):
            pixmap = QPixmap(600, 400); pixmap.fill(Qt.black)
        else:
            pixmap = QPixmap(bg_path).scaled(800, 450, Qt.KeepAspectRatioByExpanding, Qt.SmoothTransformation)
        
        super().__init__(pixmap)
        
        # Add a custom widget for the progress bar overlay
        self.container = QWidget(self)
        self.container.setGeometry(30, self.height() - 80, self.width() - 60, 60)
        self.layout = QVBoxLayout(self.container)
        
        self.status_label = QLabel("Datamixer Enterprise Core Initializing...")
        self.status_label.setStyleSheet("color: white; font-weight: bold; font-family: 'Segoe UI', sans-serif; font-size: 11pt;")
        self.layout.addWidget(self.status_label)
        
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(8)
        # Deep Blue Neon Progress Style
        self.progress.setStyleSheet("""
            QProgressBar {
                border: 1px solid #24283b;
                border-radius: 4px;
                background-color: #1a1b26;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #7aa2f7, stop:1 #bb9af7);
                border-radius: 4px;
            }
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
