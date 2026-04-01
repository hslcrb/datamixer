import sys
from PySide6.QtWidgets import QApplication
from src.utils import setup_fonts
from src.app import DataExplorerApp

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Initialize fonts and common settings
    setup_fonts()
    
    # Start the main application
    window = DataExplorerApp()
    window.show()
    
    sys.exit(app.exec())
