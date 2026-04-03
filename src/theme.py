import qdarktheme
from PySide6.QtWidgets import QApplication

class ThemeManager:
    """Modern Theme Manager with Glassmorphic Overlays (Tokyo Night High-End)."""
    
    @staticmethod
    def get_premium_qss():
        """Returns extra Tokyo Night CSS for a truly high-end feel."""
        return """
            /* Base Window and Dialogs */
            QMainWindow, QDialog { background-color: #1a1b26; color: #c0caf5; }
            
            /* Docks and Tabs */
            QDockWidget::title {
                background: #1a1b26; padding: 10px; font-weight: bold; font-size: 10pt; color: #7aa2f7; border-bottom: 2px solid #24283b;
            }
            QTabWidget::pane { border: 1px solid #24283b; background-color: #1a1b26; border-radius: 8px; }
            QTabBar::tab {
                background: #24283b; color: #c0caf5; padding: 10px 20px; border-top-left-radius: 8px; border-top-right-radius: 8px; margin: 2px;
            }
            QTabBar::tab:selected { background: #7aa2f7; color: #1a1b26; font-weight: bold; }
            
            /* Headers and Tables */
            QHeaderView::section { background-color: #1a1b26; color: #7aa2f7; padding: 6px; border: 1px solid #24283b; font-weight: bold; }
            QTreeView, QTableView { 
                background-color: #1a1b26; border: none; gridline-color: #24283b; selection-background-color: #7aa2f7; 
                selection-color: #1a1b26; alternate-background-color: #24283b; border-radius: 8px; 
            }
            
            /* Inputs and Buttons */
            QGroupBox { border: 2px solid #24283b; border-radius: 10px; margin-top: 15px; font-weight: bold; padding: 15px; color: #7aa2f7; }
            QGroupBox::title { subcontrol-origin: margin; left: 15px; padding: 0 5px; }
            
            QPushButton { background-color: #24283b; border: 1px solid #7aa2f7; border-radius: 6px; padding: 8px 16px; color: #7aa2f7; font-weight: bold; }
            QPushButton:hover { background-color: #7aa2f7; color: #1a1b26; }
            
            QLineEdit, QComboBox, QSpinBox, QTextEdit { 
                background-color: #24283b; border: 1px solid #24283b; border-radius: 6px; padding: 8px; color: #c0caf5; 
            }
            QLineEdit:focus, QComboBox:focus { border: 1px solid #7aa2f7; }
            QComboBox::drop-down { border-left: 1px solid #24283b; width: 30px; }
            
            /* Menus and Toolbars */
            QMenuBar { background-color: #1a1b26; padding: 5px; border-bottom: 1px solid #24283b; }
            QMenuBar::item:selected { background-color: #24283b; border-radius: 4px; color: #7aa2f7; }
            QMenu { background-color: #1a1b26; border: 1px solid #24283b; padding: 5px; }
            QMenu::item:selected { background-color: #7aa2f7; color: #1a1b26; border-radius: 4px; }
            
            /* Toolbars */
            QToolBar { background-color: #1a1b26; border-bottom: 1px solid #24283b; padding: 5px; spacing: 10px; }
            
            /* Status and Progress */
            QStatusBar { background-color: #1a1b26; color: #c0caf5; border-top: 2px solid #24283b; }
            QProgressBar { border: 2px solid #24283b; border-radius: 5px; text-align: center; }
            QProgressBar::chunk { background-color: #7aa2f7; width: 10px; margin: 1px; }
            
            /* Scrollbars Premium Styling (Slim & Modern) */
            QScrollBar:vertical { background: #1a1b26; width: 10px; margin: 0px; }
            QScrollBar::handle:vertical { background: #24283b; min-height: 20px; border-radius: 5px; }
            QScrollBar::handle:vertical:hover { background: #7aa2f7; }
            QScrollBar:horizontal { background: #1a1b26; height: 10px; margin: 0px; }
            QScrollBar::handle:horizontal { background: #24283b; min-width: 20px; border-radius: 5px; }
            QScrollBar::handle:horizontal:hover { background: #7aa2f7; }
            QScrollBar::add-line, QScrollBar::sub-line { background: none; border: none; }
        """

    @staticmethod
    def apply_theme(theme="dark"):
        """Applies theme (dark, light, auto) globally with premium overlays."""
        app = QApplication.instance()
        if not app: return
            
        t = theme.lower()
        if t not in ["dark", "light"]: t = "dark"
            
        try:
            if hasattr(qdarktheme, "setup_theme"):
                qdarktheme.setup_theme(t, custom_colors={"primary": "#7aa2f7"})
            else:
                app.setStyleSheet(qdarktheme.load_stylesheet(t))
            
            # Apply premium V7 overlays always
            current_qss = app.styleSheet()
            app.setStyleSheet(current_qss + ThemeManager.get_premium_qss())
                
        except Exception as e:
            print(f"Failed to apply theme: {e}")

    @staticmethod
    def apply_font_size(size):
        """Updates the global application font size without a full theme reload."""
        app = QApplication.instance()
        if not app: return
        font = app.font()
        font.setPointSize(size)
        app.setFont(font)
