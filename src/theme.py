import qdarktheme
from PySide6.QtWidgets import QApplication

class ThemeManager:
    """Modern Theme Manager with Glassmorphic Overlays (Tokyo Night)."""
    
    @staticmethod
    def get_premium_qss():
        """Returns extra Tokyo Night CSS for a truly high-end feel."""
        return """
            QMainWindow { background-color: #1a1b26; }
            QDockWidget::title {
                background: #1a1b26; padding: 10px; font-weight: bold; font-size: 10pt; color: #7aa2f7; border-bottom: 2px solid #24283b;
            }
            QTabWidget::pane { border: 1px solid #24283b; background-color: #1a1b26; border-radius: 8px; }
            QTabBar::tab {
                background: #24283b; color: #c0caf5; padding: 10px 20px; border-top-left-radius: 8px; border-top-right-radius: 8px; margin: 2px;
            }
            QTabBar::tab:selected { background: #7aa2f7; color: #1a1b26; font-weight: bold; }
            QHeaderView::section { background-color: #1a1b26; color: #7aa2f7; padding: 6px; border: 1px solid #24283b; font-weight: bold; }
            QTreeView { background-color: #1a1b26; border: none; alternate-background-color: #24283b; selection-background-color: #7aa2f7; selection-color: #1a1b26; }
            QTableView { background-color: #1a1b26; gridline-color: #24283b; selection-background-color: #7aa2f7; selection-color: #1a1b26; alternate-background-color: #24283b; border-radius: 8px; }
            QGroupBox { border: 2px solid #24283b; border-radius: 10px; margin-top: 15px; font-weight: bold; padding: 15px; color: #7aa2f7; }
            QGroupBox::title { subcontrol-origin: margin; left: 15px; padding: 0 5px; }
            QPushButton { background-color: #24283b; border: 1px solid #7aa2f7; border-radius: 6px; padding: 8px 16px; color: #7aa2f7; font-weight: bold; }
            QPushButton:hover { background-color: #7aa2f7; color: #1a1b26; }
            QLineEdit { background-color: #24283b; border: 1px solid #24283b; border-radius: 6px; padding: 8px; color: #c0caf5; }
            QLineEdit:focus { border: 1px solid #7aa2f7; }
            QStatusBar { background-color: #1a1b26; color: #c0caf5; border-top: 2px solid #24283b; }
            QProgressBar { border: 2px solid #24283b; border-radius: 5px; text-align: center; }
            QProgressBar::chunk { background-color: #7aa2f7; width: 10px; margin: 1px; }
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
