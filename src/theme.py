import qdarktheme
from PySide6.QtWidgets import QApplication

class ThemeManager:
    """Modern Theme Manager with CSS Variable support (Tokyo Night Design System)."""
    
    @staticmethod
    def get_theme_vars(mode="dark"):
        """Defines the core Tokyo Night palettes using CSS variable tokens."""
        if mode == "light":
            return """
            :root {
                --bg: #f0f1f4;
                --fg: #3760bf;
                --card-bg: #e1e2e7;
                --accent: #2e7de9;
                --accent-fg: #ffffff;
                --secondary: #9854f1;
                --border: #d5d6db;
                --hover: #cfd0d7;
                --muted: #8c8fa1;
                --error: #f7768e;
            }
            """
        else: # dark
            return """
            :root {
                --bg: #1a1b26;
                --fg: #c0caf5;
                --card-bg: #24283b;
                --accent: #7aa2f7;
                --accent-fg: #1a1b26;
                --secondary: #bb9af7;
                --border: #414868;
                --hover: #3b4261;
                --muted: #565f89;
                --error: #f7768e;
            }
            """

    @staticmethod
    def get_premium_qss():
        """Returns extra Tokyo Night CSS using variables for truly high-end feel."""
        return """
            /* Base Window and Dialogs */
            QMainWindow, QDialog { background-color: var(--bg); color: var(--fg); }
            
            /* Docks and Tabs */
            QDockWidget::title {
                background: var(--bg); padding: 12px; font-weight: bold; font-size: 10pt; color: var(--accent); border-bottom: 2px solid var(--card-bg);
            }
            QTabWidget::pane { border: 1px solid var(--border); background-color: var(--bg); border-radius: 8px; }
            QTabBar::tab {
                background: var(--card-bg); color: var(--fg); padding: 12px 24px; border-top-left-radius: 8px; border-top-right-radius: 8px; margin: 2px;
            }
            QTabBar::tab:selected { background: var(--accent); color: var(--accent-fg); font-weight: bold; }
            
            /* Headers and Tables */
            QHeaderView::section { background-color: var(--bg); color: var(--accent); padding: 8px; border: 1px solid var(--border); font-weight: bold; }
            QTreeView, QTableView { 
                background-color: var(--bg); border: none; gridline-color: var(--border); selection-background-color: var(--accent); 
                selection-color: var(--accent-fg); alternate-background-color: var(--card-bg); border-radius: 8px; 
            }
            
            /* Inputs and Buttons */
            QGroupBox { border: 2px solid var(--border); border-radius: 12px; margin-top: 20px; font-weight: bold; padding: 20px; color: var(--accent); }
            QGroupBox::title { subcontrol-origin: margin; left: 15px; padding: 0 10px; }
            
            QPushButton { background-color: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 20px; color: var(--accent); font-weight: bold; }
            QPushButton:hover { background-color: var(--accent); color: var(--accent-fg); border: 1px solid var(--accent); }
            QPushButton:pressed { background-color: var(--hover); }
            
            QLineEdit, QComboBox, QSpinBox, QTextEdit { 
                background-color: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px; color: var(--fg); 
            }
            QLineEdit:focus, QComboBox:focus { border: 1.5px solid var(--accent); }
            QComboBox::drop-down { border-left: 1px solid var(--border); width: 30px; }
            
            /* Menus and Toolbars */
            QMenuBar { background-color: var(--bg); padding: 5px; border-bottom: 1px solid var(--border); }
            QMenuBar::item:selected { background-color: var(--card-bg); border-radius: 4px; color: var(--accent); }
            QMenu { background-color: var(--bg); border: 1.5px solid var(--border); padding: 8px; border-radius: 8px; }
            QMenu::item:selected { background-color: var(--accent); color: var(--accent-fg); border-radius: 4px; }
            
            /* Toolbars */
            QToolBar { background-color: var(--bg); border-bottom: 1px solid var(--border); padding: 8px; spacing: 12px; }
            
            /* Status and Progress */
            QStatusBar { background-color: var(--bg); color: var(--muted); border-top: 1.5px solid var(--border); padding: 5px; }
            QProgressBar { border: 1.5px solid var(--border); border-radius: 6px; text-align: center; color: var(--fg); font-weight: bold; }
            QProgressBar::chunk { background-color: var(--accent); border-radius: 4px; margin: 1px; }
            
            /* Scrollbars Premium Styling */
            QScrollBar:vertical { background: var(--bg); width: 12px; margin: 0px; }
            QScrollBar::handle:vertical { background: var(--border); min-height: 25px; border-radius: 6px; margin: 2px; }
            QScrollBar::handle:vertical:hover { background: var(--accent); }
            QScrollBar:horizontal { background: var(--bg); height: 12px; margin: 0px; }
            QScrollBar::handle:horizontal { background: var(--border); min-width: 25px; border-radius: 6px; margin: 2px; }
            QScrollBar::handle:horizontal:hover { background: var(--accent); }
            QScrollBar::add-line, QScrollBar::sub-line { background: none; border: none; }
        """

    @staticmethod
    def apply_theme(theme="dark"):
        """Applies theme (dark, light, auto) globally with premium variable-based overlays."""
        app = QApplication.instance()
        if not app: return
            
        t = theme.lower()
        if t not in ["dark", "light"]: t = "dark"
            
        try:
            # First apply qdarktheme base for widget shapes/logic
            qdarktheme.setup_theme(t, custom_colors={"primary": "#7aa2f7"} if t=="dark" else {"primary": "#2e7de9"})
            
            # Layer custom Design System onto it
            vars_qss = ThemeManager.get_theme_vars(t)
            premium_qss = ThemeManager.get_premium_qss()
            
            # Set global stylesheet with Variables + Core Styles
            app.setStyleSheet(vars_qss + premium_qss)
                
        except Exception as e:
            print(f"Failed to apply theme: {e}")

    @staticmethod
    def apply_font_size(size):
        app = QApplication.instance()
        if not app: return
        font = app.font()
        font.setPointSize(size)
        app.setFont(font)
