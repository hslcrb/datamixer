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
            QMainWindow, QDialog { background-color: var(--bg); color: var(--fg); }
            QDockWidget::title {
                background: var(--bg); padding: 12px; font-weight: bold; font-size: 10pt; color: var(--accent); border-bottom: 2px solid var(--card-bg);
            }
            QTabWidget::pane { border: 1px solid var(--border); background-color: var(--bg); border-radius: 8px; }
            QTabBar::tab {
                background: var(--card-bg); color: var(--fg); padding: 12px 24px; border-top-left-radius: 8px; border-top-right-radius: 8px; margin: 2px;
            }
            QTabBar::tab:selected { background: var(--accent); color: var(--accent-fg); font-weight: bold; }
            QHeaderView::section { background-color: var(--bg); color: var(--accent); padding: 8px; border: 1px solid var(--border); font-weight: bold; }
            QTreeView, QTableView { 
                background-color: var(--bg); border: none; gridline-color: var(--border); selection-background-color: var(--accent); 
                selection-color: var(--accent-fg); alternate-background-color: var(--card-bg); border-radius: 8px; 
            }
            QGroupBox { border: 2px solid var(--border); border-radius: 12px; margin-top: 20px; font-weight: bold; padding: 20px; color: var(--accent); }
            QGroupBox::title { subcontrol-origin: margin; left: 15px; padding: 0 10px; }
            QPushButton { background-color: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 20px; color: var(--accent); font-weight: bold; }
            QPushButton:hover { background-color: var(--accent); color: var(--accent-fg); border: 1px solid var(--accent); }
            QLineEdit, QComboBox, QSpinBox, QTextEdit { 
                background-color: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px; color: var(--fg); 
            }
            QMenuBar { background-color: var(--bg); padding: 5px; border-bottom: 1px solid var(--border); }
            QMenu { background-color: var(--bg); border: 1.5px solid var(--border); padding: 8px; border-radius: 8px; }
            QToolBar { background-color: var(--bg); border-bottom: 1px solid var(--border); padding: 8px; spacing: 12px; }
            QStatusBar { background-color: var(--bg); color: var(--muted); border-top: 1.5px solid var(--border); padding: 5px; }
            QProgressBar { border: 1.5px solid var(--border); border-radius: 6px; text-align: center; color: var(--fg); font-weight: bold; }
            QProgressBar::chunk { background-color: var(--accent); border-radius: 4px; margin: 1px; }
            QScrollBar:vertical, QScrollBar:horizontal { background: var(--bg); width: 12px; height: 12px; margin: 0px; }
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal { background: var(--border); min-height: 25px; min-width: 25px; border-radius: 6px; margin: 2px; }
            QScrollBar::handle:hover { background: var(--accent); }
        """

    @staticmethod
    def get_colors(mode="dark"):
        if mode == "light":
            return {"bg": "#f0f1f4", "fg": "#3760bf", "card-bg": "#e1e2e7", "accent": "#2e7de9", "secondary": "#9854f1"}
        return {"bg": "#1a1b26", "fg": "#c0caf5", "card-bg": "#24283b", "accent": "#7aa2f7", "secondary": "#bb9af7"}

    @staticmethod
    def apply_theme(theme="dark"):
        app = QApplication.instance()
        if not app: return
        t = theme.lower()
        if t not in ["dark", "light"]: t = "dark"
        try:
            # Use safe attribute check for setup_theme
            if hasattr(qdarktheme, "setup_theme"):
                qdarktheme.setup_theme(t, custom_colors={"primary": "#7aa2f7"} if t=="dark" else {"primary": "#2e7de9"})
            else:
                app.setStyleSheet(qdarktheme.load_stylesheet(t))
            
            vars_qss = ThemeManager.get_theme_vars(t)
            premium_qss = ThemeManager.get_premium_qss()
            app.setStyleSheet(vars_qss + premium_qss)
        except Exception as e:
            print(f"Failed to apply theme: {e}")
