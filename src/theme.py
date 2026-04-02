import qdarktheme
from PySide6.QtWidgets import QApplication

class ThemeManager:
    """Modern Theme Manager using qdarktheme."""
    
    @staticmethod
    def get_dark_style():
        return ""

    @staticmethod
    def apply_theme(theme="dark"):
        """Applies theme (dark, light, auto) globally."""
        app = QApplication.instance()
        if not app: return
            
        t = theme.lower()
        if t not in ["dark", "light"]: t = "dark"
            
        try:
            if hasattr(qdarktheme, "setup_theme"):
                qdarktheme.setup_theme(t, custom_colors={"primary": "#7aa2f7"})
            else:
                app.setStyleSheet(qdarktheme.load_stylesheet(t))
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
