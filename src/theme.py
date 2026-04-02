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
        if not app:
            return
            
        t = theme.lower()
        if t not in ["dark", "light"]:
            t = "dark"  # fallback for 'auto' if not supported simply
            
        try:
            # New setup API (if available)
            if hasattr(qdarktheme, "setup_theme"):
                qdarktheme.setup_theme(t, custom_colors={"primary": "#007acc"})
            else:
                # Older API fallback
                app.setStyleSheet(qdarktheme.load_stylesheet(t))
        except Exception as e:
            print(f"Failed to apply theme: {e}")
