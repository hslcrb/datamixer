import qdarktheme

class ThemeManager:
    """Modern Theme Manager using qdarktheme."""
    
    @staticmethod
    def get_dark_style():
        return ""

    @staticmethod
    def apply_theme(theme="dark"):
        """Applies theme (dark, light, auto) globally."""
        # Using a sleek blue primary color (#007acc)
        qdarktheme.setup_theme(theme.lower(), custom_colors={"primary": "#007acc"})
