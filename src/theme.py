import qdarktheme
import darkdetect
from PySide6.QtWidgets import QApplication


class ThemeManager:
    """Tokyo Night Design System — QSS with actual hex values (Qt CSS variable workaround)."""

    DARK_COLORS = {
        "bg": "#1a1b26",
        "fg": "#c0caf5",
        "card-bg": "#24283b",
        "accent": "#7aa2f7",
        "accent-fg": "#1a1b26",
        "secondary": "#bb9af7",
        "border": "#414868",
        "hover": "#3b4261",
        "muted": "#565f89",
        "error": "#f7768e",
    }

    LIGHT_COLORS = {
        "bg": "#f0f1f4",
        "fg": "#3760bf",
        "card-bg": "#e1e2e7",
        "accent": "#2e7de9",
        "accent-fg": "#ffffff",
        "secondary": "#9854f1",
        "border": "#d5d6db",
        "hover": "#cfd0d7",
        "muted": "#8c8fa1",
        "error": "#f7768e",
    }

    @staticmethod
    def get_colors(mode="dark"):
        """Returns complete Tokyo Night color palette dict for the given mode."""
        if isinstance(mode, str) and mode.lower() == "light":
            return ThemeManager.LIGHT_COLORS
        return ThemeManager.DARK_COLORS

    @staticmethod
    def get_premium_qss(colors):
        """Builds full QSS stylesheet with actual hex values — no CSS variables."""
        bg        = colors["bg"]
        fg        = colors["fg"]
        card_bg   = colors["card-bg"]
        accent    = colors["accent"]
        accent_fg = colors["accent-fg"]
        secondary = colors["secondary"]
        border    = colors["border"]
        muted     = colors["muted"]
        error     = colors["error"]

        return f"""
            QMainWindow, QWidget  {{ background-color: {bg}; color: {fg}; }}
            QDialog               {{ background-color: {bg}; color: {fg}; }}
            QDockWidget::title    {{ background: {bg}; padding: 12px; font-weight: bold;
                                     font-size: 10pt; color: {accent}; border-bottom: 2px solid {card_bg}; }}
            QTabWidget::pane      {{ border: 1px solid {border}; background-color: {bg}; border-radius: 8px; }}
            QTabBar::tab          {{ background: {card_bg}; color: {fg}; padding: 12px 24px;
                                     border-top-left-radius: 8px; border-top-right-radius: 8px; margin: 2px; }}
            QTabBar::tab:selected {{ background: {accent}; color: {accent_fg}; font-weight: bold; }}
            QHeaderView::section  {{ background-color: {bg}; color: {accent}; padding: 8px;
                                     border: 1px solid {border}; font-weight: bold; }}
            QTreeView, QTableView {{ background-color: {bg}; border: none; gridline-color: {border};
                                     selection-background-color: {accent}; selection-color: {accent_fg};
                                     alternate-background-color: {card_bg}; border-radius: 8px; }}
            QGroupBox             {{ border: 2px solid {border}; border-radius: 12px; margin-top: 20px;
                                     font-weight: bold; padding: 20px; color: {accent}; }}
            QGroupBox::title      {{ subcontrol-origin: margin; left: 15px; padding: 0 10px; }}
            QPushButton           {{ background-color: {card_bg}; border: 1px solid {border};
                                     border-radius: 8px; padding: 10px 20px; color: {accent}; font-weight: bold; }}
            QPushButton:hover     {{ background-color: {accent}; color: {accent_fg}; border: 1px solid {accent}; }}
            QLineEdit, QComboBox, QSpinBox, QTextEdit
                                  {{ background-color: {card_bg}; border: 1px solid {border};
                                     border-radius: 8px; padding: 10px; color: {fg}; }}
            QMenuBar              {{ background-color: {bg}; padding: 5px; border-bottom: 1px solid {border}; }}
            QMenu                 {{ background-color: {bg}; border: 1.5px solid {border};
                                     padding: 8px; border-radius: 8px; }}
            QToolBar              {{ background-color: {bg}; border-bottom: 1px solid {border};
                                     padding: 8px; spacing: 12px; }}
            QStatusBar            {{ background-color: {bg}; color: {muted}; border-top: 1.5px solid {border};
                                     padding: 5px; }}
            QProgressBar          {{ border: 1.5px solid {border}; border-radius: 6px;
                                     text-align: center; color: {fg}; font-weight: bold; }}
            QProgressBar::chunk   {{ background-color: {accent}; border-radius: 4px; margin: 1px; }}
            QScrollBar:vertical, QScrollBar:horizontal
                                  {{ background: {bg}; width: 12px; height: 12px; margin: 0px; }}
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal
                                  {{ background: {border}; min-height: 25px; min-width: 25px;
                                     border-radius: 6px; margin: 2px; }}
            QScrollBar::handle:hover {{ background: {accent}; }}
            QLabel                {{ color: {fg}; }}
        """

    @staticmethod
    def apply_theme(theme="dark"):
        app = QApplication.instance()
        if not app:
            return
        t = theme.lower()
        if t not in ["dark", "light"]:
            t = "dark" if darkdetect.isDark() else "light"
        try:
            if hasattr(qdarktheme, "setup_theme"):
                qdarktheme.setup_theme(
                    t,
                    custom_colors={"primary": "#7aa2f7"} if t == "dark" else {"primary": "#2e7de9"}
                )
            else:
                app.setStyleSheet(qdarktheme.load_stylesheet(t))

            colors = ThemeManager.get_colors(t)
            app.setStyleSheet(ThemeManager.get_premium_qss(colors))
        except Exception as e:
            print(f"Failed to apply theme: {e}")
