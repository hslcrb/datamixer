class ThemeManager:
    """Enterprise Dark Mode Stylesheet for Datamixer."""
    
    @staticmethod
    def get_dark_style():
        return """
        QMainWindow {
            background-color: #1a1b26;
        }
        QWidget {
            background-color: #1a1b26;
            color: #a9b1d6;
            font-family: 'Segoe UI', 'Malgun Gothic';
        }
        QDockWidget {
            titlebar-close-icon: url(close.png);
            titlebar-normal-icon: url(undock.png);
            color: #7aa2f7;
            font-weight: bold;
            border: 1px solid #24283b;
        }
        QDockWidget::title {
            background: #24283b;
            padding: 5px;
            text-align: left;
        }
        QTabWidget::pane {
            border: 1px solid #24283b;
            background: #1a1b26;
        }
        QTabBar::tab {
            background: #24283b;
            color: #a9b1d6;
            padding: 8px 15px;
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
        }
        QTabBar::tab:selected {
            background: #414868;
            color: #7aa2f7;
            border-bottom: 2px solid #7aa2f7;
        }
        QTableView {
            background-color: #24283b;
            gridline-color: #414868;
            selection-background-color: rgba(122, 162, 247, 60);
            selection-color: #ffffff;
            border: none;
        }
        QHeaderView::section {
            background-color: #414868;
            color: #c0caf5;
            padding: 5px;
            border: 1px solid #24283b;
        }
        QTextEdit, QPlainTextEdit, QLineEdit {
            background-color: #1a1b26;
            color: #c0caf5;
            border: 1px solid #414868;
            border-radius: 4px;
            padding: 5px;
        }
        QTreeWidget {
            background-color: #24283b;
            border: none;
            color: #a9b1d6;
        }
        QTreeWidget::item:selected {
            background-color: #414868;
            color: #7aa2f7;
        }
        QPushButton {
            background-color: #414868;
            color: #c0caf5;
            border: 1px solid #565f89;
            border-radius: 4px;
            padding: 8px 15px;
        }
        QPushButton:hover {
            background-color: #565f89;
        }
        QPushButton:pressed {
            background-color: #7aa2f7;
            color: #1a1b26;
        }
        QComboBox {
            background-color: #24283b;
            color: #c0caf5;
            border: 1px solid #414868;
            border-radius: 4px;
            padding: 5px;
        }
        QComboBox::drop-down {
            border: none;
        }
        QGroupBox {
            border: 1px solid #414868;
            border-radius: 6px;
            margin-top: 10px;
            font-weight: bold;
            color: #7aa2f7;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 3px;
        }
        QStatusBar {
            background-color: #24283b;
            color: #565f89;
        }
        QToolBar {
            background-color: #1a1b26;
            border-bottom: 1px solid #24283b;
            spacing: 10px;
            padding: 5px;
        }
        """
