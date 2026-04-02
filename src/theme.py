class ThemeManager:
    """Dark Theme for Datamixer."""
    
    @staticmethod
    def get_dark_style():
        return """
        /* Global Base */
        QMainWindow, QDialog {
            background-color: #1a1b26;
            color: #c0caf5;
        }
        QWidget {
            background-color: #1a1b26;
            color: #c0caf5;
            font-family: 'Segoe UI', 'Pretendard', 'Malgun Gothic';
            font-size: 10pt;
        }
        
        /* Docks and Tabs */
        QDockWidget {
            color: #7aa2f7;
            font-weight: bold;
            titlebar-close-icon: none; 
            titlebar-normal-icon: none;
        }
        QDockWidget::title {
            background: #24283b;
            padding: 8px;
            text-align: left;
            border-bottom: 2px solid #414868;
        }
        QTabWidget::pane {
            border: 1px solid #414868;
            background: #1a1b26;
        }
        QTabBar::tab {
            background: #24283b;
            color: #565f89;
            padding: 10px 20px;
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
            margin-right: 2px;
        }
        QTabBar::tab:selected {
            background: #414868;
            color: #7aa2f7;
            border-bottom: 2px solid #7aa2f7;
        }
        QTabBar::tab:hover {
            color: #c0caf5;
        }

        /* Lists and Tables */
        QTableView {
            background-color: #1a1b26;
            alternate-background-color: #24283b;
            gridline-color: #414868;
            selection-background-color: rgba(122, 162, 247, 80);
            selection-color: #ffffff;
            border: 1px solid #414868;
        }
        QHeaderView::section {
            background-color: #24283b;
            color: #c0caf5;
            padding: 8px;
            border: 1px solid #414868;
            font-weight: bold;
        }
        QTreeWidget {
            background-color: #1a1b26;
            border: none;
            outline: none;
        }
        QTreeWidget::item {
            padding: 5px;
            color: #c0caf5;
        }
        QTreeWidget::item:selected {
            background-color: #414868;
            color: #7aa2f7;
        }

        /* Inputs and Buttons */
        QTextEdit, QPlainTextEdit, QLineEdit {
            background-color: #24283b;
            color: #c0caf5;
            border: 1px solid #414868;
            border-radius: 6px;
            padding: 8px;
            selection-background-color: #7aa2f7;
        }
        QLineEdit:focus {
            border: 1px solid #7aa2f7;
        }
        QPushButton {
            background-color: #414868;
            color: #c0caf5;
            border: 1px solid #565f89;
            border-radius: 6px;
            padding: 10px 18px;
            font-weight: bold;
        }
        QPushButton:hover {
            background-color: #565f89;
            border: 1px solid #7aa2f7;
        }
        QPushButton:pressed {
            background-color: #7aa2f7;
            color: #1a1b26;
        }
        QComboBox {
            background-color: #24283b;
            color: #c0caf5;
            border: 1px solid #414868;
            border-radius: 6px;
            padding: 8px;
            min-width: 100px;
        }
        QComboBox::drop-down {
            border: none;
            width: 20px;
        }
        QComboBox QAbstractItemView {
            background-color: #24283b;
            color: #c0caf5;
            selection-background-color: #414868;
        }

        /* Group boxes */
        QGroupBox {
            border: 2px solid #414868;
            border-radius: 10px;
            margin-top: 15px;
            font-weight: bold;
            color: #7aa2f7;
            padding: 10px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 20px;
            padding: 0 5px;
        }

        /* Scrollbars (Ultra Sleek) */
        QScrollBar:vertical {
            border: none;
            background: #1a1b26;
            width: 10px;
            margin: 0px;
        }
        QScrollBar::handle:vertical {
            background: #414868;
            min-height: 20px;
            border-radius: 5px;
        }
        QScrollBar::handle:vertical:hover {
            background: #7aa2f7;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            border: none;
            background: none;
        }
        
        QScrollBar:horizontal {
            border: none;
            background: #1a1b26;
            height: 10px;
            margin: 0px;
        }
        QScrollBar::handle:horizontal {
            background: #414868;
            min-width: 20px;
            border-radius: 5px;
        }

        /* Toolbar and Status */
        QToolBar {
            background-color: #1a1b26;
            border-bottom: 2px solid #24283b;
            spacing: 15px;
            padding: 8px;
        }
        QStatusBar {
            background-color: #24283b;
            color: #565f89;
            font-size: 8pt;
        }
        QMenuBar {
            background-color: #1a1b26;
            color: #c0caf5;
            border-bottom: 1px solid #24283b;
        }
        QMenuBar::item:selected {
            background-color: #414868;
            color: #7aa2f7;
        }
        QMenu {
            background-color: #24283b;
            color: #c0caf5;
            border: 1px solid #414868;
        }
        QMenu::item:selected {
            background-color: #414868;
            color: #7aa2f7;
        }
        """
