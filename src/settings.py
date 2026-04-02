from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QCheckBox, QGroupBox, QFormLayout,
    QComboBox, QSpinBox
)

class SettingsDialog(QDialog):
    """Settings Popup."""
    def __init__(self, parent=None, current_settings=None):
        super().__init__(parent)
        self.setWindowTitle("환경 설정")
        self.setFixedSize(450, 300)
        self.settings = current_settings or {
            "compress": True,
            "theme": "Dark",
            "font_size": 10
        }
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Persistence Group
        persist_group = QGroupBox("저장 설정")
        persist_layout = QFormLayout(persist_group)
        self.cb_compress = QCheckBox("ZIP 압축 저장 사용")
        self.cb_compress.setChecked(self.settings.get("compress", True))
        persist_layout.addRow(self.cb_compress)
        layout.addWidget(persist_group)

        layout.addWidget(persist_group)

        # Interface Group
        app_group = QGroupBox("사용자 인터페이스")
        app_layout = QFormLayout(app_group)
        self.spin_font = QSpinBox()
        self.spin_font.setRange(8, 20)
        self.spin_font.setValue(self.settings.get("font_size", 10))
        app_layout.addRow("시스템 전역 폰트 크기:", self.spin_font)
        self.combo_theme = QComboBox()
        self.combo_theme.addItems(["Dark", "Light", "Auto"])
        self.combo_theme.setCurrentText(self.settings.get("theme", "Dark"))
        app_layout.addRow("테마:", self.combo_theme)
        layout.addWidget(app_group)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_save = QPushButton("설정 적용")
        self.btn_save.clicked.connect(self.accept)
        self.btn_cancel = QPushButton("취소")
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.btn_cancel)
        layout.addLayout(btn_layout)

    def get_settings(self):
        return {
            "compress": self.cb_compress.isChecked(),
            "theme": self.combo_theme.currentText(),
            "font_size": self.spin_font.value()
        }
