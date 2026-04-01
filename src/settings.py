from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QCheckBox, QGroupBox, QFormLayout,
    QComboBox, QSpinBox
)

class SettingsDialog(QDialog):
    """Professional Settings Popup for Enterprise Edition."""
    def __init__(self, parent=None, current_settings=None):
        super().__init__(parent)
        self.setWindowTitle("시스템 환경 설정")
        self.setFixedSize(400, 350)
        self.settings = current_settings or {
            "compress": True,
            "theme": "Light",
            "font_size": 10,
            "default_engine": "Polars"
        }
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Persistence Group
        persist_group = QGroupBox("프로젝트 저장 설정 (.dmx)")
        persist_layout = QFormLayout(persist_group)
        self.cb_compress = QCheckBox("DMX 전용 패키지 압축 사용 (권장)")
        self.cb_compress.setChecked(self.settings.get("compress", True))
        persist_layout.addRow(self.cb_compress)
        persist_layout.addRow(QLabel("해제 시 단일 XML 파일로 저장되며 시스템 로드가 증가할 수 있습니다."))
        layout.addWidget(persist_group)

        # Appearance Group
        app_group = QGroupBox("인터페이스")
        app_layout = QFormLayout(app_group)
        self.combo_theme = QComboBox()
        self.combo_theme.addItems(["Light", "Dark (Coming Soon)"])
        self.combo_theme.setCurrentText(self.settings.get("theme", "Light"))
        app_layout.addRow("테마:", self.combo_theme)
        
        self.spin_font = QSpinBox()
        self.spin_font.setRange(8, 20)
        self.spin_font.setValue(self.settings.get("font_size", 10))
        app_layout.addRow("전역 폰트 크기:", self.spin_font)
        layout.addWidget(app_group)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_save = QPushButton("설정 저장")
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
            "font_size": self.spin_font.value(),
            "default_engine": self.settings.get("default_engine", "Polars")
        }
