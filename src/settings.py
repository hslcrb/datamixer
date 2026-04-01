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
        self.setFixedSize(450, 420)
        self.settings = current_settings or {
            "compress": True,
            "theme": "Light",
            "font_size": 10,
            "default_engine": "Polars",
            "auto_analysis": False
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
        layout.addWidget(persist_group)

        # Intelligence Group
        intel_group = QGroupBox("지능형 분석 설정 (AI)")
        intel_layout = QFormLayout(intel_group)
        self.cb_auto_analysis = QCheckBox("데이터 로드 시 자동 지능형 분석 수행")
        self.cb_auto_analysis.setChecked(self.settings.get("auto_analysis", False))
        intel_layout.addRow(self.cb_auto_analysis)
        intel_layout.addRow(QLabel("활성 시 시스템이 최적의 시각화 파라미터를 자동 설정합니다."))
        layout.addWidget(intel_group)

        # Appearance Group
        app_group = QGroupBox("인터페이스")
        app_layout = QFormLayout(app_group)
        self.combo_theme = QComboBox()
        self.combo_theme.addItems(["Light", "Dark"])
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
            "auto_analysis": self.cb_auto_analysis.isChecked(),
            "theme": self.combo_theme.currentText(),
            "font_size": self.spin_font.value(),
            "default_engine": self.settings.get("default_engine", "Polars")
        }
