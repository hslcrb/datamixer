from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QCheckBox, QGroupBox, QFormLayout,
    QComboBox, QSpinBox
)

class SettingsDialog(QDialog):
    """Premium Settings Popup - Strictly Dark Mode."""
    def __init__(self, parent=None, current_settings=None):
        super().__init__(parent)
        self.setWindowTitle("시스템 핵심 환경 설정")
        self.setFixedSize(450, 380)
        self.settings = current_settings or {
            "compress": True,
            "theme": "Dark",
            "font_size": 10,
            "default_engine": "Polars",
            "auto_analysis": True
        }
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Persistence Group
        persist_group = QGroupBox("프로젝트 영속성 제어 (.dmx)")
        persist_layout = QFormLayout(persist_group)
        self.cb_compress = QCheckBox("고성능 ZIP 압축 패키징 사용 (권장)")
        self.cb_compress.setChecked(self.settings.get("compress", True))
        persist_layout.addRow(self.cb_compress)
        layout.addWidget(persist_group)

        # Intelligence Group
        intel_group = QGroupBox("지능형 분석 코어 (Expert System)")
        intel_layout = QFormLayout(intel_group)
        self.cb_auto_analysis = QCheckBox("데이터 로드 시 즉각적인 AI 패턴 분석 실행")
        self.cb_auto_analysis.setChecked(self.settings.get("auto_analysis", True))
        intel_layout.addRow(self.cb_auto_analysis)
        intel_layout.addRow(QLabel("활성화 시 왜도, 결측치, 이상치를 자동으로 탐지합니다."))
        layout.addWidget(intel_group)

        # Interface Group
        app_group = QGroupBox("사용자 인터페이스 (Strict Dark)")
        app_layout = QFormLayout(app_group)
        self.spin_font = QSpinBox()
        self.spin_font.setRange(8, 20)
        self.spin_font.setValue(self.settings.get("font_size", 10))
        app_layout.addRow("시스템 전역 폰트 크기:", self.spin_font)
        app_layout.addRow(QLabel("테마: Ultra Dark (Tokyo Night Storm) 고정"))
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
            "auto_analysis": self.cb_auto_analysis.isChecked(),
            "theme": "Dark",
            "font_size": self.spin_font.value(),
            "default_engine": self.settings.get("default_engine", "Polars")
        }
