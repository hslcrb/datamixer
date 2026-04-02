from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QCheckBox, QGroupBox, QFormLayout,
    QComboBox, QSpinBox
)
from PySide6.QtCore import Signal
from .theme import ThemeManager

class SettingsDialog(QDialog):
    """Premium Settings Popup with Real-time Updates."""
    # Define signals for real-time updates
    font_changed = Signal(int)

    def __init__(self, parent=None, current_settings=None):
        super().__init__(parent)
        self.setWindowTitle("시스템 환경 설정 (Live)")
        self.setFixedSize(450, 420)
        self.settings = current_settings or {
            "compress": True, "theme": "Dark", "font_size": 10, "auto_analysis": True
        }
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 1. Persistence Group
        persist_group = QGroupBox("프로젝트 영속성 패키징 (.dmx)")
        persist_layout = QFormLayout(persist_group)
        self.cb_compress = QCheckBox("고성능 ZIP 압축 사용 (권장)")
        self.cb_compress.setChecked(self.settings.get("compress", True))
        persist_layout.addRow(self.cb_compress)
        layout.addWidget(persist_group)

        # 2. Intelligence Group
        intel_group = QGroupBox("지능형 분석 코어 V7 (Expert AI)")
        intel_layout = QFormLayout(intel_group)
        self.cb_auto_analysis = QCheckBox("데이터 로드 시 즉각적인 AI 패턴 분석 실행")
        self.cb_auto_analysis.setChecked(self.settings.get("auto_analysis", True))
        intel_layout.addRow(self.cb_auto_analysis)
        layout.addWidget(intel_group)

        # 3. UI Real-time Control Group
        ui_group = QGroupBox("사용자 인터페이스 및 폰트 (Live Update)")
        ui_layout = QFormLayout(ui_group)
        
        self.spin_font = QSpinBox()
        self.spin_font.setRange(8, 24)
        self.spin_font.setValue(self.settings.get("font_size", 10))
        # LIVE CONNECT!
        self.spin_font.valueChanged.connect(self.on_font_live_update)
        ui_layout.addRow("시스템 폰트 크기 조정:", self.spin_font)
        
        self.combo_theme = QComboBox()
        self.combo_theme.addItems(["Dark", "Light"])
        self.combo_theme.setCurrentText(self.settings.get("theme", "Dark"))
        self.combo_theme.currentTextChanged.connect(self.on_theme_live_update)
        ui_layout.addRow("전역 테마 전환:", self.combo_theme)
        
        layout.addWidget(ui_group)

        # 4. Final Control Buttons
        btn_layout = QHBoxLayout()
        self.btn_save = QPushButton("설정 저장 및 종료")
        self.btn_save.clicked.connect(self.accept)
        self.btn_cancel = QPushButton("취소")
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.btn_cancel)
        layout.addLayout(btn_layout)

    def on_font_live_update(self, val):
        """Immediately scale the application font."""
        ThemeManager.apply_font_size(val)
        self.font_changed.emit(val)

    def on_theme_live_update(self, theme):
        """Immediately switch theme."""
        ThemeManager.apply_theme(theme)

    def get_settings(self):
        return {
            "compress": self.cb_compress.isChecked(),
            "auto_analysis": self.cb_auto_analysis.isChecked(),
            "theme": self.combo_theme.currentText(),
            "font_size": self.spin_font.value()
        }
