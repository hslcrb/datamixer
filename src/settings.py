from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QCheckBox, QGroupBox, QFormLayout,
    QComboBox, QSpinBox
)
from PySide6.QtCore import Signal
import darkdetect
from .theme import ThemeManager

class SettingsDialog(QDialog):
    """Premium Settings Popup with Real-time Updates and Auto-Theme."""
    font_changed = Signal(int)

    def __init__(self, parent=None, current_settings=None):
        super().__init__(parent)
        self.setWindowTitle("시스템 핵심 환경 설정 (Live Sync)")
        self.setFixedSize(450, 420)
        self.settings = current_settings or {
            "compress": True, "theme": "Auto", "font_size": 10, "auto_analysis": True
        }
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 1. Persistence
        pg = QGroupBox("영속성 설정"); pl = QFormLayout(pg); self.cb_compress = QCheckBox("고효율 압축 엔진")
        self.cb_compress.setChecked(self.settings.get("compress", True)); pl.addRow(self.cb_compress); layout.addWidget(pg)

        # 2. Intelligence
        ig = QGroupBox("AI 코어 V7"); il = QFormLayout(ig); self.cb_auto_analysis = QCheckBox("자동 지능형 프로파일링 활성화")
        self.cb_auto_analysis.setChecked(self.settings.get("auto_analysis", True)); il.addRow(self.cb_auto_analysis); layout.addWidget(ig)

        # 3. UI Sync Control
        ug = QGroupBox("UI 테마 및 폰트 (동기화)"); ul = QFormLayout(ug)
        self.spin_font = QSpinBox(); self.spin_font.setRange(8, 24); self.spin_font.setValue(self.settings.get("font_size", 10))
        self.spin_font.valueChanged.connect(lambda v: ThemeManager.apply_font_size(v)) 
        ul.addRow("시스템 폰트 크기:", self.spin_font)
        
        self.combo_theme = QComboBox(); self.combo_theme.addItems(["Auto (시스템 설정)", "Dark", "Light"])
        st = self.settings.get("theme", "Auto")
        self.combo_theme.setCurrentText("Auto (시스템 설정)" if st == "Auto" else st)
        self.combo_theme.currentTextChanged.connect(self.on_theme_live_update)
        ul.addRow("앱 컬러 테마:", self.combo_theme); layout.addWidget(ug)

        # Buttons
        bl = QHBoxLayout(); self.btn_save = QPushButton("설정 저장 및 싱크"); self.btn_save.clicked.connect(self.accept)
        self.btn_cancel = QPushButton("취소"); self.btn_cancel.clicked.connect(self.reject)
        bl.addStretch(); bl.addWidget(self.btn_save); bl.addWidget(self.btn_cancel); layout.addLayout(bl)

    def on_theme_live_update(self, text):
        t = "Auto" if "Auto" in text else text
        if t == "Auto":
            final = "dark" if darkdetect.isDark() else "light"
        else:
            final = t.lower()
        ThemeManager.apply_theme(final)

    def get_settings(self):
        st = self.combo_theme.currentText()
        t = "Auto" if "Auto" in st else st
        return {
            "compress": self.cb_compress.isChecked(),
            "auto_analysis": self.cb_auto_analysis.isChecked(),
            "theme": t, "font_size": self.spin_font.value()
        }
