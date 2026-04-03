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
        self.setWindowTitle("시스템 핵심 환경 설정 (Enterprise Sync)")
        # Store original for rollback
        self.original_settings = current_settings.copy() if current_settings else {}
        self.settings = current_settings or {
            "compress": True, "theme": "Auto", "font_size": 10, "auto_analysis": True
        }
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20); layout.setSpacing(15)

        # 1. Core Logic
        pg = QGroupBox("영속성 및 AI 코어"); pl = QFormLayout(pg)
        self.cb_compress = QCheckBox("고효율 압축 엔진 활성화")
        self.cb_auto_analysis = QCheckBox("자동 지능형 프로파일링 (AI V7)")
        self.cb_compress.setChecked(self.settings.get("compress", True))
        self.cb_auto_analysis.setChecked(self.settings.get("auto_analysis", True))
        pl.addRow(self.cb_compress); pl.addRow(self.cb_auto_analysis)
        layout.addWidget(pg)

        # 2. UI Sync Control
        ug = QGroupBox("UI 테마 및 폰트 (실시간 싱크)"); ul = QFormLayout(ug)
        self.spin_font = QSpinBox(); self.spin_font.setRange(8, 24); self.spin_font.setValue(self.settings.get("font_size", 10))
        self.spin_font.valueChanged.connect(lambda v: ThemeManager.apply_font_size(v)) 
        ul.addRow("글로벌 폰트 크기:", self.spin_font)
        
        self.combo_theme = QComboBox(); self.combo_theme.addItems(["Auto (시스템 설정)", "Dark", "Light"])
        st = self.settings.get("theme", "Auto")
        self.combo_theme.setCurrentText("Auto (시스템 설정)" if st == "Auto" else st)
        self.combo_theme.currentTextChanged.connect(self.on_theme_live_update)
        ul.addRow("애플리케이션 테마:", self.combo_theme); layout.addWidget(ug)

        # Buttons with Premium Highlighting
        bl = QHBoxLayout(); 
        self.btn_save = QPushButton("설정 저장 및 싱크"); self.btn_save.setMinimumHeight(40)
        self.btn_save.setStyleSheet("background-color: #7aa2f7; color: #1a1b26; font-weight: bold;")
        self.btn_save.clicked.connect(self.accept)
        
        self.btn_cancel = QPushButton("취소"); self.btn_cancel.setMinimumWidth(80); self.btn_cancel.setMinimumHeight(40)
        self.btn_cancel.clicked.connect(self.on_cancel_and_rollback)
        
        bl.addStretch(); bl.addWidget(self.btn_cancel); bl.addWidget(self.btn_save); layout.addLayout(bl)

    def on_cancel_and_rollback(self):
        """Reverts live changes to original state before closing."""
        orig_theme = self.original_settings.get("theme", "Auto")
        orig_font = self.original_settings.get("font_size", 10)
        self.on_theme_live_update(orig_theme)
        ThemeManager.apply_font_size(orig_font)
        self.reject()

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
