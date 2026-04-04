import sys
import io
import os
import contextlib
import base64
import json
import pandas as pd
import polars as pl
import numpy as np
import plotly.express as px
import webbrowser
import darkdetect
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineProfile, QWebEnginePage
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTableView, QTabWidget, QLabel,
    QLineEdit, QComboBox, QGroupBox, QFormLayout,
    QMessageBox, QHeaderView, QTextEdit, QDockWidget, QTreeWidget,
    QTreeWidgetItem, QToolBar, QStatusBar, QFrame, QProgressBar,
    QGridLayout, QDialog, QToolTip
)
from PySide6.QtCore import Qt, QUrl, QSize, QByteArray, QTimer, Signal
from PySide6.QtGui import QPalette, QColor, QAction, QFont

# Internal imports
from .models import PandasModel
from .utils import detect_encoding_parallel
from .worker import GenericWorker
from .engine import DataEngine
from .session import SessionManager
from .viz_manager import VizManager
from .settings import SettingsDialog
from .repl import JupyterConsoleManager
from .intelligence.core import IntelligenceCore
from .theme import ThemeManager
from .jupyter_manager import JupyterServerManager

class EditableTableView(QTableView):
    """Premium High-End Data Grid with Intelligent Read-Only/Edit Switch."""
    switch_to_edit = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setSelectionBehavior(QTableView.SelectItems)
        self.setAlternatingRowColors(True)
        self.setStyleSheet("QTableView { background-color: var(--bg); gridline-color: var(--border); selection-background-color: var(--accent); selection-color: var(--accent-fg); }")

    def keyPressEvent(self, event):
        m = self.model()
        if not m or not hasattr(m, 'read_only'):
            super().keyPressEvent(event)
            return
        
        if m.read_only and (event.text().isalnum() or event.key() in (Qt.Key_Backspace, Qt.Key_Delete, Qt.Key_Period)):
            pos = self.viewport().mapToGlobal(self.visualRect(self.currentIndex()).center())
            QToolTip.showText(pos, "<b>현재는 읽기 전용입니다.</b><br><br>데이터를 수동으로 수정하려면 상단<br>전환 버튼이나 여기를 눌러 쓰기로 전환하세요.", self)
            self.switch_to_edit.emit()
            return
            
        super().keyPressEvent(event)

class MiniBrowser(QWidget):
    """Integrated Premium Web Browser with Tokyo Night Sync."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        
        profile_path = os.path.join(os.getcwd(), "browser_enterprise_profile")
        if not os.path.exists(profile_path): os.makedirs(profile_path)
        self.profile = QWebEngineProfile("EnterpriseProfile", self)
        self.profile.setPersistentStoragePath(profile_path)
        self.profile.setPersistentCookiesPolicy(QWebEngineProfile.AllowPersistentCookies)
        
        self.nav_layout = QHBoxLayout()
        self.btn_back = QPushButton("◀"); self.btn_forward = QPushButton("▶"); self.btn_reload = QPushButton("↺")
        self.url_bar = QLineEdit(); self.url_bar.setPlaceholderText("검색어나 URL 입력..."); self.url_bar.returnPressed.connect(self.navigate)
        self.btn_ext = QPushButton("🌐"); self.btn_ext.clicked.connect(lambda: webbrowser.open(self.browser.url().toString()))
        
        for btn in [self.btn_back, self.btn_forward, self.btn_reload, self.btn_ext]:
            btn.setFixedWidth(40)
            self.nav_layout.addWidget(btn)
        
        self.nav_layout.addWidget(self.url_bar, 1)
        self.layout.addLayout(self.nav_layout)
        
        self.browser = QWebEngineView()
        self.page = QWebEnginePage(self.profile, self.browser)
        self.browser.setPage(self.page); self.browser.setUrl(QUrl("https://www.acmicpc.net/"))
        self.browser.urlChanged.connect(lambda q: self.url_bar.setText(q.toString()))
        self.layout.addWidget(self.browser)
        
        self.btn_back.clicked.connect(self.browser.back)
        self.btn_forward.clicked.connect(self.browser.forward)
        self.btn_reload.clicked.connect(self.browser.reload)

    def navigate(self):
        u = self.url_bar.text()
        if not ("." in u) or (" " in u): u = f"https://www.acmicpc.net/search#q={u}"
        elif not u.startswith("http"): u = "https://" + u
        self.browser.setUrl(QUrl(u))

class AdvancedOpsDialog(QDialog):
    """지능형 데이터 변환 전용 '원자로 코어 패널' (V7 프리미엄 디자인)."""
    def __init__(self, parent=None, columns=None):
        super().__init__(parent)
        self.setWindowTitle("DATAMIXER NUCLEAR CORE : 초정밀 데이터 변환 센터")
        self.setMinimumSize(950, 650)
        self.columns = columns or []
        self.selected_op = None
        self.selected_params = {}
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        self.setStyleSheet("""
            QDialog { background-color: #1a1b26; color: #c0caf5; }
            QLabel#HeaderTitle { font-size: 24pt; font-weight: 800; color: #7aa2f7; margin-bottom: 5px; }
            QLabel#SubTitle { font-size: 11pt; color: #565f89; margin-bottom: 30px; }
            QFrame[class="Card"] { background-color: #24283b; border-radius: 12px; border: 1px solid #414868; padding: 15px; }
            QFrame[class="Card"]:hover { border: 1px solid #7aa2f7; }
            QLabel[class="CardTitle"] { font-size: 13pt; font-weight: bold; color: #bb9af7; border-bottom: 2px solid #bb9af7; padding-bottom: 8px; margin-bottom: 12px; }
            QLabel[class="CardDesc"] { font-size: 9pt; color: #565f89; margin-bottom: 15px; }
            QPushButton[class="OpBtn"] { background-color: #1a1b26; color: #c0caf5; border: 1px solid #414868; padding: 12px; border-radius: 6px; font-weight: bold; text-align: left; }
            QPushButton[class="OpBtn"]:hover { background-color: #7aa2f7; color: #1a1b26; border: 1px solid #7aa2f7; }
            QComboBox { background-color: #1a1b26; color: #c0caf5; border: 1px solid #414868; padding: 8px; border-radius: 4px; min-height: 30px; }
        """)

        # Header Section
        header = QLabel("NUCLEAR CORE REACTOR"); header.setObjectName("HeaderTitle"); header.setAlignment(Qt.AlignCenter)
        sub = QLabel("데이터의 물리적 구조를 재정의하는 초정밀 변환 엔진 시스템"); sub.setObjectName("SubTitle"); sub.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header); main_layout.addWidget(sub)

        grid = QGridLayout(); grid.setSpacing(25)
        
        # 1. 고정밀 클리닝 (Cleaning)
        c1 = QFrame(); c1.setProperty("class", "Card"); l1 = QVBoxLayout(c1)
        lbl1 = QLabel("🧹 NUCLEAR CLEANING"); lbl1.setProperty("class", "CardTitle"); l1.addWidget(lbl1)
        lbl1_desc = QLabel("데이터 오염 및 결측치를 물리적으로 제거합니다."); lbl1_desc.setProperty("class", "CardDesc"); l1.addWidget(lbl1_desc)
        ops1 = [("⚡ 모든 결측치(NaN) 즉시 제거", "Drop Nulls"), ("🧩 선형 보간 기반 스마트 채우기", "Fill Nulls (Mean)"), ("💎 중복 로우 물리적 제거", "Remove Duplicates")]
        for label, op in ops1:
            btn = QPushButton(f"  {label}"); btn.setProperty("class", "OpBtn"); btn.clicked.connect(lambda _, o=op: self.finalize(o))
            l1.addWidget(btn)
        l1.addStretch(); grid.addWidget(c1, 0, 0)

        # 2. 통계적 정규화 (Scaling)
        c2 = QFrame(); c2.setProperty("class", "Card"); l2 = QVBoxLayout(c2)
        lbl2 = QLabel("⚖️ SCALING & POWER"); lbl2.setProperty("class", "CardTitle"); l2.addWidget(lbl2)
        lbl2_desc = QLabel("대상 칼럼의 수치 범위를 머신러닝 최적 상태로 조정합니다."); lbl2_desc.setProperty("class", "CardDesc"); l2.addWidget(lbl2_desc)
        l2.addWidget(QLabel("변환 대상 칼럼 선택:")); self.combo_target_col = QComboBox(); self.combo_target_col.addItems(self.columns)
        l2.addWidget(self.combo_target_col)
        ops2 = [("📊 표준 점수화 (Z-Score Standard)", "Standardize (Z-Score)"), ("📏 최소-최대 정규화 (Min-Max)", "Normalize (Min-Max)"), ("📈 로그 비대칭 보정 (Log1p)", "Log Transform")]
        for label, op in ops2:
            btn = QPushButton(f"  {label}"); btn.setProperty("class", "OpBtn"); btn.clicked.connect(lambda _, o=op: self.finalize(o))
            l2.addWidget(btn)
        l1.addStretch(); grid.addWidget(c2, 0, 1)

        # 3. 이상치 여과 (Outlier)
        c3 = QFrame(); c3.setProperty("class", "Card"); l3 = QVBoxLayout(c3)
        lbl3 = QLabel("🛡️ OUTLIER SHIELD"); lbl3.setProperty("class", "CardTitle"); l3.addWidget(lbl3)
        lbl3_desc = QLabel("통계적 분포를 벗어난 극단값을 감지하여 여과합니다."); lbl3_desc.setProperty("class", "CardDesc"); l3.addWidget(lbl3_desc)
        ops3 = [("🛸 IQR 기준 이상치 자동 제거 (1.5x)", "IQR Outlier Removal"), ("🧨 극단값 제거 (Percentile Trim)", "IQR Outlier Removal")]
        for label, op in ops3:
            btn = QPushButton(f"  {label}"); btn.setProperty("class", "OpBtn"); btn.clicked.connect(lambda _, o=op: self.finalize(o))
            l3.addWidget(btn)
        l3.addStretch(); grid.addWidget(c3, 1, 0)

        # 4. 기능적 핵융합 (Encoding)
        c4 = QFrame(); c4.setProperty("class", "Card"); l4 = QVBoxLayout(c4)
        lbl4 = QLabel("🔗 FEATURE FUSION"); lbl4.setProperty("class", "CardTitle"); l4.addWidget(lbl4)
        lbl4_desc = QLabel("범주형 데이터를 수치적 벡터로 전환하여 기능을 확장합니다."); lbl4_desc.setProperty("class", "CardDesc"); l4.addWidget(lbl4_desc)
        ops4 = [("🌓 원-핫 인코딩 (One-Hot Dummy)", "One-Hot Encoding"), ("🏷️ 라벨 인코딩 (Label Encoding)", "Label Encoding")]
        for label, op in ops4:
            btn = QPushButton(f"  {label}"); btn.setProperty("class", "OpBtn"); btn.clicked.connect(lambda _, o=op: self.finalize(o))
            l4.addWidget(btn)
        l4.addStretch(); grid.addWidget(c4, 1, 1)

        main_layout.addLayout(grid); main_layout.addStretch()
        
        # Footer
        footer = QLabel("<b>경고: 모든 조작은 원본 데이터의 물리적 구조를 변경하며, 파이썬 코드 트레이스에 실시간 기록됩니다.</b>"); footer.setAlignment(Qt.AlignCenter)
        footer.setStyleSheet("color: #f7768e; font-size: 10pt; margin-top: 25px;")
        main_layout.addWidget(footer)

    def finalize(self, op):
        self.selected_op = op
        self.selected_params = {"column": self.combo_target_col.currentText()}
        self.accept()

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setObjectName("MainWindow")
        self.setWindowTitle("Datamixer Enterprise - Multi-Engine AI Workstation V7")
        self.resize(1700, 1050)
        self.setAcceptDrops(True)
        
        self.df = pd.DataFrame(); self.variables = {"df": self.df}; self.workers = []
        self.app_settings = {"compress": False, "theme": "Auto", "font_size": 10, "auto_analysis": True}
        
        ThemeManager.apply_theme(self.app_settings["theme"])
        
        self.jupyter_console = JupyterConsoleManager(self)
        self.jupyter_server = JupyterServerManager()
        self.jupyter_server.start(self.app_settings["theme"])
        
        self.init_ui()
        self.init_menu_and_toolbar()
        self.status_label.setText("Datamixer Enterprise Hub V7 - All Engines Operational (Pd/Pl/Np)")

    def closeEvent(self, event):
        if hasattr(self, 'jupyter_console'): self.jupyter_console.shutdown()
        if hasattr(self, 'jupyter_server'): self.jupyter_server.stop()
        super().closeEvent(event)

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # Grid Tab
        self.view_tab = QWidget(); v1 = QVBoxLayout(self.view_tab)
        self.grid_header_layout = QHBoxLayout()
        self.lbl_grid_mode = QLabel("MODE: READ-ONLY")
        self.lbl_grid_mode.setStyleSheet("color: #bb9af7; font-weight: bold; margin-left: 10px;")
        self.btn_toggle_edit = QPushButton("쓰기 모드로 전환"); self.btn_toggle_edit.setFixedWidth(150)
        self.btn_toggle_edit.clicked.connect(self.toggle_grid_edit_mode)
        self.grid_header_layout.addWidget(self.lbl_grid_mode); self.grid_header_layout.addStretch()
        self.grid_header_layout.addWidget(self.btn_toggle_edit)
        v1.addLayout(self.grid_header_layout)

        self.table_view = EditableTableView()
        self.table_view.switch_to_edit.connect(self.toggle_grid_edit_mode)
        v1.addWidget(self.table_view); self.central_tabs.addTab(self.view_tab, "지능형 데이터 그리드")
        
        # Viz Tab
        self.viz_tab = QWidget(); v2 = QVBoxLayout(self.viz_tab); self.browser = QWebEngineView()
        # Initial color set, will be updated by apply_theme
        v_colors = ThemeManager.get_colors(self.app_settings["theme"])
        self.browser.page().setBackgroundColor(QColor(v_colors["bg"]))
        self.static_canvas_container = QWidget(); self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        v2.addWidget(self.browser); v2.addWidget(self.static_canvas_container); self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_tab, "분석 리포트")
        
        self.jupyter_tab = QWidget(); v3 = QVBoxLayout(self.jupyter_tab); self.wb_browser = QWebEngineView()
        QTimer.singleShot(1500, lambda: self.wb_browser.setUrl(QUrl(self.jupyter_server.url)))
        v3.addWidget(self.wb_browser); self.central_tabs.addTab(self.jupyter_tab, "주피터 주크벤치")
        self.browser_tab = MiniBrowser(); self.central_tabs.addTab(self.browser_tab, "데이터 브라우저")
        
        self.setup_docks()
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.status_label = QLabel("SYSTEM: HEALTHY"); self.status_bar.addWidget(self.status_label)
        self.server_label = QLabel("● JUPYTER ONLINE"); self.server_label.setStyleSheet("color: #9ece6a; font-weight: bold; margin-right: 15px;")
        self.status_bar.addPermanentWidget(self.server_label)

    def setup_docks(self):
        self.setCorner(Qt.BottomLeftCorner, Qt.BottomDockWidgetArea)
        self.setCorner(Qt.BottomRightCorner, Qt.BottomDockWidgetArea)

        # LEFT
        self.explorer_dock = QDockWidget("변수 매니저 (Multi-Engine)", self)
        self.explorer_dock.setObjectName("ExplorerDock")
        self.explorer_tree = QTreeWidget(); self.explorer_tree.setHeaderLabels(["변수명", "라이브러리/엔진", "메모리 점유"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked); self.explorer_tree.setIndentation(15)
        self.explorer_dock.setWidget(self.explorer_tree); self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)

        self.control_dock = QDockWidget("데이터 워크벤치 설정", self)
        self.control_dock.setObjectName("ControlDock")
        self.setup_props_panel(); self.control_dock.setWidget(self.props_container)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.control_dock)
        
        # RIGHT
        self.insight_dock = QDockWidget("AI Core V7 인사이트 리포트", self)
        self.insight_dock.setObjectName("InsightDock")
        self.insight_output = QTextEdit(); self.insight_output.setReadOnly(True)
        self.insight_dock.setWidget(self.insight_output); self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        self.trace_dock = QDockWidget("실시간 파이썬 코드 트레이스", self)
        self.trace_dock.setObjectName("TraceDock")
        self.trace_view = QTextEdit(); self.trace_view.setReadOnly(True)
        self.trace_view.setStyleSheet("QTextEdit { background-color: #1a1b26; color: #c0caf5; font-family: 'Courier New', monospace; font-size: 11pt; border: none; }")
        self.trace_dock.setWidget(self.trace_view); self.addDockWidget(Qt.RightDockWidgetArea, self.trace_dock)
        self.tabifyDockWidget(self.insight_dock, self.trace_dock)

        # BOTTOM
        self.console_dock = QDockWidget("Jupyter Interactive Workbench Core", self)
        self.console_dock.setObjectName("ConsoleDock")
        self.console_dock.setWidget(self.jupyter_console.widget); self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        QTimer.singleShot(500, lambda: self.resize_docks())

    def resize_docks(self):
        self.resizeDocks([self.explorer_dock], [300], Qt.Horizontal)
        self.resizeDocks([self.explorer_dock, self.control_dock], [400, 400], Qt.Vertical)
        self.resizeDocks([self.console_dock], [400], Qt.Vertical)
        self.resizeDocks([self.insight_dock], [400], Qt.Horizontal)

    def setup_props_panel(self):
        self.props_container = QWidget(); layout = QVBoxLayout(self.props_container)
        eg = QGroupBox("Core 엔진 컨트롤"); el = QFormLayout(eg)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["Polars (최고 성능)", "Pandas (안정적 특성)"])
        el.addRow("핵심 파싱 엔진:", self.combo_engine)
        self.lbl_data_info = QLabel("지능형 맵핑 대기 중..."); self.lbl_data_info.setWordWrap(True)
        el.addRow(self.lbl_data_info); layout.addWidget(eg)

        # ADVANCED OPS HEADER with MORE BUTTON
        tg = QGroupBox(); tl = QVBoxLayout(tg)
        header_layout = QHBoxLayout()
        header_label = QLabel("고급 데이터 전처리")
        header_label.setStyleSheet("font-weight: bold; color: #7aa2f7;")
        self.btn_more_ops = QPushButton("더보기 +")
        self.btn_more_ops.setFixedWidth(80); self.btn_more_ops.setStyleSheet("QPushButton { height: 22px; font-size: 8pt; background-color: #414868; padding: 2px; }")
        self.btn_more_ops.clicked.connect(self.open_advanced_ops)
        header_layout.addWidget(header_label); header_layout.addStretch(); header_layout.addWidget(self.btn_more_ops)
        tl.addLayout(header_layout)

        grid_layout = QHBoxLayout()
        self.btn_drop_null = QPushButton("결측치 제거"); self.btn_mean_fill = QPushButton("평균값 채우기")
        self.btn_unique = QPushButton("중복 제거"); self.btn_sort = QPushButton("칼럼 정렬")
        for btn in [self.btn_drop_null, self.btn_mean_fill, self.btn_unique, self.btn_sort]:
            btn.setStyleSheet("QPushButton { background-color: #24283b; color: #c0caf5; height: 30px; }")
            grid_layout.addWidget(btn)
        tl.addLayout(grid_layout)
        self.btn_drop_null.clicked.connect(lambda: self.apply_transform_async("Drop Nulls"))
        self.btn_mean_fill.clicked.connect(lambda: self.apply_transform_async("Fill Nulls (Mean)"))
        self.btn_unique.clicked.connect(lambda: self.apply_transform_async("Remove Duplicates"))
        self.btn_sort.clicked.connect(lambda: self.apply_transform_async("Sort"))
        layout.addWidget(tg)
        
        vg = QGroupBox("지능형 시각 결과 제어"); vl = QFormLayout(vg)
        self.combo_lib = QComboBox(); self.combo_lib.addItems(["Plotly (인터랙티브)", "Matplotlib (정적 PNG)"])
        vl.addRow("시각화 엔진:", self.combo_lib)
        self.combo_viz = QComboBox(); self.combo_viz.addItems(["히스토그램", "산점도", "박스 플롯", "바이올린 플롯", "선 그래프", "바 차트", "파이 차트", "영역 차트"])
        vl.addRow("그래프 종류:", self.combo_viz); self.combo_x = QComboBox(); self.combo_y = QComboBox()
        vl.addRow("X축 칼럼:", self.combo_x); vl.addRow("Y축 칼럼:", self.combo_y)
        self.btn_exe = QPushButton("데이터 엔진 가동"); self.btn_exe.clicked.connect(self.generate_plot_dispatch)
        self.btn_exe.setStyleSheet("QPushButton { background-color: #7aa2f7; color: #1a1b26; font-weight: bold; height: 40px; border-radius: 5px; }")
        vl.addRow(self.btn_exe); layout.addWidget(vg)
        layout.addStretch()

    def open_advanced_ops(self):
        if self.is_data_empty(): 
            self.status_label.setText("SYSTEM: 데이터 로드 및 맵핑이 선행되어야 변환 엔진이 가동됩니다.")
            return
        d = AdvancedOpsDialog(self, columns=list(self.df.columns))
        if d.exec():
            self.apply_transform_async(d.selected_op, params=d.selected_params)

    def update_code_trace(self, title, code):
        current_text = self.trace_view.toHtml()
        header = f"<b style='color: #7aa2f7;'># {title}</b><br>"
        formatted_code = f"<span style='color: #bb9af7;'>{code.replace(chr(10), '<br>')}</span><br><br>"
        self.trace_view.setHtml(header + formatted_code + current_text)

    def init_menu_and_toolbar(self):
        m = self.menuBar(); f = m.addMenu("파일 (&F)")
        f.addAction("데이터 세트 로딩...").triggered.connect(self.load_data_async)
        f.addSeparator()
        f.addAction("세션 저장하기 (.dmx / .dmxz)").triggered.connect(self.save_session_ui)
        f.addAction("세션 불러오기").triggered.connect(self.load_session_ui)
        f.addSeparator()
        f.addAction("환경 설정...").triggered.connect(self.open_settings)
        f.addAction("종료").triggered.connect(self.close)
        
        lp = m.addMenu("레이아웃 (&L)")
        lp.addAction("터미널 극대화 모드").triggered.connect(lambda: self.apply_layout_preset("Terminal"))
        lp.addAction("데이터 집중 모드").triggered.connect(lambda: self.apply_layout_preset("Data"))
        lp.addAction("전문 분석가(균형) 모드").triggered.connect(lambda: self.apply_layout_preset("Balanced"))
        lp.addAction("사이드바 확장 모드").triggered.connect(lambda: self.apply_layout_preset("Explorer"))

        tb = QToolBar("Main Controls"); self.addToolBar(tb)
        btn_set = QPushButton("시스템 설정"); btn_set.clicked.connect(self.open_settings); tb.addWidget(btn_set)

    def collect_full_ui_state(self):
        return {
            "geometry": self.saveGeometry().toBase64().data().decode(),
            "window_state": self.saveState().toBase64().data().decode(),
            "central_tab_index": self.central_tabs.currentIndex(),
            "engine_selection": self.combo_engine.currentText(),
            "viz_lib": self.combo_lib.currentText(),
            "viz_type": self.combo_viz.currentText(),
            "settings": self.app_settings,
            "status": self.status_label.text()
        }

    def restore_full_ui_state(self, state):
        try:
            if "geometry" in state: self.restoreGeometry(QByteArray.fromBase64(state["geometry"].encode()))
            if "window_state" in state: self.restoreState(QByteArray.fromBase64(state["window_state"].encode()))
            if "central_tab_index" in state: self.central_tabs.setCurrentIndex(int(state["central_tab_index"]))
            if "engine_selection" in state: self.combo_engine.setCurrentText(state["engine_selection"])
            if "viz_lib" in state: self.combo_lib.setCurrentText(state["viz_lib"])
            if "viz_type" in state: self.combo_viz.setCurrentText(state["viz_type"])
            if "settings" in state: self.app_settings.update(state["settings"])
            if "status" in state: self.status_label.setText(state["status"])
        except Exception as e:
            print(f"UI Restore Error: {e}")

    def save_session_ui(self):
        p, filter_selected = QFileDialog.getSaveFileName(self, "세션 저장", "", "JSON 세션 (*.dmx);;압축 세션 (*.dmxz)")
        if not p: return
        is_compressed = ".dmxz" in filter_selected or p.endswith(".dmxz")
        def _task(): return SessionManager.save_project(p, self.variables, self.collect_full_ui_state(), compress=is_compressed)
        def _done(res):
            if res[0]: self.status_label.setText(f"SUCCESS: {res[1]}")
            else: QMessageBox.critical(self, "Error", res[1])
        self.start_worker(_task, on_success=_done)

    def load_session_ui(self):
        p, _ = QFileDialog.getOpenFileName(self, "세션 로드", "", "Datamixer Session (*.dmx *.dmxz)")
        if not p: return
        def _task(): return SessionManager.load_project(p)
        def _done(res):
            if res[0]:
                self.variables = res[1]["variables"]
                if "df" in self.variables: self.df = self.variables["df"]
                self.restore_full_ui_state(res[1]["ui_state"])
                self.update_explorer(); self.update_table(); self.update_viz_combos(); self.jupyter_console.update_namespace()
            else: QMessageBox.critical(self, "Error", res[1])
        self.start_worker(_task, on_success=_done)

    def apply_layout_preset(self, mode):
        h = self.height()
        if mode == "Terminal": self.resizeDocks([self.console_dock], [int(h * 0.6)], Qt.Vertical)
        elif mode == "Data": self.resizeDocks([self.console_dock], [200], Qt.Vertical)
        elif mode == "Balanced": self.resizeDocks([self.console_dock], [int(h * 0.35)], Qt.Vertical)
        self.status_label.setText(f"LAYOUT: {mode} 모드 가동")

    def open_settings(self):
        d = SettingsDialog(self, self.app_settings)
        if d.exec(): 
            self.app_settings = d.get_settings()
            ThemeManager.apply_theme(self.app_settings["theme"])
            self.jupyter_console.apply_dynamic_theme()

    def load_data_file_async(self, p):
        def _task():
            enc = detect_encoding_parallel(p) if p.endswith(".csv") else "utf-8"
            engine = "Polars" if "Polars" in self.combo_engine.currentText() else "Pandas"
            return DataEngine.load_data(p, enc, engine)
        def _ok(res):
            success, df, m, code = res
            if success:
                self.df = df; self.variables[os.path.basename(p).replace(".","_")] = df
                self.update_explorer(); self.update_table(); self.update_viz_combos()
                self.display_data_mapping(df, "auto"); self.jupyter_console.update_namespace()
                self.update_code_trace("Data Loaded", code)
                self.run_ai_analysis(df)
        self.start_worker(_task, on_success=_ok)

    def apply_transform_async(self, op, params=None):
        if self.is_data_empty(): return
        if params is None:
            params = {"column": self.combo_x.currentText()}
            
        def _task(): return DataEngine.apply_transformation(self.df, op, params)
        def _ok(res):
            if res[0]:
                self.df = res[1]; self.update_table(); self.update_viz_combos()
                self.update_code_trace(f"Transform: {op}", res[3])
                self.run_ai_analysis(self.df)
        self.start_worker(_task, on_success=_ok)

    def is_data_empty(self):
        """Robust engine-agnostic check for empty dataframe (Pandas/Polars)."""
        if isinstance(self.df, pd.DataFrame):
            return self.df.empty
        elif isinstance(self.df, pl.DataFrame):
            return self.df.is_empty()
        return True

    def generate_plot_dispatch(self):
        if self.is_data_empty(): return
        viz_type = self.combo_viz.currentText()
        x, y = self.combo_x.currentText(), self.combo_y.currentText()
        theme = self.app_settings["theme"]
        
        # Sync browser background immediately
        v_colors = ThemeManager.get_colors(theme)
        self.browser.page().setBackgroundColor(QColor(v_colors["bg"]))
        
        if "Plotly" in self.combo_lib.currentText():
            task = lambda: VizManager.generate_plotly_html(self.df, viz_type, x, y, theme=theme)
        else:
            task = lambda: VizManager.generate_matplotlib_fig(self.df, viz_type, x, y, theme=theme)
        def _ok(res):
            if res[0]: self.browser.setUrl(QUrl.fromLocalFile(res[0])); self.central_tabs.setCurrentIndex(1)
        self.start_worker(task, on_success=_ok)

    def display_data_mapping(self, df, e):
        self.lbl_data_info.setText(f"<b>Shape:</b> {df.shape[0]}x{df.shape[1]}<br>Columns: {', '.join(df.columns[:5])}...")

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "Import Data", "", "Data (*.csv *.xlsx *.parquet)")
        if p: self.load_data_file_async(p)

    def on_variable_clicked(self, i):
        self.df = self.variables[i.text(0)]; self.update_table(); self.update_viz_combos()

    def update_table(self):
        if self.is_data_empty(): return
        display_df = self.df if isinstance(self.df, pd.DataFrame) else self.df.to_pandas()
        model = PandasModel(display_df)
        model.dataChanged.connect(self.on_data_edited_sync)
        self.table_view.setModel(model)

    def on_data_edited_sync(self):
        m = self.table_view.model()
        if m: self.df = m._data if isinstance(self.df, pd.DataFrame) else pl.from_pandas(m._data)

    def update_viz_combos(self):
        c = list(self.df.columns); self.combo_x.clear(); self.combo_y.clear(); self.combo_x.addItems(c); self.combo_y.addItems(c)

    def start_worker(self, f, *a, on_success=None, on_status=None, **k):
        w = GenericWorker(f, *a, **k)
        if on_success: w.result_ready.connect(on_success)
        def _safe_remove():
            if w in self.workers: self.workers.remove(w)
        w.finished.connect(_safe_remove)
        self.workers.append(w); w.start()

    def update_explorer(self):
        self.explorer_tree.clear()
        for v, d in self.variables.items():
            dtype = "Polars" if isinstance(d, pl.DataFrame) else "Pandas"
            size = f"{d.shape[0]}x{d.shape[1]}"
            self.explorer_tree.addTopLevelItem(QTreeWidgetItem([v, dtype, size]))

    def dragEnterEvent(self, e):
        if e.mimeData().hasUrls(): e.accept()
    def dropEvent(self, e):
        for u in e.mimeData().urls(): self.load_data_file_async(u.toLocalFile())
    def toggle_grid_edit_mode(self):
        m = self.table_view.model()
        if m:
            m.read_only = not m.read_only
            self.lbl_grid_mode.setText("MODE: EDIT" if not m.read_only else "MODE: READ")
            self.btn_toggle_edit.setText("읽기 전용" if not m.read_only else "쓰기 모드")

    def run_ai_analysis(self, df):
        """Runs IntelligenceCore profiling in background thread and displays results."""
        def _task(): return IntelligenceCore.analyze_full_profile(df)
        self.start_worker(_task, on_success=self.display_ai_insights)
        self.status_label.setText("AI Core V7: 데이터 프로파일링 중...")

    def display_ai_insights(self, report):
        """Renders the IntelligenceCore analysis report into the Insight dock panel."""
        if not report or "insights" not in report:
            return
        summary = report.get("summary", {})
        insights = report.get("insights", [])
        suggestions = report.get("suggestions", [])

        rows = summary.get("rows", 0)
        cols_n = summary.get("cols", 0)
        engine_name = summary.get("engine", "Unknown")

        header_html = (
            "<div style='font-family: Segoe UI, sans-serif; padding: 12px; "
            "background: #1a1b26; color: #c0caf5;'>"
            "<h3 style='color: #7aa2f7; border-bottom: 2px solid #414868; "
            f"padding-bottom: 8px; margin-bottom: 12px;'>&#129302; AI Core V7 분석 리포트</h3>"
            f"<p style='color: #565f89; font-size: 10pt;'>Shape: "
            f"<b style='color: #9ece6a;'>{rows}행 &times; {cols_n}열</b>"
            f" &nbsp;|&nbsp; Engine: <b style='color: #bb9af7;'>{engine_name}</b></p>"
            "<hr style='border: 0; border-top: 1px solid #414868; margin: 10px 0;'>"
        )
        body_html = "<ul style='line-height: 2.0; padding-left: 16px; margin: 0;'>"
        for ins in insights:
            body_html += f"<li style='color: #c0caf5; margin: 4px 0;'>{ins}</li>"
        body_html += "</ul>"

        sug_html = ""
        if suggestions:
            sug_html = ("<h4 style='color: #e0af68; margin-top: 16px; margin-bottom: 8px;'>"
                        "&#128161; 추천 시각화</h4>"
                        "<ul style='line-height: 1.8; padding-left: 16px;'>")
            for s in suggestions:
                sug_html += (f"<li style='color: #73daca;'><b>{s.get('type','')}</b> "
                             f"&mdash; <span style='color:#565f89;'>{s.get('desc','')}</span></li>")
            sug_html += "</ul>"

        self.insight_output.setHtml(header_html + body_html + sug_html + "</div>")
        self.insight_dock.raise_()
        self.status_label.setText(f"AI Core V7: 분석 완료 — {rows}행 x {cols_n}열")


viz_type_map = {"히스토그램": "histogram", "산점도": "scatter", "박스 플롯": "box", "선 그래프": "line", "바이올린 플롯": "violin", "바 차트": "bar", "파이 차트": "pie", "영역 차트": "area"}
viz_type_map_sns = {"히스토그램": "histplot", "산점도": "scatterplot", "박스 플롯": "boxplot", "선 그래프": "lineplot", "바이올린 플롯": "violinplot", "바 차트": "barplot"}
