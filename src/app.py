import sys
import io
import os
import contextlib
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
    QTreeWidgetItem, QToolBar, QStatusBar, QFrame, QProgressBar
)
from PySide6.QtCore import Qt, QUrl, QSize, QByteArray, QTimer
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

class MiniBrowser(QWidget):
    """Integrated Premium Web Browser with Tokyo Night Sync."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        
        # Profile Sync
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
        self.browser.setPage(self.page); self.browser.setUrl(QUrl("https://www.google.com"))
        self.browser.urlChanged.connect(lambda q: self.url_bar.setText(q.toString()))
        self.layout.addWidget(self.browser)
        
        self.btn_back.clicked.connect(self.browser.back)
        self.btn_forward.clicked.connect(self.browser.forward)
        self.btn_reload.clicked.connect(self.browser.reload)

    def navigate(self):
        u = self.url_bar.text()
        if not ("." in u) or (" " in u): u = f"https://www.google.com/search?q={u}"
        elif not u.startswith("http"): u = "https://" + u
        self.browser.setUrl(QUrl(u))

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - Multi-Engine AI Workstation V7")
        self.resize(1700, 1050)
        self.setAcceptDrops(True)
        
        self.df = pd.DataFrame(); self.variables = {"df": self.df}; self.workers = []
        self.app_settings = {"compress": True, "theme": "Auto", "font_size": 10, "auto_analysis": True}
        
        # Apply Base Theme and Premium Overlays
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
        self.view_tab = QWidget(); v1 = QVBoxLayout(self.view_tab); self.table_view = QTableView()
        v1.addWidget(self.table_view); self.central_tabs.addTab(self.view_tab, "지능형 데이터 그리드")
        
        # Viz Tab
        self.viz_tab = QWidget(); v2 = QVBoxLayout(self.viz_tab); self.browser = QWebEngineView()
        self.browser.page().setBackgroundColor(QColor("#1a1b26"))
        self.static_canvas_container = QWidget(); self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        v2.addWidget(self.browser); v2.addWidget(self.static_canvas_container); self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_tab, "분석 리포트")
        
        # Jupyter Workbench
        self.jupyter_tab = QWidget(); v3 = QVBoxLayout(self.jupyter_tab); self.wb_browser = QWebEngineView()
        QTimer.singleShot(1500, lambda: self.wb_browser.setUrl(QUrl(self.jupyter_server.url)))
        v3.addWidget(self.wb_browser); self.central_tabs.addTab(self.jupyter_tab, "주피터 주크벤치")

        # Browser
        self.browser_tab = MiniBrowser(); self.central_tabs.addTab(self.browser_tab, "데이터 브라우저")
        
        self.setup_docks()
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.status_label = QLabel("SYSTEM: HEALTHY"); self.status_bar.addWidget(self.status_label)
        
        # Server Indicator
        self.server_label = QLabel("● JUPYTER ONLINE"); self.server_label.setStyleSheet("color: #9ece6a; font-weight: bold; margin-right: 15px;")
        self.status_bar.addPermanentWidget(self.server_label)

    def setup_docks(self):
        # Explorer (Professional Columns)
        self.explorer_dock = QDockWidget("변수 매니저 (Multi-Engine)", self)
        self.explorer_tree = QTreeWidget(); self.explorer_tree.setHeaderLabels(["변수명", "라이브러리/엔진", "메모리 점유"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked); self.explorer_tree.setIndentation(15)
        self.explorer_dock.setWidget(self.explorer_tree); self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # AI Insight
        self.insight_dock = QDockWidget("AI Core V7 인사이트 리포트", self)
        self.insight_output = QTextEdit(); self.insight_output.setReadOnly(True)
        self.insight_dock.setWidget(self.insight_output); self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        # Bottom Console
        self.console_dock = QDockWidget("Jupyter Interactive Hub", self)
        self.console_dock.setWidget(self.jupyter_console.widget); self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # Control Panel
        self.control_dock = QDockWidget("데이터 워크벤치 설정", self)
        self.setup_props_panel(); self.control_dock.setWidget(self.props_container); self.addDockWidget(Qt.RightDockWidgetArea, self.control_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget(); layout = QVBoxLayout(self.props_container)
        
        # Engine Control
        eg = QGroupBox("Core 엔진 컨트롤"); el = QFormLayout(eg)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["Polars (최고 성능)", "Pandas (안정적 특성)"])
        el.addRow("핵심 파싱 엔진:", self.combo_engine)
        self.lbl_data_info = QLabel("지능형 맵핑 대기 중..."); self.lbl_data_info.setWordWrap(True); self.lbl_data_info.setToolTip("데이터의 인코딩과 스키마 정보가 표시됩니다.")
        el.addRow(self.lbl_data_info); layout.addWidget(eg)
        
        # Viz Control
        vg = QGroupBox("지능형 시각 결과 제어"); vl = QFormLayout(vg)
        self.combo_viz = QComboBox(); self.combo_viz.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        vl.addRow("그래프 종류:", self.combo_viz); self.combo_x = QComboBox(); self.combo_y = QComboBox()
        vl.addRow("X축 칼럼:", self.combo_x); vl.addRow("Y축 칼럼:", self.combo_y)
        self.btn_exe = QPushButton("데이터 엔진 가동"); self.btn_exe.clicked.connect(self.generate_plot_dispatch)
        vl.addRow(self.btn_exe); layout.addWidget(vg)
        
        layout.addStretch()

    def init_menu_and_toolbar(self):
        m = self.menuBar(); f = m.addMenu("파일 (&F)")
        f.addAction("데이터 세트 로딩...").triggered.connect(self.load_data_async)
        f.addAction("환경 설정...").triggered.connect(self.open_settings)
        f.addAction("종료").triggered.connect(self.close)
        
        tb = QToolBar("Main Controls"); self.addToolBar(tb)
        btn_set = QPushButton("시스템 설정"); btn_set.clicked.connect(self.open_settings); tb.addWidget(btn_set)

    def open_settings(self):
        d = SettingsDialog(self, self.app_settings)
        if d.exec(): 
            self.app_settings = d.get_settings()
            ThemeManager.apply_theme(self.app_settings["theme"])
            self.status_label.setText("시스템 테마 동기화 완료.")

    def load_data_file_async(self, p):
        def _task():
            enc = detect_encoding_parallel(p) if p.endswith(".csv") else "utf-8"
            engine = "Polars" if "Polars" in self.combo_engine.currentText() else "Pandas"
            s, df, m = DataEngine.load_data(p, enc, engine)
            return df, m, enc if s else None
            
        def _ok(res):
            if not res: return
            df, m, enc = res; self.df = df
            self.variables[os.path.basename(p).replace(".","_")] = df
            self.update_explorer(); self.update_table(); self.update_viz_combos(); self.display_data_mapping(df, enc)
            self.jupyter_console.update_namespace()
            if hasattr(df, 'to_parquet'): 
                df.to_parquet(os.path.join(self.jupyter_server.notebook_dir, "last_loaded_data.parquet"))
            elif hasattr(df, 'write_parquet'): # Polars
                df.write_parquet(os.path.join(self.jupyter_server.notebook_dir, "last_loaded_data.parquet"))
            
            if self.app_settings["auto_analysis"]:
                self.start_worker(lambda: IntelligenceCore.analyze_full_profile(df), on_success=self.on_intelligence_finished)
                
        self.start_worker(_task, on_success=_ok, on_status=lambda s: self.status_label.setText(s))

    def on_intelligence_finished(self, r):
        html = "<b>[AI Intelligence Hub V7 - 통계 리포트]</b><br><br>"
        for i in r["insights"]: html += f"<span style='color: #7aa2f7;'>⚡</span> {i}<br><br>"
        self.insight_output.setHtml(html)
        if r["suggestions"]:
            b = r["suggestions"][0]; self.combo_viz.setCurrentText(b["type"])
            self.combo_x.setCurrentText(b["x"])
            if b["y"]: self.combo_y.setCurrentText(b["y"])
            self.generate_plot_dispatch()

    def generate_plot_dispatch(self):
        if self.df.empty: return
        dp = self.df
        if len(self.df) > 50000:
            dp = self.df.sample(50000) if isinstance(self.df, pd.DataFrame) else self.df.sample(n=50000)
        
        # Always use Plotly for premium interactivity
        self.start_worker(lambda: VizManager.generate_plotly_html(dp, self.combo_viz.currentText(), self.combo_x.currentText(), self.combo_y.currentText()), 
                          on_success=lambda r: self.browser.setUrl(QUrl.fromLocalFile(r[0])))

    def display_data_mapping(self, df, e):
        info = f"<b>Encoding:</b> <span style='color: #9ece6a;'>{e}</span><br><b>Shape:</b> <span style='color: #7aa2f7;'>{df.shape[0]:,} x {df.shape[1]:,}</span><br><br><b>스키마 정보:</b><br>"
        for c in df.columns: info += f"- {c}: {str(df[c].dtype if hasattr(df[c], 'dtype') else 'Polars Type')}<br>"
        self.lbl_data_info.setText(info)

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "Import Data", "", "Data (*.csv *.xlsx *.parquet)"); 
        if p: self.load_data_file_async(p)

    def on_variable_clicked(self, i): 
        self.df = self.variables[i.text(0)]; self.update_table(); self.update_viz_combos()

    def update_table(self): 
        if self.df.empty: return
        display_df = self.df if isinstance(self.df, pd.DataFrame) else self.df.to_pandas()
        self.table_view.setModel(PandasModel(display_df))

    def update_viz_combos(self): 
        c = list(self.df.columns); self.combo_x.clear(); self.combo_y.clear(); self.combo_x.addItems(c); self.combo_y.addItems(c)

    def start_worker(self, f, *a, on_success=None, on_status=None, **k):
        w = GenericWorker(f, *a, **k)
        if on_success: w.result_ready.connect(on_success)
        if on_status: w.status_update.connect(on_status)
        w.finished.connect(lambda: self.workers.remove(w))
        self.workers.append(w); w.start()

    def update_explorer(self): 
        self.explorer_tree.clear()
        for v, d in self.variables.items():
            dtype = "NumPy" if isinstance(d, np.ndarray) else ("Polars DF" if isinstance(d, pl.DataFrame) else "Pandas DF")
            size = f"{d.shape[0]}x{d.shape[1]}" if hasattr(d, 'shape') else ("N/A")
            mem = "Calculating..."
            if hasattr(d, 'memory_usage'): 
                mem = f"{d.memory_usage(deep=True).sum()/1024**2:.2f}MB"
            elif isinstance(d, pl.DataFrame):
                mem = f"{d.estimated_size() / 1024**2:.2f}MB"
            self.explorer_tree.addTopLevelItem(QTreeWidgetItem([v, dtype, f"{size} ({mem})"]))

    def dragEnterEvent(self, e): 
        if e.mimeData().hasUrls(): e.accept()
        else: e.ignore()
    def dropEvent(self, e): 
        for u in e.mimeData().urls(): self.load_data_file_async(u.toLocalFile())
