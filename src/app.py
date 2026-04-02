import sys
import io
import os
import contextlib
import pandas as pd
import numpy as np
import plotly.express as px
import webbrowser
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineProfile, QWebEnginePage
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTableView, QTabWidget, QLabel,
    QLineEdit, QComboBox, QGroupBox, QFormLayout,
    QMessageBox, QHeaderView, QTextEdit, QDockWidget, QTreeWidget,
    QTreeWidgetItem, QToolBar, QStatusBar, QFrame
)
from PySide6.QtCore import Qt, QUrl, QSize, QByteArray, QTimer
from PySide6.QtGui import QPalette, QColor, QAction

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
    """Integrated Premium Web Browser with Persistence and External Sync."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        
        # Persistent Profile Management
        profile_path = os.path.join(os.getcwd(), "browser_enterprise_profile")
        if not os.path.exists(profile_path): os.makedirs(profile_path)
        
        # Create a persistent profile for "Real Browser" feel (cookies, cache)
        self.profile = QWebEngineProfile("EnterpriseProfile", self)
        self.profile.setPersistentStoragePath(profile_path)
        self.profile.setPersistentCookiesPolicy(QWebEngineProfile.AllowPersistentCookies)
        
        # Navigation Bar
        self.nav_layout = QHBoxLayout()
        self.btn_back = QPushButton("<"); self.btn_back.setToolTip("뒤로 가기")
        self.btn_forward = QPushButton(">"); self.btn_forward.setToolTip("앞으로 가기")
        self.btn_reload = QPushButton("R"); self.btn_reload.setToolTip("새로 고침")
        
        self.url_bar = QLineEdit()
        self.url_bar.setPlaceholderText("검색어나 URL을 입력하세요 (시스템 동기화 활성)")
        self.url_bar.returnPressed.connect(self.navigate_to_url)
        
        self.btn_go = QPushButton("GO")
        self.btn_go.clicked.connect(self.navigate_to_url)
        
        # [NEW] Sync/External Buttons
        self.btn_external = QPushButton("🌐")
        self.btn_external.setToolTip("현재 페이지를 외부 브라우저(크롬/엣지)에서 열기")
        self.btn_external.clicked.connect(self.open_external)
        
        self.nav_layout.addWidget(self.btn_back); self.nav_layout.addWidget(self.btn_forward); self.nav_layout.addWidget(self.btn_reload)
        self.nav_layout.addWidget(self.url_bar, 1); self.nav_layout.addWidget(self.btn_go); self.nav_layout.addWidget(self.btn_external)
        self.layout.addLayout(self.nav_layout)
        
        # Browser Engine with Profile
        self.browser = QWebEngineView()
        self.page = QWebEnginePage(self.profile, self.browser)
        self.browser.setPage(self.page)
        self.browser.setUrl(QUrl("https://www.google.com"))
        self.browser.urlChanged.connect(self.update_url_bar)
        self.layout.addWidget(self.browser)
        
        self.btn_back.clicked.connect(self.browser.back)
        self.btn_forward.clicked.connect(self.browser.forward)
        self.btn_reload.clicked.connect(self.browser.reload)

    def navigate_to_url(self):
        url = self.url_bar.text()
        if not ("." in url) or (" " in url): # Search if not a direct URL
            url = f"https://www.google.com/search?q={url}"
        elif not url.startswith("http"): url = "https://" + url
        self.browser.setUrl(QUrl(url))

    def update_url_bar(self, q):
        self.url_bar.setText(q.toString())

    def open_external(self):
        """Opens current URL in the system's default browser (Chrome/Edge)."""
        webbrowser.open(self.browser.url().toString())

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - Ultimate AI Hub V7")
        self.resize(1600, 1000)
        self.setAcceptDrops(True)
        
        self.df = pd.DataFrame(); self.variables = {"df": self.df}; self.workers = []
        self.app_settings = {"compress": True, "theme": "Auto", "font_size": 10, "auto_analysis": True}
        
        ThemeManager.apply_theme(self.app_settings.get("theme", "Auto"))
        self.jupyter_console = JupyterConsoleManager(self); self.jupyter_server = JupyterServerManager(); self.jupyter_server.start(self.app_settings["theme"])
        
        self.init_ui()
        self.init_menu_and_toolbar()
        self.status_label.setText("Datamixer Enterprise Hub Online - Full Browser Sync Enabled")

    def closeEvent(self, event):
        if hasattr(self, 'jupyter_console'): self.jupyter_console.shutdown()
        if hasattr(self, 'jupyter_server'): self.jupyter_server.stop()
        super().closeEvent(event)

    def init_ui(self):
        self.setDockNestingEnabled(True); self.central_tabs = QTabWidget(); self.setCentralWidget(self.central_tabs)
        
        # Analysis Grid
        self.view_tab = QWidget(); v1 = QVBoxLayout(self.view_tab); self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive); v1.addWidget(self.table_view); self.central_tabs.addTab(self.view_tab, "분석 그리드")
        
        # Visual Report
        self.viz_container = QWidget(); v2 = QVBoxLayout(self.viz_container); self.browser = QWebEngineView(); self.browser.page().setBackgroundColor(QColor("#1a1b26"))
        self.static_canvas_container = QWidget(); self.static_canvas_layout = QVBoxLayout(self.static_canvas_container); v2.addWidget(self.browser); v2.addWidget(self.static_canvas_container); self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_container, "시각 분석 리포트")
        
        # Jupyter Workbench
        self.jupyter_wb_tab = QWidget(); v3 = QVBoxLayout(self.jupyter_wb_tab); self.wb_browser = QWebEngineView(); QTimer.singleShot(1500, lambda: self.wb_browser.setUrl(QUrl(self.jupyter_server.url)))
        v3.addWidget(self.wb_browser); self.central_tabs.addTab(self.jupyter_wb_tab, "주피터 워크벤치")

        # Premium Browser with External Sync
        self.browser_tab = MiniBrowser(); self.central_tabs.addTab(self.browser_tab, "인텔리전스 브라우저")
        
        self.setup_docks()
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar); self.status_label = QLabel("SYSTEM IDLE"); self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        self.explorer_dock = QDockWidget("변수 탐색기", self); self.explorer_tree = QTreeWidget(); self.explorer_tree.setHeaderLabels(["변수명", "형태", "메모리"]); self.explorer_tree.itemClicked.connect(self.on_variable_clicked); self.explorer_dock.setWidget(self.explorer_tree); self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        self.insight_dock = QDockWidget("AI 인사이트 리포트", self); self.insight_output = QTextEdit(); self.insight_output.setReadOnly(True); self.insight_output.setStyleSheet("font-family: 'Cascadia Code'; background-color: #1a1b26;"); self.insight_dock.setWidget(self.insight_output); self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)
        self.console_dock = QDockWidget("Jupyter Console (Hub)", self); self.console_dock.setWidget(self.jupyter_console.widget); self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        self.props_dock = QDockWidget("코어 컨트롤", self); self.setup_props_panel(); self.props_dock.setWidget(self.props_container); self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget(); layout = QVBoxLayout(self.props_container); info_group = QGroupBox("매핑 스키마"); self.info_v = QVBoxLayout(info_group); self.lbl_data_info = QLabel("데이터 대기 중..."); self.lbl_data_info.setWordWrap(True); self.lbl_data_info.setStyleSheet("color: #7aa2f7; font-weight: bold;"); self.info_v.addWidget(self.lbl_data_info); layout.addWidget(info_group)
        viz_group = QGroupBox("시각화 컨트롤"); vfl = QFormLayout(viz_group); self.combo_plot_type = QComboBox(); self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"]); vfl.addRow("그래프:", self.combo_plot_type); self.combo_x = QComboBox(); self.combo_y = QComboBox(); vfl.addRow("X:", self.combo_x); vfl.addRow("Y:", self.combo_y); self.btn_plot = QPushButton("엔진 실행"); self.btn_plot.clicked.connect(self.generate_plot_dispatch); vfl.addRow(self.btn_plot); layout.addWidget(viz_group); layout.addStretch()

    def init_menu_and_toolbar(self):
        m = self.menuBar()
        f = m.addMenu("파일 (&F)")
        f.addAction("데이터 로드...").triggered.connect(self.load_data_async)
        f.addAction("설정...").triggered.connect(self.open_settings)
        f.addAction("종료").triggered.connect(self.close)
        t = QToolBar("ToolBox")
        self.addToolBar(t)
        t.addWidget(QPushButton("환경 설정", clicked=self.open_settings))

    def open_settings(self):
        d = SettingsDialog(self, self.app_settings)
        if d.exec():
            self.app_settings = d.get_settings()
            ThemeManager.apply_theme(self.app_settings["theme"])

    def load_data_file_async(self, p):
        def _task():
            enc = detect_encoding_parallel(p) if p.endswith(".csv") else "utf-8"
            s, df, m = DataEngine.load_data(p, enc)
            return df, m, enc if s else None
        
        def _ok(res):
            if not res: return
            df, m, enc = res
            self.df = df
            self.variables[os.path.basename(p).replace(".", "_")] = df
            self.update_explorer()
            self.update_table()
            self.update_viz_combos()
            self.display_data_mapping(df, enc)
            self.jupyter_console.update_namespace()
            df.to_parquet(os.path.join(self.jupyter_server.notebook_dir, "last_loaded_data.parquet"), index=False)
            if self.app_settings["auto_analysis"]:
                self.start_worker(lambda: IntelligenceCore.analyze_full_profile(df), on_success=self.on_intelligence_finished)
        
        self.start_worker(_task, on_success=_ok, on_status=lambda s: self.status_label.setText(s))

    def on_intelligence_finished(self, r):
        txt = "<b>[AI 허브 V7]</b><br><br>"
        for i in r["insights"]:
            txt += f"▶ {i}<br>"
        self.insight_output.setHtml(txt)
        
        if r["suggestions"]:
            b = r["suggestions"][0]
            self.combo_plot_type.setCurrentText(b["type"])
            self.combo_x.setCurrentText(b["x"])
            if b["y"]:
                self.combo_y.setCurrentText(b["y"])
            self.generate_plot_dispatch()

    def generate_plot_dispatch(self):
        if self.df.empty: return
        dp = self.df if len(self.df) < 50000 else self.df.sample(50000)
        self.start_worker(lambda: VizManager.generate_plotly_html(dp, self.combo_plot_type.currentText(), self.combo_x.currentText(), self.combo_y.currentText()), on_success=lambda r: self.browser.setUrl(QUrl.fromLocalFile(r[0])))

    def display_data_mapping(self, df, e):
        help_map = {"utf-8": "표준형", "cp949": "한글 전용(Legacy)", "euc-kr": "한국 표준(Legacy)"}
        tip = help_map.get(e.lower(), "자동 감지 인코딩")
        
        info = f"<b>Encoding:</b> {e} [<b>?</b>]<br>"
        info += f"<b>Shape:</b> {df.shape[0]:,} x {df.shape[1]:,}<br>"
        info += "<b>Schema:</b><br>"
        for c in df.columns:
            info += f"- {c}: {str(df[c].dtype)}<br>"
        
        self.lbl_data_info.setText(info)
        self.lbl_data_info.setToolTip(f"<b>[Help]</b><br>{tip}")

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "Load", "", "Data (*.csv *.xlsx)")
        if p:
            self.load_data_file_async(p)

    def on_variable_clicked(self, i): self.df = self.variables[i.text(0)]; self.update_table(); self.update_viz_combos()
    def update_table(self): [self.table_view.setModel(PandasModel(self.df)) if not self.df.empty else None]
    def update_viz_combos(self): c = list(self.df.columns); self.combo_x.clear(); self.combo_y.clear(); self.combo_x.addItems(c); self.combo_y.addItems(c)
    def start_worker(self, f, *a, on_success=None, on_status=None, **k):
        w = GenericWorker(f, *a, **k); [w.result_ready.connect(on_success) if on_success else None]; [w.status_update.connect(on_status) if on_status else None]; w.finished.connect(lambda: self.workers.remove(w)); self.workers.append(w); w.start()
    def update_explorer(self): 
        self.explorer_tree.clear()
        for v, d in self.variables.items(): self.explorer_tree.addTopLevelItem(QTreeWidgetItem([v, f"{d.shape[0]}x{d.shape[1]}", f"{d.memory_usage(deep=True).sum()/1024**2:.2f}MB"]))

    def dragEnterEvent(self, e): [e.accept() if e.mimeData().hasUrls() else e.ignore()]
    def dropEvent(self, e): [self.load_data_file_async(u.toLocalFile()) for u in e.mimeData().urls()]
