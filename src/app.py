import sys
import io
import os
import contextlib
import pandas as pd
import numpy as np
import plotly.express as px
from PySide6.QtWebEngineWidgets import QWebEngineView
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
    """Integrated Mini Web Browser for data intelligence searches."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        
        # Navigation Bar
        self.nav_layout = QHBoxLayout()
        self.btn_back = QPushButton("<")
        self.btn_forward = QPushButton(">")
        self.btn_reload = QPushButton("R")
        self.url_bar = QLineEdit()
        self.url_bar.setPlaceholderText("https://www.google.com")
        self.url_bar.returnPressed.connect(self.navigate_to_url)
        self.btn_go = QPushButton("GO")
        self.btn_go.clicked.connect(self.navigate_to_url)
        
        self.nav_layout.addWidget(self.btn_back); self.nav_layout.addWidget(self.btn_forward); self.nav_layout.addWidget(self.btn_reload)
        self.nav_layout.addWidget(self.url_bar, 1); self.nav_layout.addWidget(self.btn_go)
        self.layout.addLayout(self.nav_layout)
        
        # Browser Engine
        self.browser = QWebEngineView()
        self.browser.setUrl(QUrl("https://www.google.com"))
        self.browser.urlChanged.connect(self.update_url_bar)
        self.layout.addWidget(self.browser)
        
        # Button connections
        self.btn_back.clicked.connect(self.browser.back)
        self.btn_forward.clicked.connect(self.browser.forward)
        self.btn_reload.clicked.connect(self.browser.reload)

    def navigate_to_url(self):
        url = self.url_bar.text()
        if not url.startswith("http"): url = "https://" + url
        self.browser.setUrl(QUrl(url))

    def update_url_bar(self, q):
        self.url_bar.setText(q.toString())

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - High-Performance AI Hub V7")
        self.resize(1600, 1000)
        self.setAcceptDrops(True)
        
        # Enterprise Data Store
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}
        self.workers = []
        
        # Application Settings (Dark V7)
        self.app_settings = {
            "compress": True, "theme": "Dark", "font_size": 10, "auto_analysis": True
        }
        
        # Force Apply Premium Theme Manager
        ThemeManager.apply_theme(self.app_settings.get("theme", "Dark"))
        
        # Core Handlers
        self.jupyter_console = JupyterConsoleManager(self)
        self.jupyter_server = JupyterServerManager()
        self.jupyter_server.start()
        
        self.init_ui()
        self.init_menu_and_toolbar()
        
        # Engine State
        self.viz_lib = "Plotly"
        self.status_label.setText("Datamixer Enterprise Hub Online - Browser/Jupyter Integrated")

    def closeEvent(self, event):
        """Shutdown handling."""
        if hasattr(self, 'jupyter_console'): self.jupyter_console.shutdown()
        if hasattr(self, 'jupyter_server'): self.jupyter_server.stop()
        super().closeEvent(event)

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # 1. Grid Tab
        self.view_tab = QWidget(); v1 = QVBoxLayout(self.view_tab); self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive); self.table_view.setAlternatingRowColors(True)
        v1.addWidget(self.table_view); self.central_tabs.addTab(self.view_tab, "분석 그리드")
        
        # 2. Visualization Tab
        self.viz_container = QWidget(); v2 = QVBoxLayout(self.viz_container); self.browser = QWebEngineView()
        self.browser.page().setBackgroundColor(QColor("#1a1b26")); self.static_canvas_container = QWidget(); self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        v2.addWidget(self.browser); v2.addWidget(self.static_canvas_container); self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_container, "시각화 리포트")
        
        # 3. Jupyter Workbench Tab
        self.jupyter_wb_tab = QWidget(); v3 = QVBoxLayout(self.jupyter_wb_tab)
        self.wb_browser = QWebEngineView(); QTimer.singleShot(1500, lambda: self.wb_browser.setUrl(QUrl(self.jupyter_server.url)))
        v3.addWidget(self.wb_browser); self.central_tabs.addTab(self.jupyter_wb_tab, "주피터 워크벤치")

        # 4. Mini Browser Tab (New!)
        self.browser_tab = MiniBrowser(); self.central_tabs.addTab(self.browser_tab, "인텔리전스 브라우저")
        
        self.setup_docks()
        
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.status_label = QLabel("SYSTEM IDLE"); self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        self.explorer_dock = QDockWidget("변수 탐색기", self)
        self.explorer_tree = QTreeWidget(); self.explorer_tree.setHeaderLabels(["변수명", "형태", "메모리"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked); self.explorer_tree.setIndentation(15)
        self.explorer_dock.setWidget(self.explorer_tree); self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        self.insight_dock = QDockWidget("AI 인사이트 리포트", self)
        self.insight_output = QTextEdit(); self.insight_output.setReadOnly(True)
        self.insight_output.setStyleSheet("font-family: 'Cascadia Code'; background-color: #1a1b26;")
        self.insight_dock.setWidget(self.insight_output); self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        self.console_dock = QDockWidget("Jupyter Console (Hub)", self)
        self.console_dock.setWidget(self.jupyter_console.widget); self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        self.props_dock = QDockWidget("코어 컨트롤", self)
        self.setup_props_panel(); self.props_dock.setWidget(self.props_container); self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget(); layout = QVBoxLayout(self.props_container)
        info_group = QGroupBox("매핑 스키마"); self.info_v = QVBoxLayout(info_group); self.lbl_data_info = QLabel("데이터 대기 중...")
        self.lbl_data_info.setWordWrap(True); self.lbl_data_info.setStyleSheet("color: #7aa2f7; font-weight: bold;"); self.info_v.addWidget(self.lbl_data_info); layout.addWidget(info_group)
        viz_group = QGroupBox("시각화 컨트롤"); vf = QFormLayout(viz_group); self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"]); vf.addRow("그래프 종류:", self.combo_plot_type)
        self.combo_x = QComboBox(); self.combo_y = QComboBox(); vf.addRow("X축:", self.combo_x); vf.addRow("Y축:", self.combo_y)
        self.btn_plot = QPushButton("엔진 실행"); self.btn_plot.clicked.connect(self.generate_plot_dispatch); vf.addRow(self.btn_plot); layout.addWidget(viz_group)
        layout.addStretch()

    def init_menu_and_toolbar(self):
        menubar = self.menuBar(); file_menu = menubar.addMenu("파일 (&F)")
        for name, callback in [
            ("데이터 로드...", self.load_data_async), ("프로젝트 저장...", self.save_project),
            ("프로젝트 열기...", self.load_project), (None, None), ("설정...", self.open_settings), (None, None), ("종료", self.close)
        ]:
            if name is None: file_menu.addSeparator()
            else: file_menu.addAction(name).triggered.connect(callback)
        toolbar = QToolBar("ToolBar"); self.addToolBar(toolbar); bt_set = QPushButton("시스템 설정"); bt_set.clicked.connect(self.open_settings); toolbar.addWidget(bt_set)

    def open_settings(self):
        diag = SettingsDialog(self, self.app_settings)
        if diag.exec(): self.app_settings = diag.get_settings(); ThemeManager.apply_theme(self.app_settings["theme"]); self.status_label.setText("설정 저장됨.")

    def load_data_file_async(self, file_path):
        def _task():
            encoding = detect_encoding_parallel(file_path) if file_path.endswith(".csv") else "utf-8"
            success, df, msg = DataEngine.load_data(file_path, encoding)
            if not success: raise Exception(msg)
            return df, msg, encoding
        def _on_success(res):
            df, msg, encoding = res; var_name = os.path.basename(file_path).replace(".", "_")
            self.variables[var_name] = df; self.df = df; self.update_explorer(); self.update_table(); self.update_viz_combos(); self.display_data_mapping(df, encoding)
            self.jupyter_console.update_namespace()
            scratch = os.path.join(self.jupyter_server.notebook_dir, "last_loaded_data.parquet"); df.to_parquet(scratch, index=False)
            self.status_label.setText(f"LOAD SUCCESS: {file_path}")
            if self.app_settings["auto_analysis"]: self.start_worker(lambda: IntelligenceCore.analyze_full_profile(df), on_success=self.on_intelligence_finished)
        self.start_worker(_task, on_success=_on_success, on_status=lambda _: self.status_label.setText("데이터 지능형 로딩 중..."))

    def on_intelligence_finished(self, report):
        txt = "<b>[지능형 서버 - AI 허브 V7]</b><br><br>"
        for insight in report["insights"]: txt += f"<span style='color: #7aa2f7;'>▶</span> {insight}<br><br>"
        self.insight_output.setHtml(txt)
        if report["suggestions"]:
            best = report["suggestions"][0]; self.combo_plot_type.setCurrentText(best["type"]); self.combo_x.setCurrentText(best["x"])
            if best["y"]: self.combo_y.setCurrentText(best["y"])
            self.generate_plot_dispatch()

    def generate_plot_dispatch(self):
        if self.df.empty: return
        t = self.combo_plot_type.currentText(); x = self.combo_x.currentText(); y = self.combo_y.currentText()
        dp = self.df if len(self.df) < 100000 else self.df.sample(100000)
        self.start_worker(lambda: VizManager.generate_plotly_html(dp, t, x, y), on_success=lambda r: self.browser.setUrl(QUrl.fromLocalFile(r[0])) if r[0] else None)

    def display_data_mapping(self, df, encoding):
        help_map = {"utf-8": "표준형", "cp949": "한글 전용(Legacy)", "euc-kr": "한국 표준(Legacy)"}
        tip = help_map.get(encoding.lower(), "자동 감지 인코딩")
        info = f"<b>Encoding:</b> <span style='color: #9ece6a;'>{encoding}</span> <span style='color: #7aa2f7;'>[?]</span><br>"
        info += f"<b>Shape:</b> <span style='color: #7aa2f7;'>{df.shape[0]:,} row x {df.shape[1]:,} col</span><br><br>"
        info += "<b>Schema:</b><br>"
        for col in df.columns: info += f"- {col}: {str(df[col].dtype)}<br>"
        self.lbl_data_info.setText(info); self.lbl_data_info.setToolTip(f"<b>[Help]</b><br>{tip}")

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "Import Data", "", "Data Files (*.csv *.xlsx *.xls)")
        if p: self.load_data_file_async(p)

    def save_project(self):
        p, _ = QFileDialog.getSaveFileName(self, "Save Project", "", "Project (*.dmx)")
        if p:
            ui = {"Geo": self.saveGeometry().toHex().data().decode(), "State": self.saveState().toHex().data().decode()}
            success, msg = SessionManager.save_project(p, self.variables, ui, True)
            if success: self.status_label.setText(msg)
            else: QMessageBox.critical(self, "오류", msg)

    def load_project(self):
        p, _ = QFileDialog.getOpenFileName(self, "Open Project", "", "Project (*.dmx)")
        if p:
            s, r = SessionManager.load_project(p)
            if s: self.variables = r["variables"]; self.update_explorer(); self.status_label.setText(f"Session Restored: {os.basename(p)}")

    def on_variable_clicked(self, item):
        vn = item.text(0); self.df = self.variables[vn]; self.update_table(); self.update_viz_combos()

    def update_table(self):
        if not self.df.empty: self.table_view.setModel(PandasModel(self.df))

    def update_viz_combos(self):
        cols = list(self.df.columns); self.combo_x.clear(); self.combo_y.clear(); self.combo_x.addItems(cols); self.combo_y.addItems(cols)

    def start_worker(self, func, *args, on_success=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        if on_status: worker.status_update.connect(on_status)
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        self.workers.append(worker); worker.start()

    def update_explorer(self):
        self.explorer_tree.clear()
        for v, d in self.variables.items():
            if isinstance(d, pd.DataFrame):
                item = QTreeWidgetItem([v, f"{d.shape[0]:,} x {d.shape[1]:,}", f"{d.memory_usage(deep=True).sum() / 1024**2:.2f} MB"])
                self.explorer_tree.addTopLevelItem(item)

    def dragEnterEvent(self, e): 
        if e.mimeData().hasUrls(): e.accept()
        else: e.ignore()
    def dropEvent(self, e):
        urls = e.mimeData().urls()
        if urls:
            p = urls[0].toLocalFile()
            if p.endswith(".dmx"): self.load_project_file(p)
            else: self.load_data_file_async(p)
