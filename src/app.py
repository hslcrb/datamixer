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
from PySide6.QtCore import Qt, QUrl, QSize, QByteArray
from PySide6.QtGui import QPalette, QColor

# Internal imports
from .models import PandasModel
from .utils import detect_encoding_parallel
from .worker import GenericWorker
from .engine import DataEngine
from .session import SessionManager
from .viz_manager import VizManager
from .settings import SettingsDialog
from .repl import ReplHandler
from .intelligence.core import IntelligenceCore
from .theme import ThemeManager

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - High-Performance AI Hub V6")
        self.resize(1600, 1000)
        self.setAcceptDrops(True)
        
        # Enterprise Data Store
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}
        self.workers = []
        
        # Global Settings (Dark as default)
        self.app_settings = {
            "compress": True, "theme": "Dark", "font_size": 10, 
            "default_engine": "Polars", "auto_analysis": True
        }
        
        # Apply Global Dark Theme
        self.setStyleSheet(ThemeManager.get_dark_style())

        # Handlers
        self.repl = ReplHandler(self)
        
        self.init_ui()
        self.init_menu_and_toolbar()
        
        # State
        self.engine_mode = "Polars" 
        self.viz_lib = "Plotly"
        self.status_label.setText("Enterprise AI Hub V6 - 병렬 엔진 로드 대기 중")

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # 1. Grid Tab
        self.view_tab = QWidget()
        vk = QVBoxLayout(self.view_tab)
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setAlternatingRowColors(True)
        vk.addWidget(self.table_view)
        self.central_tabs.addTab(self.view_tab, "지능형 분석 그리드")
        
        # 2. Intelligence Viz
        self.viz_container = QWidget()
        vz = QVBoxLayout(self.viz_container)
        self.browser = QWebEngineView()
        self.browser.page().setBackgroundColor(QColor("#1a1b26")) 
        self.static_canvas_container = QWidget()
        self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        vz.addWidget(self.browser)
        vz.addWidget(self.static_canvas_container)
        self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_container, "지능형 시각 결과")
        
        self.setup_docks()
        
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_label = QLabel("Enterprise AI HUB READY")
        self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        # Explorer
        self.explorer_dock = QDockWidget("데이터 탐색기 (Vars)", self)
        self.explorer_tree = QTreeWidget()
        self.explorer_tree.setHeaderLabels(["변수명", "형태", "메모리"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked)
        self.explorer_dock.setWidget(self.explorer_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # AI Insight
        self.insight_dock = QDockWidget("AI 지능형 분석 리포트 (Real-time)", self)
        self.insight_output = QTextEdit()
        self.insight_output.setReadOnly(True)
        self.insight_output.setPlaceholderText("분석 대기 중...")
        self.insight_output.setStyleSheet("font-family: 'Cascadia Code', 'Consolas'; background-color: #1a1b26; border: none;")
        self.insight_dock.setWidget(self.insight_output)
        self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        # REPL Virtual Console
        self.console_dock = QDockWidget("CLI 터미널 코어", self)
        self.setup_cli_tab()
        self.console_dock.setWidget(self.cli_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # Toolbox
        self.props_dock = QDockWidget("지능형 툴킷", self)
        self.setup_props_panel()
        self.props_dock.setWidget(self.props_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget()
        layout = QVBoxLayout(self.props_container)
        
        info_group = QGroupBox("매핑 스키마 (Schema Mapping)")
        self.info_v = QVBoxLayout(info_group)
        self.lbl_data_info = QLabel("데이터를 로드해 주세요.")
        self.lbl_data_info.setWordWrap(True)
        self.lbl_data_info.setStyleSheet("color: #7aa2f7; font-weight: bold;")
        self.info_v.addWidget(self.lbl_data_info)
        layout.addWidget(info_group)

        viz_group = QGroupBox("시각화 인터페이스 제어")
        vf = QFormLayout(viz_group)
        self.combo_plot_type = QComboBox(); self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        vf.addRow("그래프 종류:", self.combo_plot_type)
        self.combo_x = QComboBox(); self.combo_y = QComboBox()
        vf.addRow("X축 필드:", self.combo_x); vf.addRow("Y축 필드:", self.combo_y)
        self.btn_plot = QPushButton("엔진 렌더링 실행"); self.btn_plot.clicked.connect(self.generate_plot_dispatch)
        vf.addRow(self.btn_plot)
        layout.addWidget(viz_group)
        layout.addStretch()

    def setup_cli_tab(self):
        self.cli_container = QWidget()
        cl = QVBoxLayout(self.cli_container)
        self.cli_output = QTextEdit(); self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'Cascadia Code', 'Consolas'; font-size: 10pt; border: none;")
        cl.addWidget(self.cli_output)
        il = QHBoxLayout()
        self.cli_input = QLineEdit(); self.cli_input.setPlaceholderText("명령어 입력... (예: /help, df.info())")
        self.cli_input.returnPressed.connect(self.execute_cli)
        self.btn_exec = QPushButton("ENTER"); self.btn_exec.clicked.connect(self.execute_cli)
        il.addWidget(self.cli_input); il.addWidget(self.btn_exec)
        cl.addLayout(il)

    def init_menu_and_toolbar(self):
        menubar = self.menuBar(); file_menu = menubar.addMenu("파일 (&F)")
        for name, callback in [
            ("데이터 불러오기 (Import)...", self.load_data_async), ("프로젝트 저장...", self.save_project),
            ("프로젝트 열기...", self.load_project), (None, None),
            ("환경 설정 (Settings)...", self.open_settings), (None, None), ("종료 (Exit)", self.close)
        ]:
            if name is None: file_menu.addSeparator()
            else: file_menu.addAction(name).triggered.connect(callback)
        toolbar = QToolBar("Enterprise Hub AI Core"); self.addToolBar(toolbar)
        bt_set = QPushButton("시스템 설정"); bt_set.clicked.connect(self.open_settings); toolbar.addWidget(bt_set)

    def open_settings(self):
        diag = SettingsDialog(self, self.app_settings)
        if diag.exec(): self.app_settings = diag.get_settings(); self.status_label.setText("지능형 설정 저장됨.")

    def update_engine_mode(self, text): self.engine_mode = "Polars" if "Polars" in text else "Pandas"
    def update_viz_lib(self, text):
        self.viz_lib = "Plotly" if "Plotly" in text else "Matplotlib"
        if self.viz_lib == "Plotly": self.browser.show(); self.static_canvas_container.hide()
        else: self.browser.hide(); self.static_canvas_container.show()

    # --- IMMEDIATE LOADING & PARALLEL ANALYSIS ---
    def load_data_file_async(self, file_path):
        """Immediately display data while processing intelligence in parallel."""
        # 1. Load data worker (Primary)
        def _load_task():
            encoding = detect_encoding_parallel(file_path) if file_path.endswith(".csv") else "utf-8"
            success, df, msg = DataEngine.load_data(file_path, encoding)
            if not success: raise Exception(msg)
            return df, msg, encoding

        def _on_load_success(res):
            df, msg, encoding = res
            var_name = os.path.basename(file_path).replace(".", "_")
            self.variables[var_name] = df; self.df = df
            # IMMEDIATE UI FEEDBACK
            self.update_explorer(); self.update_table(); self.update_viz_combos()
            self.display_data_mapping(df, encoding)
            self.status_label.setText(f"데이터 즉시 로드 완료: {file_path}")
            
            # 2. Trigger Intelligence Analysis in Parallel (Second Worker)
            if self.app_settings["auto_analysis"]:
                self.start_worker(
                    lambda: IntelligenceCore.analyze_full_profile(df),
                    on_success=self.on_intelligence_finished,
                    on_status=lambda _: self.status_label.setText("데이터 지능형 패턴 분석 중...")
                )

        self.start_worker(_load_task, on_status=lambda _: self.status_label.setText("데이터 지능형 로드 중..."))

    def on_intelligence_finished(self, report):
        """Handle results from the parallel intelligence worker."""
        txt = "<b>[지능형 데이터 분석 보고서 - AI CORE V6]</b><br><br>"
        for insight in report["insights"]: txt += f"<span style='color: #7aa2f7;'>-</span> {insight}<br><br>"
        self.insight_output.setHtml(txt)
        if report["suggestions"]:
            best = report["suggestions"][0]
            self.combo_plot_type.setCurrentText(best["type"])
            self.combo_x.setCurrentText(best["x"])
            if best["y"]: self.combo_y.setCurrentText(best["y"])
            self.generate_plot_dispatch()
            self.status_label.setText(f"분석 완료: {best['desc']} 제안.")

    def display_data_mapping(self, df, encoding):
        info = f"<b>Encoding:</b> <span style='color: #9ece6a;'>{encoding}</span><br>"
        info += f"<b>Shape:</b> <span style='color: #7aa2f7;'>{df.shape[0]:,} row x {df.shape[1]:,} col</span><br><br>"
        info += "<b>Schema Mapping:</b><br>"
        for col in df.columns: info += f"- {col}: <span style='color: #e0af68;'>{str(df[col].dtype)}</span><br>"
        self.lbl_data_info.setText(info)

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "Import Data", "", "Data Files (*.csv *.xlsx *.xls)")
        if p: self.load_data_file_async(p)

    def save_project(self):
        p, _ = QFileDialog.getSaveFileName(self, "Save Project", "", "Project (*.dmx)")
        if p: self.smart_save_project(p)

    def smart_save_project(self, path):
        ui = {
            "Geometry": self.saveGeometry().toHex().data().decode(),
            "State": self.saveState().toHex().data().decode(),
            "EngineMode": self.engine_mode, "VizLib": self.viz_lib, "SelectedTab": self.central_tabs.currentIndex()
        }
        success, msg = SessionManager.save_project(path, self.variables, ui, self.app_settings["compress"])
        if success: self.status_label.setText(msg)
        else: QMessageBox.critical(self, "Save Error", msg)

    def load_project(self):
        p, _ = QFileDialog.getOpenFileName(self, "Open Project", "", "Project (*.dmx)")
        if p: self.load_project_file(p)

    def load_project_file(self, p):
        s, r = SessionManager.load_project(p)
        if s:
            self.variables = r["variables"]; self.update_explorer(); ui = r["ui_state"]
            if "Geometry" in ui: self.restoreGeometry(QByteArray.fromHex(ui["Geometry"].encode()))
            if "State" in ui: self.restoreState(QByteArray.fromHex(ui["State"].encode()))
            if "EngineMode" in ui: self.combo_engine.setCurrentText(ui["EngineMode"])
            if "VizLib" in ui: self.combo_lib.setCurrentText(ui["VizLib"])
            self.status_label.setText(f"Session Restored: {os.path.basename(p)}")
        else: QMessageBox.critical(self, "Error", r)

    def on_variable_clicked(self, item):
        vn = item.text(0); self.df = self.variables[vn]; self.update_table(); self.update_viz_combos()

    def update_table(self):
        if not self.df.empty: self.table_view.setModel(PandasModel(self.df))

    def update_viz_combos(self):
        cols = list(self.df.columns); self.combo_x.clear(); self.combo_y.clear(); self.combo_x.addItems(cols); self.combo_y.addItems(cols)

    def generate_plot_dispatch(self):
        if self.df.empty: return
        t = self.combo_plot_type.currentText(); x = self.combo_x.currentText(); y = self.combo_y.currentText()
        dp = self.df if len(self.df) < 100000 else self.df.sample(100000)
        
        if self.viz_lib == "Plotly":
            self.start_worker(lambda: VizManager.generate_plotly_html(dp, t, x, y), on_success=lambda r: self.browser.setUrl(QUrl.fromLocalFile(r[0])) if r[0] else None)
        else:
            def _mpl(): 
                plt.style.use('dark_background')
                return VizManager.generate_matplotlib_fig(dp, t, x, y)
            def _mpl_ok(r):
                if r[0]:
                    for i in reversed(range(self.static_canvas_layout.count())): self.static_canvas_layout.itemAt(i).widget().setParent(None)
                    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
                    self.static_canvas_layout.addWidget(FigureCanvas(r[0]))
            self.start_worker(_mpl, on_success=_mpl_ok)

    def execute_cli(self):
        t = self.cli_input.text()
        if not t: return
        self.cli_output.append(f"<b style='color: #7aa2f7;'>>>> {t}</b>")
        res = self.repl.process_input(t)
        if res: self.cli_output.append(f"<span style='color: #c0caf5;'>{res}</span>")
        self.cli_input.clear(); self.cli_output.ensureCursorVisible(); self.update_explorer(); self.update_table()

    def start_worker(self, func, *args, on_success=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        worker.error_occurred.connect(lambda m: QMessageBox.critical(self, "Engine Error", m))
        if on_status: worker.status_update.connect(on_status)
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        self.workers.append(worker); worker.start()

    def update_explorer(self):
        self.explorer_tree.clear()
        for v, d in self.variables.items():
            if isinstance(d, pd.DataFrame):
                mem = f"{d.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
                item = QTreeWidgetItem([v, f"{d.shape[0]:,} x {d.shape[1]:,}", mem])
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
