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

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - Intelligence Hub AI Core")
        self.resize(1500, 1000)
        self.setAcceptDrops(True)
        
        # Enterprise Data Store
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}
        self.workers = []
        
        # Global Settings
        self.app_settings = {
            "compress": True, "theme": "Light", "font_size": 10, 
            "default_engine": "Polars", "auto_analysis": False
        }
        
        # Core Handlers
        self.repl = ReplHandler(self)
        
        self.init_ui()
        self.init_menu_and_toolbar()
        
        # State
        self.engine_mode = "Polars" 
        self.viz_lib = "Plotly"
        self.status_label.setText("Enterprise Edition - 지능형 분석 모듈 가동")

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # View Tab
        self.view_tab = QWidget()
        view_layout = QVBoxLayout(self.view_tab)
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setStyleSheet("QTableView { selection-background-color: rgba(135, 206, 235, 100); }")
        view_layout.addWidget(self.table_view)
        self.central_tabs.addTab(self.view_tab, "분석 데이터 세트")
        
        # Visualizer Tab
        self.viz_container = QWidget()
        viz_vbox = QVBoxLayout(self.viz_container)
        self.browser = QWebEngineView()
        self.static_canvas_container = QWidget()
        self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        viz_vbox.addWidget(self.browser)
        viz_vbox.addWidget(self.static_canvas_container)
        self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_container, "지능형 시각 결과 리포트")
        
        self.setup_docks()
        
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_label = QLabel("Ready")
        self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        # Explorer Dock
        self.explorer_dock = QDockWidget("데이터 탐색기 (Variables)", self)
        self.explorer_dock.setObjectName("ExplorerDock")
        self.explorer_tree = QTreeWidget()
        self.explorer_tree.setHeaderLabels(["변수명", "형태", "메모리 점유"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked)
        self.explorer_dock.setWidget(self.explorer_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # AI Insight Dock
        self.insight_dock = QDockWidget("AI 지능형 인사이트 리포트", self)
        self.insight_dock.setObjectName("InsightDock")
        self.insight_output = QTextEdit()
        self.insight_output.setReadOnly(True)
        self.insight_output.setStyleSheet("font-family: 'Consolas', monospace; qproperty-placeholderText: '데이터를 로드하면 AI가 분석한 결과를 보여줍니다.';")
        self.insight_dock.setWidget(self.insight_output)
        self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        # Bottom Dock: REPL & Handler
        self.console_dock = QDockWidget("지능형 제어 명령 핸들러 (REPL)", self)
        self.console_dock.setObjectName("ConsoleDock")
        self.setup_cli_tab()
        self.console_dock.setWidget(self.cli_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # Settings Dock (Prop Control)
        self.props_dock = QDockWidget("지능형 제어 도구 모음", self)
        self.props_dock.setObjectName("PropsDock")
        self.setup_props_panel()
        self.props_dock.setWidget(self.props_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget()
        layout = QVBoxLayout(self.props_container)
        
        info_group = QGroupBox("데이터 상세 지명 (Schema Mapping)")
        self.info_layout = QVBoxLayout(info_group)
        self.lbl_data_info = QLabel("데이터를 로드해 주세요.")
        self.lbl_data_info.setWordWrap(True)
        self.lbl_data_info.setStyleSheet("color: #34495e; font-weight: bold; font-size: 9pt;")
        self.info_layout.addWidget(self.lbl_data_info)
        layout.addWidget(info_group)

        viz_group = QGroupBox("시각화 엔진 통제")
        viz_form = QFormLayout(viz_group)
        self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        viz_form.addRow("그래프 종류:", self.combo_plot_type)
        self.combo_x = QComboBox(); self.combo_y = QComboBox()
        viz_form.addRow("X축 필드:", self.combo_x); viz_form.addRow("Y축 필드:", self.combo_y)
        self.btn_plot = QPushButton("그래프 렌더링")
        self.btn_plot.clicked.connect(self.generate_plot_dispatch)
        viz_form.addRow(self.btn_plot)
        layout.addWidget(viz_group)
        
        core_group = QGroupBox("엔카운터 핵심 설정")
        core_form = QFormLayout(core_group)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["Polars (고성능)", "Pandas (표준)"])
        self.combo_engine.currentTextChanged.connect(self.update_engine_mode)
        core_form.addRow("데이터 엔진:", self.combo_engine)
        self.combo_lib = QComboBox(); self.combo_lib.addItems(["Plotly (인터렉티브)", "Matplotlib (정적)"])
        self.combo_lib.currentTextChanged.connect(self.update_viz_lib)
        core_form.addRow("시각화 엔진:", self.combo_lib)
        layout.addWidget(core_group)
        
        layout.addStretch()

    def setup_cli_tab(self):
        self.cli_container = QWidget()
        layout = QVBoxLayout(self.cli_container)
        self.cli_output = QTextEdit(); self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'Consolas', monospace; background-color: #f7f9fa; font-size: 10pt;")
        layout.addWidget(self.cli_output)
        
        input_layout = QHBoxLayout()
        self.cli_input = QLineEdit(); self.cli_input.setPlaceholderText("명령어 입력 (예: /help, df.describe())")
        self.cli_input.setStyleSheet("font-family: 'Consolas', monospace; height: 35px;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        self.btn_exec = QPushButton("전송"); self.btn_exec.clicked.connect(self.execute_cli)
        input_layout.addWidget(self.cli_input); input_layout.addWidget(self.btn_exec)
        layout.addLayout(input_layout)

    def init_menu_and_toolbar(self):
        menubar = self.menuBar(); file_menu = menubar.addMenu("파일 (&F)")
        for name, callback in [
            ("데이터 로드...", self.load_data_async), ("프로젝트 저장...", self.save_project),
            ("프로젝트 열기...", self.load_project), (None, None),
            ("지능형 팝업 설정...", self.open_settings), (None, None), ("종료", self.close)
        ]:
            if name is None: file_menu.addSeparator()
            else: file_menu.addAction(name).triggered.connect(callback)
        
        toolbar = QToolBar("Enterprise Hub AI Core"); self.addToolBar(toolbar)
        bt_set = QPushButton("지능형 코어 설정"); bt_set.clicked.connect(self.open_settings); toolbar.addWidget(bt_set)

    def open_settings(self):
        diag = SettingsDialog(self, self.app_settings)
        if diag.exec(): self.app_settings = diag.get_settings(); self.status_label.setText("지능형 설정 저장됨.")

    def update_engine_mode(self, text): self.engine_mode = "Polars" if "Polars" in text else "Pandas"
    def update_viz_lib(self, text):
        self.viz_lib = "Plotly" if "Plotly" in text else "Matplotlib"
        if self.viz_lib == "Plotly": self.browser.show(); self.static_canvas_container.hide()
        else: self.browser.hide(); self.static_canvas_container.show()

    def load_data_file_async(self, file_path):
        def _task():
            encoding = detect_encoding_parallel(file_path) if file_path.endswith(".csv") else "utf-8"
            success, df, msg = DataEngine.load_data(file_path, encoding)
            if not success: raise Exception(msg)
            return df, msg, encoding

        def _on_success(res):
            df, msg, encoding = res
            var_name = os.path.basename(file_path).replace(".", "_")
            self.variables[var_name] = df; self.df = df
            self.update_explorer(); self.update_table(); self.update_viz_combos()
            self.status_label.setText(f"{msg}: {file_path}")
            self.display_data_mapping(df, encoding)
            if self.app_settings["auto_analysis"]: self.perform_intelligence_analysis(df)

        self.start_worker(_task, on_status=lambda _: self.status_label.setText("지능형 엔진 가동 중..."))

    def display_data_mapping(self, df, encoding):
        """Immediately display schema information."""
        info = f"<b>인코딩:</b> <span style='color: #27ae60;'>{encoding}</span><br>"
        info += f"<b>형태:</b> {df.shape[0]:,} 행 x {df.shape[1]:,} 열<br><br>"
        info += "<b>지능형 맵핑 스키마:</b><br>"
        for col in df.columns:
            dtype = str(df[col].dtype)
            info += f"- {col}: <span style='color: #e67e22;'>{dtype}</span><br>"
        self.lbl_data_info.setText(info)

    def perform_intelligence_analysis(self, df):
        """Advanced profiling using Intelligence Core."""
        report = IntelligenceCore.analyze_full_profile(df)
        
        # Update Insight Dock
        txt = "<b>[지능형 데이터 패턴 분석 보고서]</b><br><br>"
        for insight in report["insights"]: txt += f"- {insight}<br><br>"
        self.insight_output.setHtml(txt)
        
        # Suggest and Execute first suggestion if applicable
        if report["suggestions"]:
            best = report["suggestions"][0]
            self.combo_plot_type.setCurrentText(best["type"])
            self.combo_x.setCurrentText(best["x"])
            if best["y"]: self.combo_y.setCurrentText(best["y"])
            self.status_label.setText(f"AI 코어 분석: {best['desc']} 추천.")
            self.generate_plot_dispatch()

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "데이터 로드", "", "데이터 파일 (*.csv *.xlsx *.xls)")
        if p: self.load_data_file_async(p)

    def save_project(self):
        p, _ = QFileDialog.getSaveFileName(self, "Enterprise 프로젝트 저장", "", "Datamixer Project (*.dmx)")
        if p:
            ui = {
                "Geometry": self.saveGeometry().toHex().data().decode(),
                "State": self.saveState().toHex().data().decode(),
                "EngineMode": self.engine_mode, "VizLib": self.viz_lib, "SelectedTab": self.central_tabs.currentIndex()
            }
            s, m = SessionManager.smart_save(p, self.variables, ui, self.app_settings["compress"])
            if s: self.status_label.setText(m)
            else: QMessageBox.critical(self, "저장 오류", m)

    def load_project(self):
        p, _ = QFileDialog.getOpenFileName(self, "프로젝트 열기", "", "Project (*.dmx)")
        if p: self.load_project_file(p)

    def load_project_file(self, p):
        s, r = SessionManager.smart_load(p)
        if s:
            self.variables = r["variables"]; self.update_explorer(); ui = r["ui_state"]
            if "Geometry" in ui: self.restoreGeometry(QByteArray.fromHex(ui["Geometry"].encode()))
            if "State" in ui: self.restoreState(QByteArray.fromHex(ui["State"].encode()))
            if "EngineMode" in ui: self.combo_engine.setCurrentText(ui["EngineMode"])
            if "VizLib" in ui: self.combo_lib.setCurrentText(ui["VizLib"])
            self.status_label.setText(f"세션 복구 완료: {os.path.basename(p)}")
        else: QMessageBox.critical(self, "오류", r)

    def on_variable_clicked(self, item):
        var_name = item.text(0); self.df = self.variables[var_name]; self.update_table(); self.update_viz_combos()

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
            def _mpl(): return VizManager.generate_matplotlib_fig(dp, t, x, y)
            def _mpl_ok(r):
                if r[0]:
                    for i in reversed(range(self.static_canvas_layout.count())): self.static_canvas_layout.itemAt(i).widget().setParent(None)
                    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
                    self.static_canvas_layout.addWidget(FigureCanvas(r[0]))
            self.start_worker(_mpl, on_success=_mpl_ok)

    def run_engine_ops(self):
        if self.df.empty: return
        r, c, q, nv = self.input_loc_rows.text(), self.input_loc_cols.text(), self.input_query.text(), self.input_new_var.text()
        def _t():
            res = self.df
            if r != ":" or c != ":": res = res.loc[eval(r) if r != ":" else slice(None), eval(c) if c != ":" else slice(None)]
            if q: res = res.query(q)
            return res
        def _ok(res): self.variables[nv] = res; self.update_explorer(); QMessageBox.information(self, "성공", f"연산 완료: {nv}")
        self.start_worker(_t)

    def execute_cli(self):
        t = self.cli_input.text(); if not t: return
        self.cli_output.append(f"<b style='color: #27ae60;'>>>> {t}</b>")
        res = self.repl.process_input(t)
        if res: self.cli_output.append(res)
        self.cli_input.clear(); self.cli_output.ensureCursorVisible(); self.update_explorer(); self.update_table()

    def start_worker(self, func, *args, on_success=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        worker.error_occurred.connect(lambda m: QMessageBox.critical(self, "엔진 실행 오류", m))
        if on_status: worker.status_update.connect(on_status)
        worker.finished.connect(lambda: self.setEnabled(True))
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        self.workers.append(worker); self.setEnabled(False); worker.start()

    def update_explorer(self):
        self.explorer_tree.clear()
        for var, data in self.variables.items():
            if isinstance(data, pd.DataFrame):
                mem = f"{data.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
                item = QTreeWidgetItem([var, f"{data.shape[0]:,} x {data.shape[1]:,}", mem])
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
