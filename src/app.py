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
        self.setWindowTitle("Datamixer Enterprise - Intelligence Hub AI V5")
        self.resize(1600, 1000)
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
        
        # Handlers
        self.repl = ReplHandler(self)
        
        self.init_ui()
        self.init_menu_and_toolbar()
        
        self.engine_mode = "Polars" 
        self.viz_lib = "Plotly"
        self.status_label.setText("Datamixer Enterprise AI Core V5 활성화 - 시스템 무결성 확인 완료")

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # Grid Tab
        self.view_tab = QWidget()
        vk = QVBoxLayout(self.view_tab)
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setStyleSheet("QTableView { selection-background-color: rgba(135, 206, 235, 100); }")
        vk.addWidget(self.table_view)
        self.central_tabs.addTab(self.view_tab, "지능형 데이터 그리드")
        
        # Visualization Tab
        self.viz_container = QWidget()
        vz = QVBoxLayout(self.viz_container)
        self.browser = QWebEngineView()
        self.static_canvas_container = QWidget()
        self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        vz.addWidget(self.browser)
        vz.addWidget(self.static_canvas_container)
        self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_container, "분석 및 시각화 결과")
        
        self.setup_docks()
        
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_label = QLabel("Ready")
        self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        # 1. Variable Explorer
        self.explorer_dock = QDockWidget("데이터 탐색기 (Workspace Variables)", self)
        self.explorer_tree = QTreeWidget()
        self.explorer_tree.setHeaderLabels(["변수명", "형태", "메모리 점유"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked)
        self.explorer_dock.setWidget(self.explorer_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # 2. AI Insight Dock
        self.insight_dock = QDockWidget("AI 지능형 분석 리포트", self)
        self.insight_output = QTextEdit()
        self.insight_output.setReadOnly(True)
        self.insight_output.setPlaceholderText("데이터를 로드하면 AI 중심 분석 결과가 여기에 표시됩니다.")
        self.insight_output.setStyleSheet("font-family: 'Consolas', monospace; background-color: #f7f9fa;")
        self.insight_output.setMinimumHeight(180)
        self.insight_dock.setWidget(self.insight_output)
        self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        # 3. REPL Console
        self.console_dock = QDockWidget("명령 핸들러 및 REPL 가상 터미널", self)
        self.setup_cli_tab()
        self.console_dock.setWidget(self.cli_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # 4. Property Controls
        self.props_dock = QDockWidget("지능형 제어 및 툴박스", self)
        self.setup_props_panel()
        self.props_dock.setWidget(self.props_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget()
        layout = QVBoxLayout(self.props_container)
        
        info_group = QGroupBox("데이터 상세 맵킹 스키마")
        self.info_v = QVBoxLayout(info_group)
        self.lbl_data_info = QLabel("로드 대기 중...")
        self.lbl_data_info.setWordWrap(True)
        self.lbl_data_info.setStyleSheet("color: #2c3e50; font-weight: bold; font-size: 9pt;")
        self.info_v.addWidget(self.lbl_data_info)
        layout.addWidget(info_group)

        viz_group = QGroupBox("시각화 엔진 통제")
        vf = QFormLayout(viz_group)
        self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        vf.addRow("그래프 타입:", self.combo_plot_type)
        self.combo_x = QComboBox(); self.combo_y = QComboBox()
        vf.addRow("X축 선택:", self.combo_x); vf.addRow("Y축 선택:", self.combo_y)
        self.btn_plot = QPushButton("엔진 렌더링 실행")
        self.btn_plot.clicked.connect(self.generate_plot_dispatch)
        vf.addRow(self.btn_plot)
        layout.addWidget(viz_group)
        
        core_group = QGroupBox("시스템 코어 설정")
        cf = QFormLayout(core_group)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["Polars (고성능)", "Pandas (표준)"])
        self.combo_engine.currentTextChanged.connect(self.update_engine_mode)
        cf.addRow("분석 엔진:", self.combo_engine)
        self.combo_lib = QComboBox(); self.combo_lib.addItems(["Plotly (인터렉티브)", "Matplotlib (정적)"])
        self.combo_lib.currentTextChanged.connect(self.update_viz_lib)
        cf.addRow("시각화 엔진:", self.combo_lib)
        layout.addWidget(core_group)
        
        layout.addStretch()

    def setup_cli_tab(self):
        self.cli_container = QWidget()
        cl = QVBoxLayout(self.cli_container)
        self.cli_output = QTextEdit(); self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'Consolas', monospace; background-color: #f8f9fa; font-size: 10pt;")
        cl.addWidget(self.cli_output)
        
        il = QHBoxLayout()
        self.cli_input = QLineEdit(); self.cli_input.setPlaceholderText("명령어/슬래시 명령 입력 (/help)")
        self.cli_input.setStyleSheet("font-family: 'Consolas', monospace; height: 35px;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        self.btn_exec = QPushButton("전송"); self.btn_exec.clicked.connect(self.execute_cli)
        il.addWidget(self.cli_input); il.addWidget(self.btn_exec)
        cl.addLayout(il)

    def init_menu_and_toolbar(self):
        menubar = self.menuBar(); file_menu = menubar.addMenu("파일 (&F)")
        for name, callback in [
            ("데이터 불러오기...", self.load_data_async), ("프로젝트 세션 저장...", self.save_project),
            ("프로젝트 세션 열기...", self.load_project), (None, None),
            ("지능형 팝업 설정...", self.open_settings), (None, None), ("종료", self.close)
        ]:
            if name is None: file_menu.addSeparator()
            else: file_menu.addAction(name).triggered.connect(callback)
        
        toolbar = QToolBar("Enterprise Hub AI Core"); self.addToolBar(toolbar)
        bt_set = QPushButton("지능형 코어 설정"); bt_set.clicked.connect(self.open_settings); toolbar.addWidget(bt_set)

    def open_settings(self):
        diag = SettingsDialog(self, self.app_settings)
        if diag.exec(): self.app_settings = diag.get_settings(); self.status_label.setText("지능형 설정 업데이트됨.")

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

        self.start_worker(_task, on_status=lambda _: self.status_label.setText("지능형 분석 가동 중..."))

    def display_data_mapping(self, df, encoding):
        info = f"<b>인코딩:</b> <span style='color: #27ae60;'>{encoding}</span><br>"
        info += f"<b>상태:</b> {df.shape[0]:,} 행 x {df.shape[1]:,} 열<br><br>"
        info += "<b>지능형 맵핑 스키마:</b><br>"
        for col in df.columns:
            dtype = str(df[col].dtype)
            info += f"- {col}: <span style='color: #e67e22;'>{dtype}</span><br>"
        self.lbl_data_info.setText(info)

    def perform_intelligence_analysis(self, df):
        report = IntelligenceCore.analyze_full_profile(df)
        txt = "<b>[지능형 데이터 패턴 분석 보고서]</b><br><br>"
        for insight in report["insights"]: txt += f"- {insight}<br><br>"
        self.insight_output.setHtml(txt)
        if report["suggestions"]:
            best = report["suggestions"][0]
            self.combo_plot_type.setCurrentText(best["type"])
            self.combo_x.setCurrentText(best["x"])
            if best["y"]: self.combo_y.setCurrentText(best["y"])
            self.status_label.setText(f"AI 코어: {best['desc']} 제안.")
            self.generate_plot_dispatch()

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "데이터 로드", "", "데이터 파일 (*.csv *.xlsx *.xls)")
        if p: self.load_data_file_async(p)

    def smart_save_project(self, path):
        ui = {
            "Geometry": self.saveGeometry().toHex().data().decode(),
            "State": self.saveState().toHex().data().decode(),
            "EngineMode": self.engine_mode, "VizLib": self.viz_lib, "SelectedTab": self.central_tabs.currentIndex()
        }
        success, msg = SessionManager.save_project(path, self.variables, ui, self.app_settings["compress"])
        if success: self.status_label.setText(msg)
        else: QMessageBox.critical(self, "저장 오류", msg)

    def save_project(self):
        p, _ = QFileDialog.getSaveFileName(self, "Enterprise 프로젝트 저장", "", "Datamixer Project (*.dmx)")
        if p: self.smart_save_project(p)

    def load_project(self):
        p, _ = QFileDialog.getOpenFileName(self, "프로젝트 열기", "", "Project (*.dmx)")
        if p: self.load_project_file(p)

    def load_project_file(self, p):
        s, r = SessionManager.load_project(p)
        if s:
            self.variables = r["variables"]; self.update_explorer(); ui = r["ui_state"]
            if "Geometry" in ui: self.restoreGeometry(QByteArray.fromHex(ui["Geometry"].encode()))
            if "State" in ui: self.restoreState(QByteArray.fromHex(ui["State"].encode()))
            if "EngineMode" in ui: self.combo_engine.setCurrentText(ui["EngineMode"])
            if "VizLib" in ui: self.combo_lib.setCurrentText(ui["VizLib"])
            self.status_label.setText(f"세션 복구 완료: {os.path.basename(p)}")
        else: QMessageBox.critical(self, "오류", r)

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
            def _mpl(): return VizManager.generate_matplotlib_fig(dp, t, x, y)
            def _mpl_ok(r):
                if r[0]:
                    for i in reversed(range(self.static_canvas_layout.count())): self.static_canvas_layout.itemAt(i).widget().setParent(None)
                    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
                    self.static_canvas_layout.addWidget(FigureCanvas(r[0]))
            self.start_worker(_mpl, on_success=_mpl_ok)

    def execute_cli(self):
        t = self.cli_input.text()
        if not t: return
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
