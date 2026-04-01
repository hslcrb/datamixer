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

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - Ultimate Intelligence Suite")
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
        
        # Default State
        self.engine_mode = "Polars" 
        self.viz_lib = "Plotly"
        self.status_label.setText("Enterprise Edition 로딩 완료 - 지능형 분석 코어 활성")

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # Data View Tab
        self.view_tab = QWidget()
        view_layout = QVBoxLayout(self.view_tab)
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setStyleSheet("QTableView { selection-background-color: rgba(135, 206, 235, 100); }")
        view_layout.addWidget(self.table_view)
        self.central_tabs.addTab(self.view_tab, "데이터 인사이트 그리드")
        
        # Visualizer Tab
        self.viz_container = QWidget()
        viz_vbox = QVBoxLayout(self.viz_container)
        self.browser = QWebEngineView()
        self.static_canvas_container = QWidget()
        self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        viz_vbox.addWidget(self.browser)
        viz_vbox.addWidget(self.static_canvas_container)
        self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_container, "지능형 시각화 리포트")
        
        self.setup_docks()
        
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_label = QLabel("Ready")
        self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        # Explorer Dock
        self.explorer_dock = QDockWidget("데이터 탐색기 (Vars)", self)
        self.explorer_dock.setObjectName("ExplorerDock")
        self.explorer_tree = QTreeWidget()
        self.explorer_tree.setHeaderLabels(["변수명", "형태 (Shape)", "메모리 점유"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked)
        self.explorer_dock.setWidget(self.explorer_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # Console Dock
        self.console_dock = QDockWidget("명령 핸들러 (CLI / REPL)", self)
        self.console_dock.setObjectName("ConsoleDock")
        self.setup_cli_tab()
        self.console_dock.setWidget(self.cli_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # Properties (AI analysis results show here temporarily)
        self.props_dock = QDockWidget("지능형 제어 및 속성", self)
        self.props_dock.setObjectName("PropsDock")
        self.setup_props_panel()
        self.props_dock.setWidget(self.props_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock)

    def setup_props_panel(self):
        self.props_container = QWidget()
        layout = QVBoxLayout(self.props_container)
        
        # Info Box (Immediate feedback)
        info_group = QGroupBox("데이터 상세 지명 (Mapping)")
        self.info_layout = QVBoxLayout(info_group)
        self.lbl_data_info = QLabel("데이터를 로드하면 정보가 표시됩니다.")
        self.lbl_data_info.setWordWrap(True)
        self.lbl_data_info.setStyleSheet("color: #2c3e50; font-weight: bold;")
        self.info_layout.addWidget(self.lbl_data_info)
        layout.addWidget(info_group)

        # Plot Settings
        viz_group = QGroupBox("시각화 엔진 제어")
        viz_form = QFormLayout(viz_group)
        self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        viz_form.addRow("그래프 종류:", self.combo_plot_type)
        self.combo_x = QComboBox(); self.combo_y = QComboBox()
        viz_form.addRow("X축 필드:", self.combo_x)
        viz_form.addRow("Y축 필드:", self.combo_y)
        self.btn_plot = QPushButton("그래프 렌더링")
        self.btn_plot.clicked.connect(self.generate_plot_dispatch)
        viz_form.addRow(self.btn_plot)
        layout.addWidget(viz_group)
        
        # Engine Control
        core_group = QGroupBox("시스템 코어 설정")
        core_form = QFormLayout(core_group)
        self.combo_engine = QComboBox()
        self.combo_engine.addItems(["Polars (고성능)", "Pandas (표준)"])
        self.combo_engine.currentTextChanged.connect(self.update_engine_mode)
        core_form.addRow("데이터 엔진:", self.combo_engine)
        self.combo_lib = QComboBox()
        self.combo_lib.addItems(["Plotly (인터렉티브)", "Matplotlib (정적)"])
        self.combo_lib.currentTextChanged.connect(self.update_viz_lib)
        core_form.addRow("시각화 라이브러리:", self.combo_lib)
        layout.addWidget(core_group)
        
        layout.addStretch()

    def setup_cli_tab(self):
        self.cli_container = QWidget()
        layout = QVBoxLayout(self.cli_container)
        self.cli_output = QTextEdit()
        self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'Consolas', monospace; background-color: #f8f9fa;")
        layout.addWidget(self.cli_output)
        
        input_layout = QHBoxLayout()
        self.cli_input = QLineEdit()
        self.cli_input.setPlaceholderText("명령어 입력 (예: /help, df.describe())")
        self.cli_input.setStyleSheet("font-family: 'Consolas', monospace; height: 35px;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        self.btn_exec = QPushButton("실행")
        self.btn_exec.clicked.connect(self.execute_cli)
        input_layout.addWidget(self.cli_input)
        input_layout.addWidget(self.btn_exec)
        layout.addLayout(input_layout)

    def init_menu_and_toolbar(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("파일 (&F)")
        
        for name, callback in [
            ("데이터 로드...", self.load_data_async), ("프로젝트 저장...", self.save_project),
            ("프로젝트 열기...", self.load_project), (None, None),
            ("환경 설정...", self.open_settings), (None, None), ("종료", self.close)
        ]:
            if name is None: file_menu.addSeparator()
            else: file_menu.addAction(name).triggered.connect(callback)
        
        toolbar = QToolBar("Enterprise Hub")
        self.addToolBar(toolbar)
        bt_set = QPushButton("환경 설정"); bt_set.clicked.connect(self.open_settings); toolbar.addWidget(bt_set)

    def open_settings(self):
        diag = SettingsDialog(self, self.app_settings)
        if diag.exec():
            self.app_settings = diag.get_settings()
            self.status_label.setText("설정 업데이트됨.")

    def update_engine_mode(self, text): self.engine_mode = "Polars" if "Polars" in text else "Pandas"
    def update_viz_lib(self, text):
        self.viz_lib = "Plotly" if "Plotly" in text else "Matplotlib"
        if self.viz_lib == "Plotly": self.browser.show(); self.static_canvas_container.hide()
        else: self.browser.hide(); self.static_canvas_container.show()

    def smart_save_project(self, path):
        ui_state = {
            "Geometry": self.saveGeometry().toHex().data().decode(),
            "State": self.saveState().toHex().data().decode(),
            "EngineMode": self.engine_mode, "VizLib": self.viz_lib, "SelectedTab": self.central_tabs.currentIndex()
        }
        success, msg = SessionManager.smart_save(path, self.variables, ui_state, self.app_settings["compress"])
        if success: self.status_label.setText(msg)
        else: QMessageBox.critical(self, "저장 오류", msg)

    def load_project_file(self, path):
        success, res = SessionManager.smart_load(path)
        if success:
            self.variables = res["variables"]; self.update_explorer()
            ui_state = res["ui_state"]
            if "Geometry" in ui_state: self.restoreGeometry(QByteArray.fromHex(ui_state["Geometry"].encode()))
            if "State" in ui_state: self.restoreState(QByteArray.fromHex(ui_state["State"].encode()))
            if "EngineMode" in ui_state: self.combo_engine.setCurrentText(ui_state["EngineMode"])
            if "VizLib" in ui_state: self.combo_lib.setCurrentText(ui_state["VizLib"])
            self.status_label.setText(f"프로젝트 로드 완료: {os.path.basename(path)}")
        else: QMessageBox.critical(self, "오류", res)

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
            # Immediate Mapping Info and AI Analysis
            self.display_data_mapping(df, encoding)
            if self.app_settings["auto_analysis"]: self.perform_ai_analysis(df)

        self.start_worker(_task, on_status=lambda _: self.status_label.setText("데이터 엔진 로드 중..."))

    def display_data_mapping(self, df, encoding):
        """Immediately display schema information."""
        info = f"<b>인코딩:</b> {encoding}<br>"
        info += f"<b>상태:</b> {df.shape[0]} 행 x {df.shape[1]} 열<br><br>"
        info += "<b>필드 속성:</b><br>"
        for col in df.columns:
            dtype = str(df[col].dtype)
            info += f"- {col}: <span style='color: #2980b9;'>{dtype}</span><br>"
        self.lbl_data_info.setText(info)

    def perform_ai_analysis(self, df):
        """Automatically analyze data and suggest best chart."""
        nums = df.select_dtypes(include=[np.number]).columns.tolist()
        cats = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        suggest_type = "히스토그램"
        x_suggest = ""
        y_suggest = ""
        
        if len(nums) >= 2:
            suggest_type = "산점도"
            x_suggest, y_suggest = nums[0], nums[1]
        elif len(nums) == 1 and len(cats) >= 1:
            suggest_type = "박스 플롯"
            x_suggest, y_suggest = cats[0], nums[0]
        elif len(nums) == 1:
            suggest_type = "히스토그램"
            x_suggest = nums[0]
        
        if x_suggest:
            self.combo_plot_type.setCurrentText(suggest_type)
            self.combo_x.setCurrentText(x_suggest)
            if y_suggest: self.combo_y.setCurrentText(y_suggest)
            self.status_label.setText(f"AI 지능형 분석: {suggest_type} 추천됨.")
            self.generate_plot_dispatch()

    def load_data_async(self):
        path, _ = QFileDialog.getOpenFileName(self, "데이터 로드", "", "데이터 파일 (*.csv *.xlsx *.xls)")
        if path: self.load_data_file_async(path)

    def save_project(self):
        path, _ = QFileDialog.getSaveFileName(self, "프로젝트 저장", "", "Datamixer Project (*.dmx)")
        if path: self.smart_save_project(path)

    def load_project(self):
        path, _ = QFileDialog.getOpenFileName(self, "프로젝트 열기", "", "Project (*.dmx)")
        if path: self.load_project_file(path)

    def on_variable_clicked(self, item):
        var_name = item.text(0)
        self.df = self.variables[var_name]; self.update_table(); self.update_viz_combos()

    def update_table(self):
        if not self.df.empty: self.table_view.setModel(PandasModel(self.df))

    def update_viz_combos(self):
        cols = list(self.df.columns)
        self.combo_x.clear(); self.combo_y.clear()
        self.combo_x.addItems(cols); self.combo_y.addItems(cols)

    def generate_plot_dispatch(self):
        if self.df.empty: return
        t = self.combo_plot_type.currentText(); x = self.combo_x.currentText(); y = self.combo_y.currentText()
        df_plot = self.df if len(self.df) < 100000 else self.df.sample(100000)
        if self.viz_lib == "Plotly":
            self.start_worker(lambda: VizManager.generate_plotly_html(df_plot, t, x, y),
                              on_success=lambda r: self.browser.setUrl(QUrl.fromLocalFile(r[0])) if r[0] else None)
        else:
            def _mpl(): return VizManager.generate_matplotlib_fig(df_plot, t, x, y)
            def _mpl_ok(r):
                if r[0]:
                    for i in reversed(range(self.static_canvas_layout.count())): self.static_canvas_layout.itemAt(i).widget().setParent(None)
                    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
                    self.static_canvas_layout.addWidget(FigureCanvas(r[0]))
            self.start_worker(_mpl, on_success=_mpl_ok)

    def run_engine_ops(self):
        if self.df.empty: return
        r, c = self.input_loc_rows.text(), self.input_loc_cols.text()
        q, nv = self.input_query.text(), self.input_new_var.text()
        def _t():
            res = self.df
            if r != ":" or c != ":": res = res.loc[eval(r) if r != ":" else slice(None), eval(c) if c != ":" else slice(None)]
            if q: res = res.query(q)
            return res
        def _ok(res): self.variables[nv] = res; self.update_explorer(); QMessageBox.information(self, "성공", f"연산 완료: {nv}")
        self.start_worker(_t)

    def execute_cli(self):
        t = self.cli_input.text(); if not t: return
        self.cli_output.append(f"<b style='color: #2e7d32;'>>>> {t}</b>")
        res = self.repl.process_input(t)
        if res: self.cli_output.append(res)
        self.cli_input.clear(); self.cli_output.ensureCursorVisible(); self.update_explorer(); self.update_table()

    def start_worker(self, func, *args, on_success=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        worker.error_occurred.connect(lambda m: QMessageBox.critical(self, "오류", m))
        if on_status: worker.status_update.connect(on_status)
        worker.finished.connect(lambda: self.setEnabled(True))
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        self.workers.append(worker); self.setEnabled(False); worker.start()

    def dragEnterEvent(self, e): 
        if e.mimeData().hasUrls(): e.accept()
        else: e.ignore()
    def dropEvent(self, e):
        urls = e.mimeData().urls()
        if urls:
            p = urls[0].toLocalFile()
            if p.endswith(".dmx"): self.load_project_file(p)
            else: self.load_data_file_async(p)
