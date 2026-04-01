import sys
import io
import os
import contextlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTableView, QTabWidget, QLabel,
    QPlainTextEdit, QLineEdit, QComboBox, QGroupBox, QFormLayout,
    QMessageBox, QHeaderView, QTextEdit, QDockWidget, QTreeWidget,
    QTreeWidgetItem, QToolBar, QStatusBar, QFrame, QSplitter
)
from PySide6.QtCore import Qt, Slot, QUrl, QSize, QByteArray, QIODevice
from PySide6.QtGui import QAction, QIcon, QDragEnterEvent, QDropEvent

# Internal imports
from .models import PandasModel
from .utils import detect_encoding_parallel
from .worker import GenericWorker
from .engine import DataEngine
from .session import SessionManager
from .viz_manager import VizManager

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - Ultimate Intelligence Suite")
        self.resize(1400, 950)
        self.setAcceptDrops(True)
        
        # Enterprise Data Store
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}
        self.workers = []
        
        self.init_ui()
        self.init_menu_and_toolbar()
        
        # Default Settings
        self.engine_mode = "Polars" 
        self.viz_lib = "Plotly"
        
        self.status_label.setText("Enterprise Edition 로딩 완료 - 데이터 허브 활성화")

    def init_ui(self):
        self.setDockNestingEnabled(True)
        
        # 1. Central Widget (Tabs)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # 1-1. View Tab (Table)
        self.view_tab = QWidget()
        view_layout = QVBoxLayout(self.view_tab)
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setStyleSheet("QTableView { selection-background-color: rgba(135, 206, 235, 100); }")
        view_layout.addWidget(self.table_view)
        self.central_tabs.addTab(self.view_tab, "데이터 그리드")
        
        # 1-2. Visualizer Tab (Plotly / Matplotlib)
        self.viz_container = QWidget()
        viz_vbox = QVBoxLayout(self.viz_container)
        self.browser = QWebEngineView()
        self.static_canvas_container = QWidget()
        self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        
        viz_vbox.addWidget(self.browser)
        viz_vbox.addWidget(self.static_canvas_container)
        self.static_canvas_container.hide()
        
        self.central_tabs.addTab(self.viz_container, "분석 시각화")
        
        # 2. Docks
        self.setup_docks()
        
        # 5. Bottom Status Bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_label = QLabel("준비")
        self.status_bar.addWidget(self.status_label)

    def setup_docks(self):
        # Explorer
        self.explorer_dock = QDockWidget("데이터 탐색기", self)
        self.explorer_dock.setObjectName("ExplorerDock")
        self.explorer_tree = QTreeWidget()
        self.explorer_tree.setHeaderLabels(["변수명", "형태", "메모리"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked)
        self.explorer_dock.setWidget(self.explorer_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # Console
        self.console_dock = QDockWidget("명령 핸들러 (CLI)", self)
        self.console_dock.setObjectName("ConsoleDock")
        self.setup_cli_tab()
        self.console_dock.setWidget(self.cli_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # Properties & Settings
        self.props_dock = QDockWidget("엔진 및 속성", self)
        self.props_dock.setObjectName("PropsDock")
        self.setup_props_panel()
        self.props_dock.setWidget(self.props_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.props_dock)

    def setup_props_panel(self):
        self.props_container = QWidget()
        layout = QVBoxLayout(self.props_container)
        
        # Engine Settings
        engine_group = QGroupBox("시스템 코어 설정")
        engine_form = QFormLayout(engine_group)
        self.combo_engine = QComboBox()
        self.combo_engine.addItems(["Polars (고성능)", "Pandas (표준)"])
        self.combo_engine.currentTextChanged.connect(self.update_engine_mode)
        engine_form.addRow("데이터 엔진:", self.combo_engine)
        
        self.combo_lib = QComboBox()
        self.combo_lib.addItems(["Plotly (인터렉티브)", "Matplotlib (정적)"])
        self.combo_lib.currentTextChanged.connect(self.update_viz_lib)
        engine_form.addRow("시각화 라이브러리:", self.combo_lib)
        layout.addWidget(engine_group)

        # Plot Settings
        viz_group = QGroupBox("시각화 상세 설정")
        viz_form = QFormLayout(viz_group)
        self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        viz_form.addRow("그래프 종류:", self.combo_plot_type)
        self.combo_x = QComboBox()
        self.combo_y = QComboBox()
        viz_form.addRow("X축 필드:", self.combo_x)
        viz_form.addRow("Y축 필드:", self.combo_y)
        self.btn_plot = QPushButton("그래프 렌더링 실행")
        self.btn_plot.clicked.connect(self.generate_plot_dispatch)
        viz_form.addRow(self.btn_plot)
        layout.addWidget(viz_group)
        
        # Ops
        ops_group = QGroupBox("데이터 조작 파이프라인")
        ops_form = QFormLayout(ops_group)
        self.input_loc_rows = QLineEdit(":")
        self.input_loc_cols = QLineEdit(":")
        ops_form.addRow("loc Rows:", self.input_loc_rows)
        ops_form.addRow("loc Cols:", self.input_loc_cols)
        self.input_query = QLineEdit()
        ops_form.addRow("Query:", self.input_query)
        self.input_new_var = QLineEdit("df_processed")
        ops_form.addRow("저장될 변수명:", self.input_new_var)
        btn_apply = QPushButton("연산 파이프라인 적용")
        btn_apply.clicked.connect(self.run_engine_ops)
        ops_form.addRow(btn_apply)
        layout.addWidget(ops_group)
        
        layout.addStretch()

    def setup_cli_tab(self):
        self.cli_container = QWidget()
        layout = QVBoxLayout(self.cli_container)
        self.cli_output = QTextEdit()
        self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'Consolas', monospace; font-size: 10pt;")
        layout.addWidget(self.cli_output)
        
        input_layout = QHBoxLayout()
        self.cli_input = QLineEdit()
        self.cli_input.setPlaceholderText("엔진 직접 제어 명령어 입력... (예: self.df.info())")
        self.cli_input.setStyleSheet("font-family: 'Consolas', monospace; font-size: 10pt; height: 30px;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        self.btn_exec = QPushButton("실행")
        self.btn_exec.setFixedWidth(80)
        self.btn_exec.clicked.connect(self.execute_cli)
        input_layout.addWidget(self.cli_input)
        input_layout.addWidget(self.btn_exec)
        layout.addLayout(input_layout)

    def init_menu_and_toolbar(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("파일 (&F)")
        
        actions = [
            ("데이터 로드...", self.load_data_async),
            ("프로젝트 저장 (.dmx)...", self.save_project),
            ("프로젝트 열기 (.dmx)...", self.load_project),
            (None, None),
            ("종료", self.close)
        ]
        
        for name, callback in actions:
            if name is None:
                file_menu.addSeparator()
            else:
                act = QAction(name, self)
                act.triggered.connect(callback)
                file_menu.addAction(act)
        
        # Toolbar
        toolbar = QToolBar("Enterprise Tools")
        toolbar.setIconSize(QSize(24, 24))
        self.addToolBar(toolbar)
        
        # Add basic actions
        load_act = QAction("로드", self)
        load_act.triggered.connect(self.load_data_async)
        toolbar.addAction(load_act)
        
        save_act = QAction("저장", self)
        save_act.triggered.connect(self.save_project)
        toolbar.addAction(save_act)

    # --- Mode Management ---
    def update_engine_mode(self, text):
        self.engine_mode = "Polars" if "Polars" in text else "Pandas"
        self.status_label.setText(f"엔진 모드 전환: {self.engine_mode}")

    def update_viz_lib(self, text):
        self.viz_lib = "Plotly" if "Plotly" in text else "Matplotlib"
        if self.viz_lib == "Plotly":
            self.browser.show()
            self.static_canvas_container.hide()
        else:
            self.browser.hide()
            self.static_canvas_container.show()
        self.status_label.setText(f"시각화 라이브러리 전환: {self.viz_lib}")

    # --- Drag & Drop ---
    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event: QDropEvent):
        urls = event.mimeData().urls()
        if urls:
            file_path = urls[0].toLocalFile()
            if file_path.endswith(".dmx"):
                self.load_project_file(file_path)
            else:
                self.load_data_file_async(file_path)

    # --- Persistence Architecture ---
    def save_project(self):
        path, _ = QFileDialog.getSaveFileName(self, "Enterprise 프로젝트 저장", "", "Datamixer Project (*.dmx)")
        if path:
            ui_state = {
                "Geometry": self.saveGeometry().toHex().data().decode(),
                "State": self.saveState().toHex().data().decode(),
                "SelectedTab": self.central_tabs.currentIndex(),
                "EngineMode": self.engine_mode,
                "VizLib": self.viz_lib
            }
            success, msg = SessionManager.save_project(path, self.variables, ui_state)
            if success: self.status_label.setText(msg)
            else: QMessageBox.critical(self, "저장 오류", msg)

    def load_project(self):
        path, _ = QFileDialog.getOpenFileName(self, "Enterprise 프로젝트 열기", "", "Datamixer Project (*.dmx)")
        if path:
            self.load_project_file(path)

    def load_project_file(self, path):
        success, res = SessionManager.load_project(path)
        if success:
            self.variables = res["variables"]
            self.update_explorer()
            # Restore UI State
            ui_state = res["ui_state"]
            if "Geometry" in ui_state:
                self.restoreGeometry(QByteArray.fromHex(ui_state["Geometry"].encode()))
            if "State" in ui_state:
                self.restoreState(QByteArray.fromHex(ui_state["State"].encode()))
            if "EngineMode" in ui_state:
                self.combo_engine.setCurrentText(ui_state["EngineMode"])
            if "VizLib" in ui_state:
                self.combo_lib.setCurrentText(ui_state["VizLib"])
            
            QMessageBox.information(self, "로드 완료", "프로젝트 세션이 성공적으로 복구되었습니다.")
        else:
            QMessageBox.critical(self, "오류", res)

    # --- Worker Core ---
    def start_worker(self, func, *args, on_success=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        worker.error_occurred.connect(lambda msg: QMessageBox.critical(self, "실행 오류", msg))
        if on_status: worker.status_update.connect(on_status)
        
        worker.finished.connect(lambda: self.set_ui_enabled(True))
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        
        self.workers.append(worker)
        self.set_ui_enabled(False)
        worker.start()

    # --- Data Logic ---
    def load_data_async(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "데이터 소스 로드", "", "데이터 파일 (*.csv *.xlsx *.xls)")
        if file_path:
            self.load_data_file_async(file_path)

    def load_data_file_async(self, file_path):
        def _task():
            encoding = detect_encoding_parallel(file_path) if file_path.endswith(".csv") else "utf-8"
            success, df, msg = DataEngine.load_data(file_path, encoding)
            if not success: raise Exception(msg)
            return df, msg

        def _on_success(res):
            df, msg = res
            var_name = os.path.basename(file_path).replace(".", "_")
            self.variables[var_name] = df
            self.df = df
            self.update_explorer()
            self.update_table()
            self.update_viz_combos()
            self.status_label.setText(f"{msg}: {file_path}")

        self.start_worker(_task, on_status=lambda _: self.status_label.setText("데이터 엔진 가동 중..."))

    def update_explorer(self):
        self.explorer_tree.clear()
        for var, data in self.variables.items():
            if isinstance(data, pd.DataFrame):
                mem = f"{data.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
                item = QTreeWidgetItem([var, f"{data.shape[0]}x{data.shape[1]}", mem])
                self.explorer_tree.addTopLevelItem(item)

    def on_variable_clicked(self, item):
        var_name = item.text(0)
        self.df = self.variables[var_name]
        self.update_table()
        self.update_viz_combos()

    def update_table(self):
        if not self.df.empty:
            self.table_view.setModel(PandasModel(self.df))

    def update_viz_combos(self):
        cols = list(self.df.columns)
        self.combo_x.clear()
        self.combo_y.clear()
        self.combo_x.addItems(cols)
        self.combo_y.addItems(cols)

    # --- Visualization Dispatcher ---
    def generate_plot_dispatch(self):
        if self.df.empty: return
        plot_type = self.combo_plot_type.currentText()
        x = self.combo_x.currentText()
        y = self.combo_y.currentText()
        
        # Intelligent Sampling
        sample_df = self.df
        if len(self.df) > 100000:
            sample_df = self.df.sample(100000)

        if self.viz_lib == "Plotly":
            def _plotly_task():
                return VizManager.generate_plotly_html(sample_df, plot_type, x, y)
            
            def _on_plotly_done(res):
                html_path, msg = res
                if html_path:
                    self.browser.setUrl(QUrl.fromLocalFile(html_path))
                    self.central_tabs.setCurrentIndex(1)
                else: QMessageBox.warning(self, "Plotly", msg)
            
            self.start_worker(_plotly_task, on_success=_on_plotly_done)
        
        else: # Matplotlib
            def _mpl_task():
                return VizManager.generate_matplotlib_fig(sample_df, plot_type, x, y)
            
            def _on_mpl_done(res):
                fig, msg = res
                if fig:
                    # Clear canvas container
                    for i in reversed(range(self.static_canvas_layout.count())): 
                        self.static_canvas_layout.itemAt(i).widget().setParent(None)
                    
                    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
                    canvas = FigureCanvas(fig)
                    self.static_canvas_layout.addWidget(canvas)
                    self.central_tabs.setCurrentIndex(1)
                else: QMessageBox.warning(self, "Matplotlib", msg)
            
            self.start_worker(_mpl_task, on_success=_on_mpl_done)

    # --- Engine Ops ---
    def run_engine_ops(self):
        if self.df.empty: return
        rows_str = self.input_loc_rows.text()
        cols_str = self.input_loc_cols.text()
        query_str = self.input_query.text()
        new_var_name = self.input_new_var.text()

        def _task():
            res_df = self.df
            if rows_str != ":" or cols_str != ":":
                rows = eval(rows_str) if rows_str != ":" else slice(None)
                cols = eval(cols_str) if cols_str != ":" else slice(None)
                res_df = res_df.loc[rows, cols]
            if query_str:
                res_df = res_df.query(query_str)
            return res_df

        def _on_success(res):
            self.variables[new_var_name] = res
            self.update_explorer()
            QMessageBox.information(self, "성공", f"엔진 연산 완료 및 저장: {new_var_name}")

        self.start_worker(_task, on_status=lambda _: self.status_label.setText("데이터 엔진 파이프라인 가동..."))

    def execute_cli(self):
        command = self.cli_input.text()
        if not command: return
        self.cli_output.append(f"<b style='color: #2c5f2d;'>>>> {command}</b>")
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                result = eval(command, {"self": self, "pd": pd, "np": np, "px": px, "df": self.df}, self.variables)
            output = f.getvalue()
            if output: self.cli_output.append(output.strip())
            if result is not None: self.cli_output.append(str(result))
        except Exception:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(command, {"self": self, "pd": pd, "np": np, "px": px, "df": self.df}, self.variables)
                output = f.getvalue()
                if output: self.cli_output.append(output.strip())
            except Exception as e2:
                self.cli_output.append(f"<span style='color: #c0392b;'>Error: {str(e2)}</span>")
        
        self.cli_input.clear()
        self.cli_output.ensureCursorVisible()
        self.update_explorer()
        self.update_table()
