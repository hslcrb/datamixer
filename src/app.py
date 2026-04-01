import sys
import io
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
    QTreeWidgetItem, QToolBar, QStatusBar, QFrame
)
from PySide6.QtCore import Qt, Slot, QUrl, QSize
from PySide6.QtGui import QAction, QIcon

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
        self.setWindowTitle("Datamixer Enterprise - BigData Intelligence Hub")
        self.resize(1400, 950)
        
        # Enterprise Data Store
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}
        self.workers = []
        
        self.init_ui()
        self.init_menu_and_toolbar()
        self.status_label.setText("Enterprise Edition 로딩 완료 - 실시간 분석 대기 중")

    def init_ui(self):
        # Allow docking anywhere
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
        self.central_tabs.addTab(self.view_tab, "데이터 시트")
        
        # 1-2. Visualizer Tab (Plotly)
        self.viz_tab = QWidget()
        viz_layout = QVBoxLayout(self.viz_tab)
        self.browser = QWebEngineView()
        viz_layout.addWidget(self.browser)
        self.central_tabs.addTab(self.viz_tab, "인터렉티브 시각화")
        
        # 2. Left Dock: Explorer
        self.explorer_dock = QDockWidget("데이터 탐색기", self)
        self.explorer_tree = QTreeWidget()
        self.explorer_tree.setHeaderLabels(["변수명", "데이터 형태"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked)
        self.explorer_dock.setWidget(self.explorer_tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # 3. Bottom Dock: Console
        self.console_dock = QDockWidget("명령 콘솔 (CLI)", self)
        self.setup_cli_tab()
        self.console_dock.setWidget(self.cli_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # 4. Right Dock: Info & Ops
        self.ops_dock = QDockWidget("고급 필터링 및 통계", self)
        self.setup_ops_panel()
        self.ops_dock.setWidget(self.ops_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.ops_dock)
        
        # 5. Bottom Status Bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_label = QLabel("준비")
        self.status_bar.addWidget(self.status_label)

    def init_menu_and_toolbar(self):
        # Menu
        menubar = self.menuBar()
        file_menu = menubar.addMenu("파일 (&F)")
        
        load_action = QAction("데이터 로드 (CSV/Excel)", self)
        load_action.triggered.connect(self.load_data_async)
        file_menu.addAction(load_action)
        
        save_proj_action = QAction("프로젝트 저장 (.dmx)", self)
        save_proj_action.triggered.connect(self.save_project)
        file_menu.addAction(save_proj_action)
        
        load_proj_action = QAction("프로젝트 불러오기 (.dmx)", self)
        load_proj_action.triggered.connect(self.load_project)
        file_menu.addAction(load_proj_action)
        
        file_menu.addSeparator()
        exit_action = QAction("종료", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Toolbar
        toolbar = QToolBar("기본 툴바")
        self.addToolBar(toolbar)
        toolbar.addAction(load_action)
        toolbar.addAction(save_proj_action)

    def setup_ops_panel(self):
        self.ops_container = QWidget()
        layout = QVBoxLayout(self.ops_container)
        
        # Plot Settings (Moved to Dock for professionalism)
        viz_group = QGroupBox("시각화 엔진 설정")
        viz_form = QFormLayout(viz_group)
        self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        viz_form.addRow("형식:", self.combo_plot_type)
        self.combo_x = QComboBox()
        self.combo_y = QComboBox()
        viz_form.addRow("X축:", self.combo_x)
        viz_form.addRow("Y축:", self.combo_y)
        self.btn_plot = QPushButton("인터렉티브 생성")
        self.btn_plot.clicked.connect(self.generate_interactive_plot)
        viz_form.addRow(self.btn_plot)
        layout.addWidget(viz_group)
        
        # loc/query moved here
        ops_group = QGroupBox("고속 데이터 정제 (Engine)")
        ops_form = QFormLayout(ops_group)
        self.input_loc_rows = QLineEdit(":")
        self.input_loc_cols = QLineEdit(":")
        ops_form.addRow("loc (rows):", self.input_loc_rows)
        ops_form.addRow("loc (cols):", self.input_loc_cols)
        self.input_query = QLineEdit()
        ops_form.addRow("Query:", self.input_query)
        self.input_new_var = QLineEdit("df_new")
        ops_form.addRow("저장 변수:", self.input_new_var)
        btn_apply = QPushButton("엔진 연산 실행 (Async)")
        btn_apply.clicked.connect(self.run_engine_ops)
        ops_form.addRow(btn_apply)
        layout.addWidget(ops_group)
        layout.addStretch()

    def setup_cli_tab(self):
        self.cli_container = QWidget()
        layout = QVBoxLayout(self.cli_container)
        self.cli_output = QTextEdit()
        self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'Consolas', monospace;")
        layout.addWidget(self.cli_output)
        
        input_layout = QHBoxLayout()
        self.cli_input = QLineEdit()
        self.cli_input.setPlaceholderText("명령어 입력... (예: self.df.describe())")
        self.cli_input.setStyleSheet("font-family: 'Consolas', monospace;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        self.btn_exec = QPushButton("실행")
        self.btn_exec.clicked.connect(self.execute_cli)
        input_layout.addWidget(self.cli_input)
        input_layout.addWidget(self.btn_exec)
        layout.addLayout(input_layout)

    # --- Async Infrastructure ---
    def start_worker(self, func, *args, on_success=None, on_error=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        worker.error_occurred.connect(lambda msg: QMessageBox.critical(self, "엔진 오류", msg))
        if on_status: worker.status_update.connect(on_status)
        else: worker.status_update.connect(lambda msg: self.status_label.setText(msg))
        
        worker.finished.connect(lambda: self.set_ui_enabled(True))
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        
        self.workers.append(worker)
        self.set_ui_enabled(False)
        worker.start()

    def set_ui_enabled(self, enabled):
        self.setEnabled(enabled)

    # --- Enterprise Logic ---
    def load_data_async(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "데이터 불러오기", "", "데이터 파일 (*.csv *.xlsx *.xls)")
        if not file_path: return

        def _task():
            encoding = detect_encoding_parallel(file_path) if file_path.endswith(".csv") else "utf-8"
            success, df, msg = DataEngine.load_data(file_path, encoding)
            if not success: raise Exception(msg)
            return df, msg

        def _on_success(res):
            df, msg = res
            self.df = df
            self.variables["df"] = df
            self.update_explorer()
            self.update_table()
            self.update_viz_combos()
            self.status_label.setText(f"{msg}: {file_path}")

        self.start_worker(_task, on_success=_on_success)

    def save_project(self):
        path, _ = QFileDialog.getSaveFileName(self, "프로젝트 저장", "", "Datamixer Project (*.dmx)")
        if path:
            success, msg = SessionManager.save_project(path, self.variables)
            if success: QMessageBox.information(self, "저장", msg)
            else: QMessageBox.critical(self, "오류", msg)

    def load_project(self):
        path, _ = QFileDialog.getOpenFileName(self, "프로젝트 열기", "", "Datamixer Project (*.dmx)")
        if path:
            success, res = SessionManager.load_project(path)
            if success:
                self.variables = res
                self.update_explorer()
                QMessageBox.information(self, "로드", "성공적으로 프로젝트를 불러왔습니다.")
            else:
                QMessageBox.critical(self, "오류", res)

    def update_explorer(self):
        self.explorer_tree.clear()
        for var, data in self.variables.items():
            if isinstance(data, pd.DataFrame):
                item = QTreeWidgetItem([var, f"{data.shape[0]}x{data.shape[1]}"])
                self.explorer_tree.addTopLevelItem(item)

    def on_variable_clicked(self, item):
        var_name = item.text(0)
        self.df = self.variables[var_name]
        self.update_table()
        self.update_viz_combos()
        self.status_label.setText(f"탐색 변수 전환: {var_name}")

    def update_table(self):
        if not self.df.empty:
            model = PandasModel(self.df)
            self.table_view.setModel(model)

    def update_viz_combos(self):
        cols = list(self.df.columns)
        self.combo_x.clear()
        self.combo_y.clear()
        self.combo_x.addItems(cols)
        self.combo_y.addItems(cols)

    def generate_interactive_plot(self):
        if self.df.empty: return
        plot_type = self.combo_plot_type.currentText()
        x = self.combo_x.currentText()
        y = self.combo_y.currentText()
        
        def _task():
            # Sampling for speed on large data
            sample_df = self.df
            if len(self.df) > 100000:
                sample_df = self.df.sample(100000)
            return VizManager.generate_plotly_html(sample_df, plot_type, x, y)

        def _on_success(res):
            html_path, msg = res
            if html_path:
                self.browser.setUrl(QUrl.fromLocalFile(html_path))
                self.central_tabs.setCurrentIndex(1) # Switch to Viz Tab
            else:
                QMessageBox.warning(self, "그래프", msg)

        self.start_worker(_task, on_success=_on_success)

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
            QMessageBox.information(self, "성공", f"엔진 연산 완료: {new_var_name}")

        self.start_worker(_task, on_success=_on_success)

    def execute_cli(self):
        command = self.cli_input.text()
        if not command: return
        self.cli_output.append(f"<b style='color: #2e7d32;'>>>> {command}</b>")
        
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                # Ensure we pass current app variables
                result = eval(command, {"self": self, "pd": pd, "np": np, "px": px}, self.variables)
            output = f.getvalue()
            if output: self.cli_output.append(output.strip())
            if result is not None: self.cli_output.append(str(result))
        except Exception:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(command, {"self": self, "pd": pd, "np": np, "px": px}, self.variables)
                output = f.getvalue()
                if output: self.cli_output.append(output.strip())
            except Exception as e2:
                self.cli_output.append(f"<span style='color: #d32f2f;'>Error: {str(e2)}</span>")
        
        self.cli_input.clear()
        self.cli_output.ensureCursorVisible()
        self.update_explorer()
        self.update_table()
