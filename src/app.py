import sys
import io
import contextlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTableView, QTabWidget, QLabel,
    QPlainTextEdit, QLineEdit, QComboBox, QGroupBox, QFormLayout,
    QMessageBox, QHeaderView, QTextEdit
)
from PySide6.QtCore import Qt, Slot

# Internal imports
from .models import PandasModel
from .utils import detect_encoding_parallel
from .worker import GenericWorker

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BigData Explorer Pro V2 - Enterprise Edition (Async)")
        self.resize(1300, 900)
        
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}
        self.current_var_name = "df"
        self.workers = [] # Keep references to active workers
        
        self.init_ui()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QVBoxLayout(central_widget)

        self.tabs = QTabWidget()
        self.main_layout.addWidget(self.tabs)

        # Tabs setup
        self.data_tab = QWidget()
        self.setup_data_tab()
        self.tabs.addTab(self.data_tab, "데이터 관리")

        self.ops_tab = QWidget()
        self.setup_ops_tab()
        self.tabs.addTab(self.ops_tab, "실무 데이터 조작")

        self.viz_tab = QWidget()
        self.setup_viz_tab()
        self.tabs.addTab(self.viz_tab, "데이터 시각화")

        self.cli_tab = QWidget()
        self.setup_cli_tab(self.cli_tab)
        self.tabs.addTab(self.cli_tab, "CLI 콘솔")

        # Bottom Bar
        bottom_layout = QHBoxLayout()
        self.status_label = QLabel("준비 완료")
        bottom_layout.addWidget(self.status_label)
        bottom_layout.addStretch()
        
        license_label = QLabel("이 프로그램에는 네이버에서 제공한 나눔글꼴이 적용되어 있습니다.")
        license_label.setStyleSheet("color: gray; font-size: 9pt;")
        bottom_layout.addWidget(license_label)
        self.main_layout.addLayout(bottom_layout)

    def setup_data_tab(self):
        layout = QVBoxLayout(self.data_tab)
        toolbar_layout = QHBoxLayout()
        self.btn_load = QPushButton("데이터 불러오기 (CSV/Excel)")
        self.btn_load.clicked.connect(self.load_data_async)
        toolbar_layout.addWidget(self.btn_load)
        
        self.btn_info = QPushButton("데이터 정보 보기")
        self.btn_info.clicked.connect(self.show_data_info)
        toolbar_layout.addWidget(self.btn_info)
        toolbar_layout.addStretch()
        layout.addLayout(toolbar_layout)

        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setStyleSheet("""
            QTableView {
                selection-background-color: rgba(135, 206, 235, 100);
                selection-color: #333333;
                gridline-color: #f0f0f0;
            }
            QTableView::item:selected {
                background-color: rgba(135, 206, 235, 100);
            }
        """)
        layout.addWidget(self.table_view)

    def start_worker(self, func, *args, on_success=None, on_error=None, on_status=None, **kwargs):
        worker = GenericWorker(func, *args, **kwargs)
        if on_success: worker.result_ready.connect(on_success)
        if on_error: worker.error_occurred.connect(on_error)
        else: worker.error_occurred.connect(lambda msg: QMessageBox.critical(self, "오류", msg))
        if on_status: worker.status_update.connect(on_status)
        else: worker.status_update.connect(lambda msg: self.status_label.setText(msg))
        
        worker.finished.connect(lambda: self.set_ui_enabled(True))
        worker.finished.connect(lambda: self.workers.remove(worker) if worker in self.workers else None)
        
        self.workers.append(worker)
        self.set_ui_enabled(False)
        worker.start()

    def set_ui_enabled(self, enabled):
        self.tabs.setEnabled(enabled)
        # Add more specific UI elements if needed

    def load_data_async(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "데이터 파일 열기", "", "CSV 파일 (*.csv);;Excel 파일 (*.xlsx *.xls)")
        if not file_path: return

        def _load_task(path):
            if path.endswith(".csv"):
                encoding = detect_encoding_parallel(path)
                df = pd.read_csv(path, encoding=encoding)
                return df, f"로드됨 (CSV, {encoding})"
            else:
                df = pd.read_excel(path)
                return df, "로드됨 (Excel)"

        def _on_loaded(res):
            df, msg = res
            self.df = df
            self.variables["df"] = self.df
            self.update_table()
            self.update_viz_combos()
            self.status_label.setText(f"{msg}: {file_path} ({df.shape[0]} 행, {df.shape[1]} 열)")

        self.start_worker(_load_task, file_path, on_success=_on_loaded)

    def update_table(self, target_df=None):
        if target_df is None: target_df = self.df
        if not target_df.empty:
            model = PandasModel(target_df)
            self.table_view.setModel(model)

    def show_data_info(self):
        if self.df.empty:
            QMessageBox.warning(self, "경고", "로드된 데이터가 없습니다.")
            return
        info_str = f"데이터 형태: {self.df.shape}\n\n컬럼 정보:\n{self.df.dtypes}\n\n결측치:\n{self.df.isnull().sum()}"
        QMessageBox.information(self, "데이터 정보", info_str)

    # --- Operations logic ---
    def setup_ops_tab(self):
        layout = QVBoxLayout(self.ops_tab)
        var_group = QGroupBox("변수 관리")
        var_layout = QHBoxLayout(var_group)
        self.combo_vars = QComboBox()
        self.combo_vars.addItems(["df"])
        self.combo_vars.currentTextChanged.connect(self.switch_variable)
        var_layout.addWidget(QLabel("현재 변수:"))
        var_layout.addWidget(self.combo_vars)
        self.btn_refresh_vars = QPushButton("변수 목록 갱신")
        self.btn_refresh_vars.clicked.connect(self.refresh_variable_list)
        var_layout.addWidget(self.btn_refresh_vars)
        layout.addWidget(var_group)

        # loc selection
        loc_group = QGroupBox("데이터 선택 (loc)")
        loc_layout = QFormLayout(loc_group)
        self.input_loc_rows = QLineEdit(":")
        self.input_loc_cols = QLineEdit(":")
        loc_layout.addRow("행 선택 (예: 0:10, [1,3,5]):", self.input_loc_rows)
        loc_layout.addRow("열 선택 (예: 'col1':'col3', ['A','B']):", self.input_loc_cols)
        btn_loc = QPushButton("loc 실행 및 새 변수로 저장")
        btn_loc.clicked.connect(self.run_loc_async)
        loc_layout.addRow(btn_loc)
        layout.addWidget(loc_group)

        # Query filter
        filter_group = QGroupBox("데이터 필터링 (Query)")
        filter_layout = QVBoxLayout(filter_group)
        self.input_query = QLineEdit()
        self.input_query.setPlaceholderText("예: age > 20 and city == 'Seoul'")
        filter_layout.addWidget(QLabel("필터 조건:"))
        filter_layout.addWidget(self.input_query)
        btn_filter = QPushButton("필터링 실행 및 새 변수로 저장")
        btn_filter.clicked.connect(self.run_filter_async)
        filter_layout.addWidget(btn_filter)
        layout.addWidget(filter_group)

        # New var layout
        new_var_layout = QHBoxLayout()
        self.input_new_var = QLineEdit("df_new")
        new_var_layout.addWidget(QLabel("새 변수 이름:"))
        new_var_layout.addWidget(self.input_new_var)
        layout.addLayout(new_var_layout)
        layout.addStretch()

    def refresh_variable_list(self):
        current = self.combo_vars.currentText()
        self.combo_vars.clear()
        df_vars = [k for k, v in self.variables.items() if isinstance(v, pd.DataFrame)]
        self.combo_vars.addItems(df_vars)
        if current in df_vars: self.combo_vars.setCurrentText(current)

    def switch_variable(self, var_name):
        if var_name in self.variables:
            self.df = self.variables[var_name]
            self.update_table()
            self.update_viz_combos()
            self.status_label.setText(f"현재 변수 변경됨: {var_name}")

    def run_loc_async(self):
        if self.df.empty: return
        new_var_name = self.input_new_var.text()
        rows_str = self.input_loc_rows.text()
        cols_str = self.input_loc_cols.text()

        def _loc_task():
            rows = eval(rows_str) if rows_str != ":" else slice(None)
            cols = eval(cols_str) if cols_str != ":" else slice(None)
            return self.df.loc[rows, cols]

        def _on_done(res):
            self.variables[new_var_name] = res
            self.refresh_variable_list()
            self.combo_vars.setCurrentText(new_var_name)
            QMessageBox.information(self, "성공", f"데이터가 {new_var_name}에 저장되었습니다.")

        self.start_worker(_loc_task, on_success=_on_done)

    def run_filter_async(self):
        if self.df.empty: return
        query_str = self.input_query.text()
        new_var_name = self.input_new_var.text()

        def _filter_task():
            return self.df.query(query_str)

        def _on_done(res):
            self.variables[new_var_name] = res
            self.refresh_variable_list()
            self.combo_vars.setCurrentText(new_var_name)
            QMessageBox.information(self, "성공", f"필터링 결과가 {new_var_name}에 저장되었습니다.")

        self.start_worker(_filter_task, on_success=_on_done)

    # --- Visualization ---
    def setup_viz_tab(self):
        layout = QHBoxLayout(self.viz_tab)
        controls_panel = QWidget()
        controls_panel.setFixedWidth(300)
        controls_layout = QVBoxLayout(controls_panel)
        
        group_box = QGroupBox("그래프 설정")
        form_layout = QFormLayout(group_box)
        self.combo_plot_type = QComboBox()
        self.combo_plot_type.addItems(["히스토그램", "산점도", "박스 플롯", "히트맵", "선 그래프"])
        form_layout.addRow("그래프 종류:", self.combo_plot_type)
        self.combo_x = QComboBox()
        self.combo_y = QComboBox()
        form_layout.addRow("X축:", self.combo_x)
        form_layout.addRow("Y축:", self.combo_y)
        
        self.btn_plot = QPushButton("그래프 생성")
        self.btn_plot.clicked.connect(self.generate_plot)
        controls_layout.addWidget(group_box)
        controls_layout.addWidget(self.btn_plot)
        controls_layout.addStretch()
        
        self.canvas_widget = QWidget()
        self.canvas_layout = QVBoxLayout(self.canvas_widget)
        self.figure = Figure(figsize=(8, 6), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        self.canvas_layout.addWidget(self.canvas)
        
        layout.addWidget(controls_panel)
        layout.addWidget(self.canvas_widget)

    def update_viz_combos(self):
        if not self.df.empty:
            cols = list(self.df.columns)
            self.combo_x.clear()
            self.combo_y.clear()
            self.combo_x.addItems(cols)
            self.combo_y.addItems(cols)

    def generate_plot(self):
        if self.df.empty:
            QMessageBox.warning(self, "경고", "로드된 데이터가 없습니다.")
            return
        
        plot_type = self.combo_plot_type.currentText()
        x_col = self.combo_x.currentText()
        y_col = self.combo_y.currentText()
        
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        try:
            if plot_type == "히스토그램":
                sns.histplot(data=self.df, x=x_col, ax=ax, kde=True)
            elif plot_type == "산점도":
                sns.scatterplot(data=self.df, x=x_col, y=y_col, ax=ax)
            elif plot_type == "박스 플롯":
                sns.boxplot(data=self.df, x=x_col, y=y_col, ax=ax)
            elif plot_type == "히트맵":
                sns.heatmap(self.df.corr(), annot=True, cmap='coolwarm', ax=ax)
            elif plot_type == "선 그래프":
                sns.lineplot(data=self.df, x=x_col, y=y_col, ax=ax)
            
            ax.set_title(f"{plot_type}: {x_col} vs {y_col}")
            self.canvas.draw()
        except Exception as e:
            QMessageBox.critical(self, "그래프 오류", f"그래프 생성 실패: {str(e)}")

    # --- CLI Console ---
    def setup_cli_tab(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        self.btn_detach = QPushButton("CLI를 새 창으로 열기")
        self.btn_detach.clicked.connect(self.open_cli_window)
        layout.addWidget(self.btn_detach)

        self.cli_output = QTextEdit()
        self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'NanumBarunGothic', Consolas, monospace; border: 1px solid #cccccc;")
        layout.addWidget(self.cli_output)
        
        input_layout = QHBoxLayout()
        self.cli_input = QLineEdit()
        self.cli_input.setPlaceholderText("Python/Pandas 명령어를 입력하세요 (예: self.df.head())")
        self.cli_input.setStyleSheet("font-family: 'NanumBarunGothic', Consolas, monospace; border: 1px solid #cccccc;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        
        self.btn_exec = QPushButton("실행")
        self.btn_exec.clicked.connect(self.execute_cli)
        input_layout.addWidget(self.cli_input)
        input_layout.addWidget(self.btn_exec)
        layout.addLayout(input_layout)

    def execute_cli(self):
        command = self.cli_input.text()
        if not command: return
        self.cli_output.append(f"<b style='color: #0056b3;'>>>> {command}</b>")
        
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                result = eval(command, {"self": self, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.variables)
            output = f.getvalue()
            if output: self.cli_output.append(output.strip())
            if result is not None: self.cli_output.append(str(result))
        except Exception:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(command, {"self": self, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.variables)
                output = f.getvalue()
                if output: self.cli_output.append(output.strip())
            except Exception as e2:
                self.cli_output.append(f"<span style='color: #d9534f;'>오류: {str(e2)}</span>")
        
        self.cli_input.clear()
        self.cli_output.ensureCursorVisible()
        self.update_table()

    def open_cli_window(self):
        self.cli_window = CLIWindow(self)
        self.cli_window.show()

class CLIWindow(QMainWindow):
    def __init__(self, parent_app):
        super().__init__()
        self.parent_app = parent_app
        self.setWindowTitle("독립형 CLI 콘솔")
        self.resize(700, 500)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        self.cli_output = QTextEdit()
        self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'NanumBarunGothic', Consolas, monospace; border: 1px solid #cccccc;")
        layout.addWidget(self.cli_output)
        
        input_layout = QHBoxLayout()
        self.cli_input = QLineEdit()
        self.cli_input.setPlaceholderText("명령어 입력...")
        self.cli_input.setStyleSheet("font-family: 'NanumBarunGothic', Consolas, monospace; border: 1px solid #cccccc;")
        self.cli_input.returnPressed.connect(self.execute_cli)
        
        self.btn_exec = QPushButton("실행")
        self.btn_exec.clicked.connect(self.execute_cli)
        input_layout.addWidget(self.cli_input)
        input_layout.addWidget(self.btn_exec)
        layout.addLayout(input_layout)

    def execute_cli(self):
        command = self.cli_input.text()
        if not command: return
        self.cli_output.append(f"<b style='color: #0056b3;'>>>> {command}</b>")
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                result = eval(command, {"self": self.parent_app, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.parent_app.variables)
            output = f.getvalue()
            if output: self.cli_output.append(output.strip())
            if result is not None: self.cli_output.append(str(result))
        except Exception:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(command, {"self": self.parent_app, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.parent_app.variables)
                output = f.getvalue()
                if output: self.cli_output.append(output.strip())
            except Exception as e2:
                self.cli_output.append(f"<span style='color: #d9534f;'>오류: {str(e2)}</span>")
        
        self.cli_input.clear()
        self.cli_output.ensureCursorVisible()
        self.parent_app.update_table()
