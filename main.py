import sys
import os
import io
import contextlib
import concurrent.futures
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTableView, QTabWidget, QLabel, QSplitter,
    QPlainTextEdit, QLineEdit, QComboBox, QGroupBox, QFormLayout,
    QMessageBox, QHeaderView, QScrollArea, QTextEdit, QFrame
)
from PySide6.QtCore import Qt, QAbstractTableModel, Signal, Slot
from PySide6.QtGui import QFont, QAction, QFontDatabase

# --- Encoding Utilities ---
ENCODING_CANDIDATES = [
    'utf-8', 'cp949', 'euc-kr', 'utf-8-sig', 'utf-16', 
    'cp1252', 'iso-8859-1', 'latin1', 'utf-16-le', 'utf-16-be'
]

def try_decode(chunk, encoding):
    """Helper to try decoding a chunk with a specific encoding."""
    try:
        chunk.decode(encoding)
        return encoding
    except:
        return None

def detect_encoding_parallel(file_path):
    """Detect encoding of a file by trying candidates in parallel."""
    try:
        with open(file_path, 'rb') as f:
            chunk = f.read(1024 * 100) # Read first 100KB
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Map try_decode across candidates
            results = list(executor.map(lambda enc: try_decode(chunk, enc), ENCODING_CANDIDATES))
            
        # Return the first successful encoding
        for res in results:
            if res:
                return res
    except Exception as e:
        print(f"Encoding detection failed: {e}")
    return 'utf-8' # Default fallback

# --- Font Setup ---
FONT_PATH = "NanumBarunGothic.ttf"

def setup_fonts():
    """Register the NanumBarunGothic font for both PySide6 and Matplotlib."""
    if os.path.exists(FONT_PATH):
        # 1. Register for PySide6
        font_id = QFontDatabase.addApplicationFont(FONT_PATH)
        if font_id != -1:
            font_family = QFontDatabase.applicationFontFamilies(font_id)[0]
            QApplication.setFont(QFont(font_family, 10))
            print(f"PySide6 Font Registered: {font_family}")
        
        # 2. Register for Matplotlib
        fe = fm.FontEntry(fname=FONT_PATH, name='NanumBarunGothic')
        fm.fontManager.ttflist.insert(0, fe)
        plt.rcParams['font.family'] = fe.name
        plt.rcParams['axes.unicode_minus'] = False  # Fix minus sign display
        print(f"Matplotlib Font Registered: {fe.name}")
    else:
        print(f"Warning: {FONT_PATH} not found. Using default fonts.")

class PandasModel(QAbstractTableModel):
    """A model to interface between Pandas DataFrame and QTableView."""
    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BigData Explorer Pro V2 - Enterprise Edition")
        self.resize(1300, 900)
        
        self.df = pd.DataFrame()
        self.variables = {"df": self.df}  # Dictionary to store user-defined variables
        self.current_var_name = "df"
        
        self.init_ui()

    def init_ui(self):
        # Central Widget and Main Layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QVBoxLayout(central_widget)

        # Tab Widget for different modules
        self.tabs = QTabWidget()
        self.main_layout.addWidget(self.tabs)

        # 1. Data Management Tab
        self.data_tab = QWidget()
        self.setup_data_tab()
        self.tabs.addTab(self.data_tab, "데이터 관리")

        # 2. Advanced Operations Tab
        self.ops_tab = QWidget()
        self.setup_ops_tab()
        self.tabs.addTab(self.ops_tab, "실무 데이터 조작")

        # 3. Visualization Tab
        self.viz_tab = QWidget()
        self.setup_viz_tab()
        self.tabs.addTab(self.viz_tab, "데이터 시각화")

        # 4. CLI Console Tab
        self.cli_tab = QWidget()
        self.setup_cli_tab(self.cli_tab)
        self.tabs.addTab(self.cli_tab, "CLI 콘솔")

        # --- License Notice & Status Bar ---
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
        
        # Toolbar for Data Loading
        toolbar_layout = QHBoxLayout()
        self.btn_load = QPushButton("데이터 불러오기 (CSV/Excel)")
        self.btn_load.clicked.connect(self.load_data)
        toolbar_layout.addWidget(self.btn_load)
        
        self.btn_info = QPushButton("데이터 정보 보기")
        self.btn_info.clicked.connect(self.show_data_info)
        toolbar_layout.addWidget(self.btn_info)
        
        toolbar_layout.addStretch()
        layout.addLayout(toolbar_layout)

        # Data Table View
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        layout.addWidget(self.table_view)

    def load_data(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "데이터 파일 열기", "", "CSV 파일 (*.csv);;Excel 파일 (*.xlsx *.xls)")
        if file_path:
            try:
                if file_path.endswith(".csv"):
                    encoding = detect_encoding_parallel(file_path)
                    print(f"Detected Encoding: {encoding}")
                    self.df = pd.read_csv(file_path, encoding=encoding)
                    status_msg = f"로드됨 (CSV, {encoding}): {file_path} ({self.df.shape[0]} 행, {self.df.shape[1]} 열)"
                else:
                    self.df = pd.read_excel(file_path)
                    status_msg = f"로드됨 (Excel): {file_path} ({self.df.shape[0]} 행, {self.df.shape[1]} 열)"
                
                self.variables["df"] = self.df
                self.update_table()
                self.update_viz_combos()
                self.status_label.setText(status_msg)
            except Exception as e:
                QMessageBox.critical(self, "오류", f"파일 로드 실패: {str(e)}")

    def update_table(self, target_df=None):
        if target_df is None:
            target_df = self.df
            
        if not target_df.empty:
            model = PandasModel(target_df)
            self.table_view.setModel(model)

    def show_data_info(self):
        if self.df.empty:
            QMessageBox.warning(self, "경고", "로드된 데이터가 없습니다.")
            return
        
        info_str = f"데이터 형태: {self.df.shape}\n\n컬럼 정보:\n{self.df.dtypes}\n\n결측치:\n{self.df.isnull().sum()}"
        QMessageBox.information(self, "데이터 정보", info_str)

    def setup_ops_tab(self):
        layout = QVBoxLayout(self.ops_tab)
        
        # 1. Variable Management
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

        # 2. Data Selection (loc)
        loc_group = QGroupBox("데이터 선택 (loc)")
        loc_layout = QFormLayout(loc_group)
        self.input_loc_rows = QLineEdit(":")
        self.input_loc_cols = QLineEdit(":")
        loc_layout.addRow("행 선택 (예: 0:10, [1,3,5]):", self.input_loc_rows)
        loc_layout.addRow("열 선택 (예: 'col1':'col3', ['A','B']):", self.input_loc_cols)
        
        btn_loc = QPushButton("loc 실행 및 새 변수로 저장")
        btn_loc.clicked.connect(self.run_loc)
        loc_layout.addRow(btn_loc)
        layout.addWidget(loc_group)

        # 3. Filtering (Query)
        filter_group = QGroupBox("데이터 필터링 (Query)")
        filter_layout = QVBoxLayout(filter_group)
        self.input_query = QLineEdit()
        self.input_query.setPlaceholderText("예: age > 20 and city == 'Seoul'")
        filter_layout.addWidget(QLabel("필터 조건:"))
        filter_layout.addWidget(self.input_query)
        
        btn_filter = QPushButton("필터링 실행 및 새 변수로 저장")
        btn_filter.clicked.connect(self.run_filter)
        filter_layout.addWidget(btn_filter)
        layout.addWidget(filter_group)

        # 4. New Variable Name
        new_var_layout = QHBoxLayout()
        self.input_new_var = QLineEdit("df_new")
        new_var_layout.addWidget(QLabel("새 변수 이름:"))
        new_var_layout.addWidget(self.input_new_var)
        layout.addLayout(new_var_layout)
        
        layout.addStretch()

    def refresh_variable_list(self):
        current = self.combo_vars.currentText()
        self.combo_vars.clear()
        # Only include DataFrames from the variables dict
        df_vars = [k for k, v in self.variables.items() if isinstance(v, pd.DataFrame)]
        self.combo_vars.addItems(df_vars)
        if current in df_vars:
            self.combo_vars.setCurrentText(current)

    def switch_variable(self, var_name):
        if var_name in self.variables:
            self.df = self.variables[var_name]
            self.update_table()
            self.update_viz_combos()
            self.status_label.setText(f"현재 변수 변경됨: {var_name}")

    def run_loc(self):
        if self.df.empty: return
        try:
            rows = eval(self.input_loc_rows.text()) if self.input_loc_rows.text() != ":" else slice(None)
            cols = eval(self.input_loc_cols.text()) if self.input_loc_cols.text() != ":" else slice(None)
            
            new_df = self.df.loc[rows, cols]
            new_var_name = self.input_new_var.text()
            self.variables[new_var_name] = new_df
            self.refresh_variable_list()
            self.combo_vars.setCurrentText(new_var_name)
            QMessageBox.information(self, "성공", f"데이터가 {new_var_name}에 저장되었습니다.")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"loc 실행 실패: {str(e)}")

    def run_filter(self):
        if self.df.empty: return
        try:
            query_str = self.input_query.text()
            new_df = self.df.query(query_str)
            new_var_name = self.input_new_var.text()
            self.variables[new_var_name] = new_df
            self.refresh_variable_list()
            self.combo_vars.setCurrentText(new_var_name)
            QMessageBox.information(self, "성공", f"필터링 결과가 {new_var_name}에 저장되었습니다.")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"필터링 실행 실패: {str(e)}")

    def setup_viz_tab(self):
        layout = QHBoxLayout(self.viz_tab)
        
        # Left Panel: Controls
        controls_panel = QWidget()
        controls_layout = QVBoxLayout(controls_panel)
        controls_panel.setFixedWidth(300)
        
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
        
        # Right Panel: Canvas
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

    def setup_cli_tab(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        
        # Detach Button
        self.btn_detach = QPushButton("CLI를 새 창으로 열기")
        self.btn_detach.clicked.connect(self.open_cli_window)
        layout.addWidget(self.btn_detach)

        # Output Area (Light Theme)
        self.cli_output = QTextEdit()
        self.cli_output.setReadOnly(True)
        self.cli_output.setStyleSheet("font-family: 'NanumBarunGothic', Consolas, monospace; border: 1px solid #cccccc;")
        layout.addWidget(self.cli_output)
        
        # Input Area
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
        if not command:
            return
        
        self.cli_output.append(f"<b style='color: #0056b3;'>>>> {command}</b>")
        
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                # Use self.variables as the local namespace
                result = eval(command, {"self": self, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.variables)
            
            output = f.getvalue()
            if output:
                self.cli_output.append(output.strip())
            if result is not None:
                self.cli_output.append(str(result))
                
        except Exception as e:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(command, {"self": self, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.variables)
                output = f.getvalue()
                if output:
                    self.cli_output.append(output.strip())
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
        if not command:
            return
        
        self.cli_output.append(f"<b style='color: #0056b3;'>>>> {command}</b>")
        
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                result = eval(command, {"self": self.parent_app, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.parent_app.variables)
            
            output = f.getvalue()
            if output:
                self.cli_output.append(output.strip())
            if result is not None:
                self.cli_output.append(str(result))
                
        except Exception as e:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(command, {"self": self.parent_app, "pd": pd, "np": np, "plt": plt, "sns": sns}, self.parent_app.variables)
                output = f.getvalue()
                if output:
                    self.cli_output.append(output.strip())
            except Exception as e2:
                self.cli_output.append(f"<span style='color: #d9534f;'>오류: {str(e2)}</span>")
        
        self.cli_input.clear()
        self.cli_output.ensureCursorVisible()
        self.parent_app.update_table()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    setup_fonts()
    window = DataExplorerApp()
    window.show()
    sys.exit(app.exec())
