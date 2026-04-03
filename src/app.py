import sys
import io
import os
import contextlib
import pandas as pd
import polars as pl
import numpy as np
import plotly.express as px
import webbrowser
import darkdetect
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineProfile, QWebEnginePage
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTableView, QTabWidget, QLabel,
    QLineEdit, QComboBox, QGroupBox, QFormLayout,
    QMessageBox, QHeaderView, QTextEdit, QDockWidget, QTreeWidget,
    QTreeWidgetItem, QToolBar, QStatusBar, QFrame, QProgressBar
)
from PySide6.QtCore import Qt, QUrl, QSize, QByteArray, QTimer, Signal
from PySide6.QtGui import QPalette, QColor, QAction, QFont

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

class EditableTableView(QTableView):
    """Premium High-End Data Grid with Intelligent Read-Only/Edit Switch."""
    switch_to_edit = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setSelectionBehavior(QTableView.SelectItems)
        self.setAlternatingRowColors(True)
        self.setStyleSheet("QTableView { background-color: #1a1b26; gridline-color: #24283b; }")

    def keyPressEvent(self, event):
        m = self.model()
        if not m or not hasattr(m, 'read_only'):
            super().keyPressEvent(event)
            return
        
        # If user tries to type/edit while restricted
        if m.read_only and (event.text().isalnum() or event.key() in (Qt.Key_Backspace, Qt.Key_Delete, Qt.Key_Period)):
            from PySide6.QtWidgets import QToolTip
            pos = self.viewport().mapToGlobal(self.visualRect(self.currentIndex()).center())
            QToolTip.showText(pos, "<b>현재는 읽기 전용입니다.</b><br><br>데이터를 수동으로 수정하려면 상단<br>전환 버튼이나 여기를 눌러 쓰기로 전환하세요.", self)
            self.switch_to_edit.emit()
            return
            
        super().keyPressEvent(event)

class MiniBrowser(QWidget):
    """Integrated Premium Web Browser with Tokyo Night Sync."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        
        # Profile Sync
        profile_path = os.path.join(os.getcwd(), "browser_enterprise_profile")
        if not os.path.exists(profile_path): os.makedirs(profile_path)
        self.profile = QWebEngineProfile("EnterpriseProfile", self)
        self.profile.setPersistentStoragePath(profile_path)
        self.profile.setPersistentCookiesPolicy(QWebEngineProfile.AllowPersistentCookies)
        
        self.nav_layout = QHBoxLayout()
        self.btn_back = QPushButton("◀"); self.btn_forward = QPushButton("▶"); self.btn_reload = QPushButton("↺")
        self.url_bar = QLineEdit(); self.url_bar.setPlaceholderText("검색어나 URL 입력..."); self.url_bar.returnPressed.connect(self.navigate)
        self.btn_ext = QPushButton("🌐"); self.btn_ext.clicked.connect(lambda: webbrowser.open(self.browser.url().toString()))
        
        for btn in [self.btn_back, self.btn_forward, self.btn_reload, self.btn_ext]:
            btn.setFixedWidth(40)
            self.nav_layout.addWidget(btn)
        
        self.nav_layout.addWidget(self.url_bar, 1)
        self.layout.addLayout(self.nav_layout)
        
        self.browser = QWebEngineView()
        self.page = QWebEnginePage(self.profile, self.browser)
        self.browser.setPage(self.page); self.browser.setUrl(QUrl("https://www.google.com"))
        self.browser.urlChanged.connect(lambda q: self.url_bar.setText(q.toString()))
        self.layout.addWidget(self.browser)
        
        self.btn_back.clicked.connect(self.browser.back)
        self.btn_forward.clicked.connect(self.browser.forward)
        self.btn_reload.clicked.connect(self.browser.reload)

    def navigate(self):
        u = self.url_bar.text()
        if not ("." in u) or (" " in u): u = f"https://www.google.com/search?q={u}"
        elif not u.startswith("http"): u = "https://" + u
        self.browser.setUrl(QUrl(u))

class DataExplorerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Datamixer Enterprise - Multi-Engine AI Workstation V7")
        self.resize(1700, 1050)
        self.setAcceptDrops(True)
        
        self.df = pd.DataFrame(); self.variables = {"df": self.df}; self.workers = []
        self.app_settings = {"compress": True, "theme": "Auto", "font_size": 10, "auto_analysis": True}
        
        # Apply Base Theme and Premium Overlays
        ThemeManager.apply_theme(self.app_settings["theme"])
        
        self.jupyter_console = JupyterConsoleManager(self)
        self.jupyter_server = JupyterServerManager()
        self.jupyter_server.start(self.app_settings["theme"])
        
        self.init_ui()
        self.init_menu_and_toolbar()
        
        self.status_label.setText("Datamixer Enterprise Hub V7 - All Engines Operational (Pd/Pl/Np)")

    def closeEvent(self, event):
        if hasattr(self, 'jupyter_console'): self.jupyter_console.shutdown()
        if hasattr(self, 'jupyter_server'): self.jupyter_server.stop()
        super().closeEvent(event)

    def init_ui(self):
        self.setDockNestingEnabled(True)
        self.central_tabs = QTabWidget()
        self.setCentralWidget(self.central_tabs)
        
        # Grid Tab
        self.view_tab = QWidget(); v1 = QVBoxLayout(self.view_tab)
        # Header for the Grid
        self.grid_header_layout = QHBoxLayout()
        self.lbl_grid_mode = QLabel("MODE: READ-ONLY")
        self.lbl_grid_mode.setStyleSheet("color: #bb9af7; font-weight: bold; margin-left: 10px;")
        self.btn_toggle_edit = QPushButton("쓰기 모드로 전환"); self.btn_toggle_edit.setFixedWidth(150)
        self.btn_toggle_edit.clicked.connect(self.toggle_grid_edit_mode)
        self.grid_header_layout.addWidget(self.lbl_grid_mode); self.grid_header_layout.addStretch()
        self.grid_header_layout.addWidget(self.btn_toggle_edit)
        v1.addLayout(self.grid_header_layout)

        self.table_view = EditableTableView()
        self.table_view.switch_to_edit.connect(self.toggle_grid_edit_mode)
        v1.addWidget(self.table_view); self.central_tabs.addTab(self.view_tab, "지능형 데이터 그리드")
        
        # Viz Tab
        self.viz_tab = QWidget(); v2 = QVBoxLayout(self.viz_tab); self.browser = QWebEngineView()
        self.browser.page().setBackgroundColor(QColor("#1a1b26"))
        self.static_canvas_container = QWidget(); self.static_canvas_layout = QVBoxLayout(self.static_canvas_container)
        v2.addWidget(self.browser); v2.addWidget(self.static_canvas_container); self.static_canvas_container.hide()
        self.central_tabs.addTab(self.viz_tab, "분석 리포트")
        
        # Jupyter Workbench
        self.jupyter_tab = QWidget(); v3 = QVBoxLayout(self.jupyter_tab); self.wb_browser = QWebEngineView()
        QTimer.singleShot(1500, lambda: self.wb_browser.setUrl(QUrl(self.jupyter_server.url)))
        v3.addWidget(self.wb_browser); self.central_tabs.addTab(self.jupyter_tab, "주피터 주크벤치")

        # Browser
        self.browser_tab = MiniBrowser(); self.central_tabs.addTab(self.browser_tab, "데이터 브라우저")
        
        self.setup_docks()
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.status_label = QLabel("SYSTEM: HEALTHY"); self.status_bar.addWidget(self.status_label)
        
        # Server Indicator
        self.server_label = QLabel("● JUPYTER ONLINE"); self.server_label.setStyleSheet("color: #9ece6a; font-weight: bold; margin-right: 15px;")
        self.status_bar.addPermanentWidget(self.server_label)

    def setup_docks(self):
        # Explorer (Professional Columns)
        self.explorer_dock = QDockWidget("변수 매니저 (Multi-Engine)", self)
        self.explorer_tree = QTreeWidget(); self.explorer_tree.setHeaderLabels(["변수명", "라이브러리/엔진", "메모리 점유"])
        self.explorer_tree.itemClicked.connect(self.on_variable_clicked); self.explorer_tree.setIndentation(15)
        self.explorer_dock.setWidget(self.explorer_tree); self.addDockWidget(Qt.LeftDockWidgetArea, self.explorer_dock)
        
        # AI Insight
        self.insight_dock = QDockWidget("AI Core V7 인사이트 리포트", self)
        self.insight_output = QTextEdit(); self.insight_output.setReadOnly(True)
        self.insight_dock.setWidget(self.insight_output); self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

        # Bottom Console
        self.console_dock = QDockWidget("Jupyter Interactive Hub", self)
        self.console_dock.setWidget(self.jupyter_console.widget); self.addDockWidget(Qt.BottomDockWidgetArea, self.console_dock)
        
        # NEW: Python Code Trace Dock (Professional Feature)
        self.trace_dock = QDockWidget("실시간 파이썬 코드 트레이스 (Code Transparency)", self)
        self.trace_view = QTextEdit(); self.trace_view.setReadOnly(True)
        self.trace_view.setStyleSheet("QTextEdit { background-color: #1a1b26; color: #9ece6a; font-family: 'Courier New'; font-size: 11pt; border: none; }")
        self.trace_dock.setWidget(self.trace_view); self.addDockWidget(Qt.RightDockWidgetArea, self.trace_dock)

        # Control Panel
        self.control_dock = QDockWidget("데이터 워크벤치 설정", self)
        self.setup_props_panel(); self.control_dock.setWidget(self.props_container); self.addDockWidget(Qt.RightDockWidgetArea, self.control_dock, Qt.Vertical)

    def setup_props_panel(self):
        self.props_container = QWidget(); layout = QVBoxLayout(self.props_container)
        
        # Engine Control
        eg = QGroupBox("Core 엔진 컨트롤"); el = QFormLayout(eg)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["Polars (최고 성능)", "Pandas (안정적 특성)"])
        el.addRow("핵심 파싱 엔진:", self.combo_engine)
        self.lbl_data_info = QLabel("지능형 맵핑 대기 중..."); self.lbl_data_info.setWordWrap(True); self.lbl_data_info.setToolTip("데이터의 인코딩과 스키마 정보가 표시됩니다.")
        el.addRow(self.lbl_data_info); layout.addWidget(eg)

        # Advanced Transformation Control (NEW)
        tg = QGroupBox("고급 데이터 전처리 (Advanced Ops)"); tl = QVBoxLayout(tg)
        grid_layout = QHBoxLayout()
        self.btn_drop_null = QPushButton("결측치 제거"); self.btn_mean_fill = QPushButton("평균값 채우기")
        self.btn_unique = QPushButton("중복 제거"); self.btn_sort = QPushButton("칼럼 정렬")
        
        for btn in [self.btn_drop_null, self.btn_mean_fill, self.btn_unique, self.btn_sort]:
            btn.setStyleSheet("QPushButton { background-color: #24283b; color: #c0caf5; height: 30px; }")
            grid_layout.addWidget(btn)
        tl.addLayout(grid_layout)
        
        self.btn_drop_null.clicked.connect(lambda: self.apply_transform_async("Drop Nulls"))
        self.btn_mean_fill.clicked.connect(lambda: self.apply_transform_async("Fill Nulls (Mean)"))
        self.btn_unique.clicked.connect(lambda: self.apply_transform_async("Remove Duplicates"))
        self.btn_sort.clicked.connect(lambda: self.apply_transform_async("Sort"))
        layout.addWidget(tg)
        
        # Viz Control
        vg = QGroupBox("지능형 시각 결과 제어"); vl = QFormLayout(vg)
        self.combo_lib = QComboBox(); self.combo_lib.addItems(["Plotly (인터랙티브)", "Matplotlib (정적 PNG)"])
        vl.addRow("시각화 엔진:", self.combo_lib)
        self.combo_viz = QComboBox()
        self.combo_viz.addItems([
            "히스토그램", "산점도", "박스 플롯", "바이올린 플롯", 
            "선 그래프", "바 차트", "파이 차트", "영역 차트", 
            "히트맵 (상관관계)", "밀도 히트맵"
        ])
        vl.addRow("그래프 종류:", self.combo_viz); self.combo_x = QComboBox(); self.combo_y = QComboBox()
        vl.addRow("X축 칼럼:", self.combo_x); vl.addRow("Y축 칼럼:", self.combo_y)
        self.btn_exe = QPushButton("데이터 엔진 가동"); self.btn_exe.clicked.connect(self.generate_plot_dispatch)
        self.btn_exe.setStyleSheet("QPushButton { background-color: #7aa2f7; color: #1a1b26; font-weight: bold; height: 40px; border-radius: 5px; }")
        vl.addRow(self.btn_exe); layout.addWidget(vg)
        
        layout.addStretch()

    def update_code_trace(self, title, code):
        """Updates the Code Trace window with a premium formatted snippet."""
        current_text = self.trace_view.toHtml()
        header = f"<b style='color: #7aa2f7;'># {title}</b><br>"
        formatted_code = f"<span style='color: #9ece6a;'>{code.replace('\n', '<br>')}</span><br><br>"
        # Prepend to show the latest at the top
        self.trace_view.setHtml(header + formatted_code + current_text)

    def init_menu_and_toolbar(self):
        m = self.menuBar(); f = m.addMenu("파일 (&F)")
        f.addAction("데이터 세트 로딩...").triggered.connect(self.load_data_async)
        f.addAction("환경 설정...").triggered.connect(self.open_settings)
        f.addAction("종료").triggered.connect(self.close)
        
        tb = QToolBar("Main Controls"); self.addToolBar(tb)
        btn_set = QPushButton("시스템 설정"); btn_set.clicked.connect(self.open_settings); tb.addWidget(btn_set)

    def open_settings(self):
        d = SettingsDialog(self, self.app_settings)
        if d.exec(): 
            self.app_settings = d.get_settings()
            ThemeManager.apply_theme(self.app_settings["theme"])
            self.status_label.setText("시스템 테마 동기화 완료.")

    def load_data_file_async(self, p):
        notebook_dir = self.jupyter_server.notebook_dir
        
        def _task():
            enc = detect_encoding_parallel(p) if p.endswith(".csv") else "utf-8"
            engine = "Polars" if "Polars" in self.combo_engine.currentText() else "Pandas"
            s, df, m, code = DataEngine.load_data(p, enc, engine)
            
            if s and df is not None:
                # Synchronize with Jupyter Server immediately in background
                cache_path = os.path.join(notebook_dir, "last_loaded_data.parquet")
                try:
                    if hasattr(df, 'to_parquet'): # Pandas
                        df.to_parquet(cache_path)
                    elif hasattr(df, 'write_parquet'): # Polars
                        df.write_parquet(cache_path)
                except Exception as e:
                    print(f"Background Sync Error: {e}")
                    
                return df, m, enc, code
            return None
            
        def _ok(res):
            if not res: return
            df, m, enc, code = res; self.df = df
            self.variables[os.path.basename(p).replace(".","_")] = df
            self.update_code_trace("Data Loading", code)
            
            # Fluid UI Update Phase
            self.update_explorer()
            self.update_table()
            self.update_viz_combos()
            self.display_data_mapping(df, enc)
            self.jupyter_console.update_namespace()
            
            if self.app_settings["auto_analysis"]:
                self.start_worker(lambda: IntelligenceCore.analyze_full_profile(df), on_success=self.on_intelligence_finished)
                
        self.start_worker(_task, on_success=_ok, on_status=lambda s: self.status_label.setText(f"LOADING: {s}"))

    def apply_transform_async(self, op):
        if self.is_data_empty(): return
        params = {"column": self.combo_x.currentText()} # Use combo_x as target for Sort etc.
        
        def _task():
            return DataEngine.apply_transformation(self.df, op, params)
            
        def _ok(res):
            success, df, msg, code = res
            if success:
                self.df = df
                self.update_table(); self.update_viz_combos()
                self.update_code_trace(f"Transformation: {op}", code)
                self.status_label.setText(f"SUCCESS: {msg}")
            else:
                QMessageBox.warning(self, "Transformation Failed", msg)
                
        self.start_worker(_task, on_success=_ok)

    def on_intelligence_finished(self, r):
        html = "<b>[AI Intelligence Hub V7 - 통계 리포트]</b><br><br>"
        for i in r["insights"]: html += f"<span style='color: #7aa2f7;'>⚡</span> {i}<br><br>"
        self.insight_output.setHtml(html)
        if r["suggestions"]:
            b = r["suggestions"][0]; self.combo_viz.setCurrentText(b["type"])
            self.combo_x.setCurrentText(b["x"])
            if b["y"]: self.combo_y.setCurrentText(b["y"])
            self.generate_plot_dispatch()

    def generate_plot_dispatch(self):
        if self.is_data_empty(): return
        dp = self.df
        
        # High-performance sampling for large data
        if isinstance(self.df, pd.DataFrame):
            if len(self.df) > 50000: dp = self.df.sample(50000)
        elif isinstance(self.df, pl.DataFrame):
            if self.df.height > 50000: dp = self.df.sample(n=50000)
        
        # Dynamic Engine Selection (Plotly Hub vs Matplotlib Canvas)
        lib_mode = self.combo_lib.currentText()
        viz_type = self.combo_viz.currentText()
        x, y = self.combo_x.currentText(), self.combo_y.currentText()

        def _on_plot_ready(res):
            if not res[0]: return
            self.browser.setUrl(QUrl.fromLocalFile(res[0]))
            self.central_tabs.setCurrentIndex(1)
            self.status_label.setText(f"SUCCESS: {res[1]}")

        if "Plotly" in lib_mode:
            task = lambda: VizManager.generate_plotly_html(dp, viz_type, x, y)
            code_title = "Visualization (Plotly)"
            code = f"import plotly.express as px\nfig = px.{viz_type_map.get(viz_type, 'plot')}(df, x='{x}', y='{y}')\nfig.show()"
        else:
            task = lambda: VizManager.generate_matplotlib_fig(dp, viz_type, x, y)
            code_title = "Visualization (Matplotlib)"
            code = f"import matplotlib.pyplot as plt\nimport seaborn as sns\nsns.{viz_type_map_sns.get(viz_type, 'plot')}(data=df, x='{x}', y='{y}')\nplt.show()"
            
        self.update_code_trace(code_title, code)
        self.start_worker(task, on_success=_on_plot_ready)

    def display_data_mapping(self, df, e):
        info = f"<b>Encoding:</b> <span style='color: #9ece6a;'>{e}</span><br><b>Shape:</b> <span style='color: #7aa2f7;'>{df.shape[0]:,} x {df.shape[1]:,}</span><br><br><b>스키마 정보:</b><br>"
        for c in df.columns:
            dtype_str = str(df[c].dtype if hasattr(df[c], 'dtype') else 'Polars Type')
            info += f"- {c}: <span style='color: #e0af68; font-weight: bold; font-size: 1.1em;'>{dtype_str}</span><br>"
        self.lbl_data_info.setText(info)

    def load_data_async(self):
        p, _ = QFileDialog.getOpenFileName(self, "Import Data", "", "Data (*.csv *.xlsx *.parquet)"); 
        if p: self.load_data_file_async(p)

    def on_variable_clicked(self, i): 
        self.df = self.variables[i.text(0)]; self.update_table(); self.update_viz_combos()

    def is_data_empty(self):
        """Engine-agnostic check for empty dataframe."""
        if isinstance(self.df, pd.DataFrame):
            return self.df.empty
        elif isinstance(self.df, pl.DataFrame):
            return self.df.is_empty()
        return True

    def update_table(self): 
        if self.is_data_empty(): return
        # Logic: Models need Pandas for UI rendering speed and compatibility
        display_df = self.df if isinstance(self.df, pd.DataFrame) else self.df.to_pandas()
        model = PandasModel(display_df)
        # Sync back changes to the main engine
        model.dataChanged.connect(self.on_data_edited_sync)
        self.table_view.setModel(model)

    def on_data_edited_sync(self):
        """Synchronizes edits silently without interrupting the user's focus."""
        m = self.table_view.model()
        if not m: return
        
        edited_df = m._data
        if isinstance(self.df, pl.DataFrame):
            self.df = pl.from_pandas(edited_df)
        else:
            self.df = edited_df 
            
        self.status_label.setText("SYSTEM: 데이터 실시간 동기화 중 (편집 모드)")
        
        # Re-trigger intelligence BUT WITHOUT auto-visual-jump
        if self.app_settings["auto_analysis"]:
            # Pass a silent flag or custom callback that won't trigger generate_plot_dispatch
            self.start_worker(lambda: IntelligenceCore.analyze_full_profile(self.df), 
                               on_success=self.on_intelligence_finished_silent)

    def on_intelligence_finished_silent(self, r):
        """Update terminal output but don't jump tabs or generate new plots during editing."""
        html = "<b>[AI Intelligence Hub V7 - 통계 리포트 (실시간)]</b><br><br>"
        for i in r["insights"]: html += f"<span style='color: #7aa2f7;'>⚡</span> {i}<br><br>"
        self.insight_output.setHtml(html)
        self.status_label.setText("SYSTEM: AI 분석 결과 최신화 완료 (편집 중)")

    def update_viz_combos(self): 
        c = list(self.df.columns); self.combo_x.clear(); self.combo_y.clear(); self.combo_x.addItems(c); self.combo_y.addItems(c)

    def start_worker(self, f, *a, on_success=None, on_status=None, **k):
        w = GenericWorker(f, *a, **k)
        if on_success: w.result_ready.connect(on_success)
        if on_status: w.status_update.connect(on_status)
        w.finished.connect(lambda: self.workers.remove(w))
        self.workers.append(w); w.start()

    def update_explorer(self): 
        self.explorer_tree.clear()
        for v, d in self.variables.items():
            dtype = "NumPy" if isinstance(d, np.ndarray) else ("Polars DF" if isinstance(d, pl.DataFrame) else "Pandas DF")
            size = f"{d.shape[0]}x{d.shape[1]}" if hasattr(d, 'shape') else ("N/A")
            mem = "Calculating..."
            if hasattr(d, 'memory_usage'): 
                mem = f"{d.memory_usage(deep=True).sum()/1024**2:.2f}MB"
            elif isinstance(d, pl.DataFrame):
                mem = f"{d.estimated_size() / 1024**2:.2f}MB"
            self.explorer_tree.addTopLevelItem(QTreeWidgetItem([v, dtype, f"{size} ({mem})"]))

    def dragEnterEvent(self, e): 
        if e.mimeData().hasUrls(): 
            e.accept()
            self.status_label.setText("DROP: 데이터 파일을 놓으면 즉시 분석을 시작합니다.")
            self.status_label.setStyleSheet("color: #7aa2f7; font-weight: bold;")
        else: 
            e.ignore()
            
    def dragLeaveEvent(self, e):
        self.status_label.setText("SYSTEM: HEALTHY")
        self.status_label.setStyleSheet("")

    def dropEvent(self, e): 
        self.status_label.setText("SYSTEM: PROCESSING...")
        self.status_label.setStyleSheet("")
        for u in e.mimeData().urls(): 
            self.load_data_file_async(u.toLocalFile())
    def toggle_grid_edit_mode(self):
        m = self.table_view.model()
        if not m or not hasattr(m, 'read_only'): return
        
        m.read_only = not m.read_only
        text = "MODE: EDIT-ENABLED" if not m.read_only else "MODE: READ-ONLY"
        btn_text = "읽기 전용으로 전환" if not m.read_only else "쓰기 모드로 전환"
        color = "#9ece6a" if not m.read_only else "#bb9af7"
        
        self.lbl_grid_mode.setText(text)
        self.lbl_grid_mode.setStyleSheet(f"color: {color}; font-weight: bold; margin-left: 10px;")
        self.btn_toggle_edit.setText(btn_text)
        
        # Force refresh for flags
        self.table_view.viewport().update()
        self.status_label.setText(f"SYSTEM: {text} - 데이터 직접 수정이 가능합니다.")

# Helper Maps for Code Trace Generation
viz_type_map = {"히스토그램": "histogram", "산점도": "scatter", "박스 플롯": "box", "선 그래프": "line", "바이올린 플롯": "violin", "바 차트": "bar", "파이 차트": "pie", "영역 차트": "area"}
viz_type_map_sns = {"히스토그램": "histplot", "산점도": "scatterplot", "박스 플롯": "boxplot", "선 그래프": "lineplot", "바이올린 플롯": "violinplot", "바 차트": "barplot"}
