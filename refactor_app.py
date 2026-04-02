import os

app_path = r"src\app.py"
with open(app_path, "r", encoding="utf-8") as f:
    text = f.read()

# 1. Imports
text = text.replace("from .intelligence.core import IntelligenceCore\n", "")

# 2. Window title and init
text = text.replace("Datamixer Enterprise - Intelligence Hub Ultimate Dark Edition", "Datamixer")
text = text.replace("# Enterprise Data Store", "# Data Store")
text = text.replace("# Global Settings (Strictly Dark)", "# Application Settings")
text = text.replace('"default_engine": "Polars", "auto_analysis": True', "")

# 3. Engine state & Status
text = text.replace('self.engine_mode = "Polars" \n', "")
text = text.replace("Datamixer Enterprise AI Core V6 - Ultra Dark HUB Online", "Datamixer Ready")

# 4. Tabs
text = text.replace("지능형 분석 그리드 (Workspace)", "데이터 뷰어")
text = text.replace("시각적 분석 레포트 (Visuals)", "시각화 레포트")

# 5. Insight dock removal
insight_dock_content = """        # AI Insight
        self.insight_dock = QDockWidget("AI 지능형 분석 리포트", self)
        self.insight_output = QTextEdit(); self.insight_output.setReadOnly(True)
        self.insight_output.setPlaceholderText("데이터 로드 대기 중... AI 엔진이 즉시 분석을 시작합니다.")
        self.insight_output.setMinimumHeight(180)
        self.insight_output.setFrameShape(QFrame.NoFrame)
        self.insight_dock.setWidget(self.insight_output)
        self.addDockWidget(Qt.RightDockWidgetArea, self.insight_dock)

"""
text = text.replace(insight_dock_content, "")

# 6. Titles
text = text.replace("지능형 제어 명령 핸들러 (CLI / REPL)", "명령어 콘솔")
text = text.replace("분석 코어 컨트롤러", "설정")

# 7. Props panel
text = text.replace("매핑 스키마 (Detailed Schema)", "데이터 정보")
text = text.replace("지능형 맵핑 대기 중...", "데이터 대기 중...")
text = text.replace("시각화 엔진 매뉴얼 통제", "시각화 설정")
text = text.replace("엔진 렌더링 실행", "렌더링 실행")

# 8. Core group Engine Mode removal
core_group_old = """        core_group = QGroupBox("시스템 코어 설정")
        cf = QFormLayout(core_group)
        self.combo_engine = QComboBox(); self.combo_engine.addItems(["Polars (고성능)", "Pandas (표준)"])
        self.combo_engine.currentTextChanged.connect(self.update_engine_mode)
        cf.addRow("코어 엔진:", self.combo_engine)
        self.combo_lib = QComboBox(); self.combo_lib.addItems(["Plotly (인터렉티브)", "Matplotlib (정적)"])
        self.combo_lib.currentTextChanged.connect(self.update_viz_lib)
        cf.addRow("시각화 엔진:", self.combo_lib)
        layout.addWidget(core_group)"""

core_group_new = """        core_group = QGroupBox("시각화 엔진 설정")
        cf = QFormLayout(core_group)
        self.combo_lib = QComboBox(); self.combo_lib.addItems(["Plotly (인터렉티브)", "Matplotlib (정적)"])
        self.combo_lib.currentTextChanged.connect(self.update_viz_lib)
        cf.addRow("시각화 엔진:", self.combo_lib)
        layout.addWidget(core_group)"""
text = text.replace(core_group_old, core_group_new)

# 9. Toolbar / Menu
text = text.replace("지능형 팝업 설정...", "환경 설정...")
text = text.replace("Enterprise Hub AI Core", "Toolbar")
text = text.replace("지능형 코어 설정", "환경 설정")
text = text.replace('self.status_label.setText("지능형 설정 업데이트됨.")', 'self.status_label.setText("설정 업데이트됨.")')

# 10. update_engine_mode removal
engine_mode_func = '    def update_engine_mode(self, text): self.engine_mode = "Polars" if "Polars" in text else "Pandas"\n\n'
text = text.replace(engine_mode_func, "")

# 11. Async load & auto analysis
auto_analysis_block = """            if self.app_settings["auto_analysis"]:
                self.start_worker(lambda: IntelligenceCore.analyze_full_profile(df), on_success=self.on_intelligence_finished)"""
text = text.replace(auto_analysis_block, "")
text = text.replace("데이터 지능형 로딩...", "데이터 로딩중...")

on_inte_finished = """    def on_intelligence_finished(self, report):
        txt = "<b>[지능형 데이터 분석 보고서 - AI CORE V7]</b><br><br>"
        for insight in report["insights"]: txt += f"<span style='color: #7aa2f7;'>▶</span> {insight}<br><br>"
        self.insight_output.setHtml(txt)
        if report["suggestions"]:
            best = report["suggestions"][0]
            self.combo_plot_type.setCurrentText(best["type"])
            self.combo_x.setCurrentText(best["x"])
            if best["y"]: self.combo_y.setCurrentText(best["y"])
            self.generate_plot_dispatch()
            self.status_label.setText(f"AI 코어 분석 완료: {best['desc']} 시각화.")

"""
text = text.replace(on_inte_finished, "")

# 12. smart_save / load
text = text.replace('"EngineMode": self.engine_mode, ', '')
text = text.replace('if "EngineMode" in ui: self.combo_engine.setCurrentText(ui["EngineMode"])\n            ', '')

# Minor fixes
text = text.replace('데이터 즉시 로드 완료:', '데이터 로드 완료:')

with open(app_path, "w", encoding="utf-8") as f:
    f.write(text)
