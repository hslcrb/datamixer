import io
import contextlib
import pandas as pd
import numpy as np
import plotly.express as px
import os

class ReplHandler:
    """Enterprise-grade REPL & Slash Command Handler."""
    def __init__(self, main_app):
        self.app = main_app
        self.commands = {
            "/help": self.cmd_help,
            "/load": self.cmd_load,
            "/save": self.cmd_save,
            "/list": self.cmd_list,
            "/engine": self.cmd_engine,
            "/viz": self.cmd_viz,
            "/clear": self.cmd_clear
        }

    def process_input(self, text):
        """Processes input: Slash commands or Python code."""
        text = text.strip()
        if not text: return None
        
        if text.startswith("/"):
            parts = text.split()
            cmd = parts[0].lower()
            args = parts[1:]
            if cmd in self.commands:
                return self.commands[cmd](args)
            else:
                return f"<span style='color: #c0392b;'>알 수 없는 명령: {cmd}</span>"
        else:
            # Execute as Python/Pandas REPL
            return self.execute_python(text)

    def execute_python(self, code):
        f = io.StringIO()
        try:
            with contextlib.redirect_stdout(f):
                # Context injection for REPL
                context = {
                    "self": self.app, "pd": pd, "np": np, "px": px, 
                    "df": self.app.df, "vars": self.app.variables
                }
                result = eval(code, context, self.app.variables)
            output = f.getvalue()
            res_str = ""
            if output: res_str += output.strip() + "\n"
            if result is not None: res_str += str(result)
            return res_str if res_str else "Executed successfully."
        except Exception:
            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    exec(code, context, self.app.variables)
                output = f.getvalue()
                return output.strip() if output else "Executed successfully."
            except Exception as e:
                return f"<span style='color: #c0392b;'>Python Error: {str(e)}</span>"

    def cmd_help(self, args):
        help_text = """
        <b><span style='color: #2e7d32;'>[Datamixer Enterprise CLI Help]</span></b><br>
        /help - 이 도움말을 표시합니다.<br>
        /load [파일경로] - 파일에서 데이터를 로드합니다.<br>
        /save [파일경로] - 현재 프로젝트를 .dmx 파일로 저장합니다.<br>
        /list - 현재 로드된 모든 변수 목록을 보여줍니다.<br>
        /engine [pandas|polars] - 분석 엔진을 전환합니다.<br>
        /viz [plotly|mpl] - 시각화 엔진을 전환합니다.<br>
        /clear - 콘솔 화면을 지웁니다.
        """
        return help_text

    def cmd_load(self, args):
        if not args: return "사용법: /load [파일경로]"
        path = args[0]
        if not os.path.exists(path): return f"오류: 파일을 찾을 수 없음: {path}"
        self.app.load_data_file_async(path)
        return f"로드 명령 전달됨: {path}"

    def cmd_save(self, args):
        if not args: return "사용법: /save [파일경로.dmx]"
        path = args[0]
        self.app.smart_save_project(path)
        return f"파일 저장 명령 전달됨: {path}"

    def cmd_list(self, args):
        items = [f"<b>{k}</b>: {v.shape if isinstance(v, pd.DataFrame) else type(v)}" for k, v in self.app.variables.items()]
        return "<br>".join(items)

    def cmd_engine(self, args):
        if not args: return f"현재 엔진: {self.app.engine_mode}"
        mode = args[0].lower()
        if "polars" in mode: self.app.combo_engine.setCurrentText("Polars (고성능)")
        else: self.app.combo_engine.setCurrentText("Pandas (표준)")
        return f"엔진이 {self.app.engine_mode}로 전환되었습니다."

    def cmd_viz(self, args):
        if not args: return f"현재 시각화: {self.app.viz_lib}"
        lib = args[0].lower()
        if "plotly" in lib: self.app.combo_lib.setCurrentText("Plotly (인터렉티브)")
        else: self.app.combo_lib.setCurrentText("Matplotlib (정적)")
        return f"시각화 라이브러리가 {self.app.viz_lib}로 전환되었습니다."

    def cmd_clear(self, args):
        self.app.cli_output.clear()
        return "Cleared."
