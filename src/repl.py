import os
import pandas as pd
import numpy as np
import polars as pl
import plotly.express as px
from qtconsole.inprocess import QtInProcessKernelManager
from qtconsole.rich_jupyter_widget import RichJupyterWidget
from src.theme import ThemeManager

class JupyterConsoleManager:
    """Professional Multi-Engine Jupyter Console for Datamixer (Theme-Aware)."""
    def __init__(self, app_context):
        self.app = app_context
        self.kernel_manager = QtInProcessKernelManager()
        self.kernel_manager.start_kernel(show_banner=False)
        self.kernel_client = self.kernel_manager.client()
        self.kernel_client.start_channels()
        
        self.widget = RichJupyterWidget()
        self.widget.kernel_manager = self.kernel_manager
        self.widget.kernel_client = self.kernel_client
        
        self.apply_dynamic_theme()
        self.inject_full_stack()

    def apply_dynamic_theme(self):
        """Applies current design system variables to the interactive console."""
        mode = self.app.app_settings.get("theme", "dark").lower()
        colors = ThemeManager.get_colors(mode)
        
        # Base Widget Styling
        self.widget.setStyleSheet(f"""
            QWidget {{ background-color: {colors["bg"]}; color: {colors["fg"]}; border: none; font-family: 'Consolas', 'Courier New', monospace; font-size: 10pt; }}
            QPlainTextEdit {{ background-color: {colors["bg"]} !important; color: {colors["fg"]} !important; }}
            RichJupyterWidget {{ border: none; background-color: {colors["bg"]}; }}
        """)
        
        # Internal Syntax & Prompt Styling
        self.widget.set_default_style('light' if mode == 'light' else 'linux')
        self.widget.style_sheet = f"""
            .ipython {{ color: {colors["secondary"]}; }}
            .input_prompt {{ color: {colors["accent"]}; font-weight: bold; }}
            .output_prompt {{ color: {colors["secondary"]}; font-weight: bold; }}
            .in_prompt {{ color: {colors["accent"]}; }}
            .out_prompt {{ color: {colors["secondary"]}; }}
        """

    def inject_full_stack(self):
        ns = self.kernel_manager.kernel.shell.user_ns
        ns['app'] = self.app
        ns['pd'] = pd; ns['np'] = np; ns['pl'] = pl; ns['px'] = px
        ns['df'] = self.app.df; ns['vars'] = self.app.variables

    def update_namespace(self):
        ns = self.kernel_manager.kernel.shell.user_ns
        ns['df'] = self.app.df; ns['vars'] = self.app.variables

    def execute_code(self, code):
        self.kernel_client.execute(code)

    def shutdown(self):
        self.kernel_client.stop_channels()
        self.kernel_manager.shutdown_kernel()
