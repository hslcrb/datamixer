import os
import pandas as pd
import numpy as np
import polars as pl
import plotly.express as px
from qtconsole.inprocess import QtInProcessKernelManager
from qtconsole.rich_jupyter_widget import RichJupyterWidget

class JupyterConsoleManager:
    """Professional Multi-Engine Jupyter Console for Datamixer."""
    def __init__(self, app_context):
        self.app = app_context
        # Use an in-process kernel for immediate native variable sharing
        self.kernel_manager = QtInProcessKernelManager()
        self.kernel_manager.start_kernel(show_banner=False)
        
        self.kernel_client = self.kernel_manager.client()
        self.kernel_client.start_channels()
        
        # High-end Stylistic Widget
        self.widget = RichJupyterWidget()
        self.widget.kernel_manager = self.kernel_manager
        self.widget.kernel_client = self.kernel_client
        
        # Styling: Tokyo Night Deep UI
        self.widget.setStyleSheet("""
            QWidget { background-color: #1a1b26; color: #c0caf5; border: none; }
            RichJupyterWidget { border: 1px solid #24283b; }
        """)
        
        # Immediate Global Namespace Injection
        self.inject_full_stack()

    def inject_full_stack(self):
        """Pre-load critical data science stack into the console."""
        ns = self.kernel_manager.kernel.shell.user_ns
        ns['app'] = self.app
        ns['pd'] = pd
        ns['np'] = np
        ns['pl'] = pl # Polars Engine
        ns['px'] = px
        ns['df'] = self.app.df
        ns['vars'] = self.app.variables

    def update_namespace(self):
        """Keep the console consistent with the GUI state."""
        ns = self.kernel_manager.kernel.shell.user_ns
        ns['df'] = self.app.df
        ns['vars'] = self.app.variables

    def execute_code(self, code):
        """Silent execution helper."""
        self.kernel_client.execute(code)

    def shutdown(self):
        self.kernel_client.stop_channels()
        self.kernel_manager.shutdown_kernel()
