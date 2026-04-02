import os
import pandas as pd
import numpy as np
import plotly.express as px
from qtconsole.inprocess import QtInProcessKernelManager
from qtconsole.rich_jupyter_widget import RichJupyterWidget

class JupyterConsoleManager:
    """Manages the in-process Jupyter Kernel and provides the Qt Widget."""
    def __init__(self, app_context):
        self.app = app_context
        # Create an in-process kernel
        self.kernel_manager = QtInProcessKernelManager()
        self.kernel_manager.start_kernel(show_banner=False)
        
        self.kernel_client = self.kernel_manager.client()
        self.kernel_client.start_channels()
        
        # Create the widget and connect it
        self.widget = RichJupyterWidget()
        self.widget.kernel_manager = self.kernel_manager
        self.widget.kernel_client = self.kernel_client
        
        # Optional: set some GUI properties on the widget for dark mode
        self.widget.setStyleSheet("background-color: #24283b; color: #c0caf5;")
        
        # Inject standard variables
        self._inject_context()

    def _inject_context(self):
        user_ns = self.kernel_manager.kernel.shell.user_ns
        user_ns['app'] = self.app
        user_ns['pd'] = pd
        user_ns['np'] = np
        user_ns['px'] = px
        user_ns['df'] = self.app.df
        user_ns['vars'] = self.app.variables

    def update_namespace(self):
        """Update variables if main application changes state."""
        user_ns = self.kernel_manager.kernel.shell.user_ns
        user_ns['df'] = self.app.df
        
    def execute_code(self, code):
        """Helper to run code silently from the UI."""
        self.kernel_client.execute(code)

    def shutdown(self):
        """Clean shutdown of the channels and kernel."""
        self.kernel_client.stop_channels()
        self.kernel_manager.shutdown_kernel()
