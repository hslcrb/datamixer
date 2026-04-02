import subprocess
import os
import sys
import socket
import darkdetect

class JupyterServerManager:
    """Manages an external Jupyter Lab server with system theme awareness."""
    def __init__(self, port=18888, notebook_dir="notebooks"):
        self.port = port
        self.notebook_dir = notebook_dir
        self.process = None
        self.token = "datamixer_secret_token"
        
        if not os.path.exists(self.notebook_dir):
            os.makedirs(self.notebook_dir)

    @property
    def url(self):
        return f"http://localhost:{self.port}/lab?token={self.token}"

    def is_port_in_use(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', self.port)) == 0

    def get_effective_theme(self, app_theme_setting="Dark"):
        """Detect system theme if setting is 'Auto' or fixed otherwise."""
        if app_theme_setting.lower() == "light":
            return "JupyterLab Light"
        elif app_theme_setting.lower() == "dark":
            return "JupyterLab Dark"
        
        # If 'Auto', check system
        return "JupyterLab Dark" if darkdetect.isDark() else "JupyterLab Light"

    def start(self, app_theme="Dark"):
        """Starts Jupyter Lab using the correct theme based on app/system state."""
        if self.is_port_in_use():
            print(f"Port {self.port} already in use.")
            return

        target_theme = self.get_effective_theme(app_theme)
        
        cmd = [
            sys.executable, "-m", "jupyter", "lab",
            "--no-browser",
            f"--port={self.port}",
            f"--notebook-dir={self.notebook_dir}",
            f"--ServerApp.token={self.token}",
            "--ServerApp.password=''",
            "--ServerApp.allow_origin='*'",
            "--ServerApp.disable_check_xsrf=True",
            # Injecting theme settings via command line for first launch
            f"--LabApp.default_theme={target_theme}"
        ]
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        )
        print(f"Jupyter Lab server starting with theme '{target_theme}' on port {self.port}...")

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
            print("Jupyter server stopped.")
