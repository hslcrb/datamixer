import subprocess
import os
import time
import socket
import threading

class JupyterServerManager:
    """Manages an external Jupyter Lab server for embedding."""
    def __init__(self, port=18888, notebook_dir="notebooks"):
        self.port = port
        self.notebook_dir = notebook_dir
        self.process = None
        self.token = "datamixer_secret_token"
        self.url = f"http://localhost:{self.port}/lab?token={self.token}"
        
        if not os.path.exists(self.notebook_dir):
            os.makedirs(self.notebook_dir)

    def is_port_in_use(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', self.port)) == 0

    def start(self):
        """Starts the Jupyter Lab server in a background process."""
        if self.is_port_in_use():
            print(f"Port {self.port} already in use. Assuming server is running.")
            return

        import sys
        cmd = [
            sys.executable, "-m", "jupyter", "lab",
            "--no-browser",
            f"--port={self.port}",
            f"--notebook-dir={self.notebook_dir}",
            f"--ServerApp.token={self.token}",
            "--ServerApp.password=''",
            "--ServerApp.allow_origin='*'",
            "--ServerApp.disable_check_xsrf=True"
        ]
        
        # Use subprocess.DEVNULL to avoid blocking with output buffers
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        )
        print(f"Jupyter Lab server starting on port {self.port}...")

    def stop(self):
        """Terminates the Jupyter server."""
        if self.process:
            self.process.terminate()
            self.process.wait()
            print("Jupyter server stopped.")
