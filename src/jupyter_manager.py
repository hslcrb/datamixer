import subprocess
import os
import sys
import socket
import darkdetect
import atexit
import time

class JupyterServerManager:
    """Manages an external Jupyter Lab server with robust lifecycle and port clearing."""
    def __init__(self, port=18888, notebook_dir="notebook_core"):
        self.port = port
        self.notebook_dir = notebook_dir
        self.process = None
        self.token = "datamixer_secret_token"
        
        if not os.path.exists(self.notebook_dir):
            os.makedirs(self.notebook_dir)

    @property
    def url(self):
        return f"http://localhost:{self.port}/lab?token={self.token}"

    def kill_port_process(self):
        """Forcefully kills any process currently occupying the target port (Windows specific)."""
        if os.name != 'nt': return # Linux/Mac placeholder
        try:
            # Find PID using netstat
            cmd = f'netstat -ano | findstr :{self.port}'
            res = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
            pids = set()
            for line in res:
                parts = line.split()
                if len(parts) > 4:
                    pids.add(parts[-1])
            
            for pid in pids:
                if pid == "0": continue
                print(f"TERMINATING Ghost Process on Port {self.port} (PID: {pid})...")
                subprocess.run(['taskkill', '/F', '/T', '/PID', pid], capture_output=True)
            time.sleep(1) # Wait for port to release
        except:
            pass # No process found or permission denied

    def is_port_in_use(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', self.port)) == 0

    def get_effective_theme(self, app_theme_setting="Dark"):
        if app_theme_setting.lower() == "light": return "JupyterLab Light"
        elif app_theme_setting.lower() == "dark": return "JupyterLab Dark"
        return "JupyterLab Dark" if darkdetect.isDark() else "JupyterLab Light"

    def start(self, app_theme="Dark"):
        """Starts Jupyter Lab, clearing any prior process on the same port first."""
        self.kill_port_process()
        
        target_theme = self.get_effective_theme(app_theme)
        cmd = [
            sys.executable, "-m", "jupyter", "lab",
            "--ServerApp.open_browser=False",
            "--LabApp.open_browser=False",
            f"--port={self.port}",
            f"--notebook-dir={self.notebook_dir}",
            f"--ServerApp.token={self.token}",
            "--ServerApp.password=''",
            "--ServerApp.allow_origin='*'",
            "--ServerApp.disable_check_xsrf=True",
            f"--LabApp.default_theme={target_theme}"
        ]
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        )
        atexit.register(self.stop)
        print(f"Jupyter Node V7: Online at {self.port} (Theme: {target_theme})")

    def stop(self):
        """Forcefully kills the Jupyter process and its children."""
        if self.process:
            try:
                if os.name == 'nt':
                    subprocess.run(['taskkill', '/F', '/T', '/PID', str(self.process.pid)], capture_output=True)
                else:
                    self.process.terminate()
                    self.process.wait(timeout=3)
            except: pass
            finally:
                self.process = None
                print("Jupyter Node V7: Offline")
