import traceback
from PySide6.QtCore import QThread, Signal

class GenericWorker(QThread):
    """A generic worker thread that runs a function and emits signals."""
    result_ready = Signal(object)
    status_update = Signal(str)
    error_occurred = Signal(str)
    finished = Signal()

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            self.status_update.emit("작업을 처리 중입니다...")
            result = self.func(*self.args, **self.kwargs)
            self.result_ready.emit(result)
        except Exception:
            err_msg = traceback.format_exc()
            self.error_occurred.emit(err_msg)
        finally:
            self.finished.emit()
