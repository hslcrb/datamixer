import pickle
import os
import pandas as pd

class SessionManager:
    """Manages project persistence (Save/Load)."""
    
    @staticmethod
    def save_project(path, variables):
        """Saves current DataFrames to a project file."""
        try:
            # We only save the DataFrames in variables
            data_to_save = {k: v for k, v in variables.items() if isinstance(v, pd.DataFrame)}
            with open(path, 'wb') as f:
                pickle.dump(data_to_save, f)
            return True, "저장 완료"
        except Exception as e:
            return False, f"저장 실패: {e}"

    @staticmethod
    def load_project(path):
        """Loads DataFrames from a project file."""
        try:
            with open(path, 'rb') as f:
                loaded_vars = pickle.load(f)
            return True, loaded_vars
        except Exception as e:
            return False, f"파일 로드 실패: {e}"
