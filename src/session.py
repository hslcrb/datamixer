import os
import zipfile
import io
import base64
import json
import pandas as pd
import polars as pl

class SessionManager:
    """Project session manager (ZIP + JSON package)."""
    
    @staticmethod
    def save_project(path, variables, ui_state, compress=False):
        """Saves project. If compress=True, saves as ZIP. Else, saves as absolute JSON."""
        try:
            # Prepare JSON Structure
            session_data = {
                "version": "3.0",
                "compressed": compress,
                "ui_state": ui_state,
                "dataframes": []
            }
            
            if compress:
                # 1. Compressed ZIP Path (External Parquets)
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for name, data in variables.items():
                        if isinstance(data, (pd.DataFrame, pl.DataFrame)):
                            df_filename = f"data/{name}.parquet"
                            session_data["dataframes"].append({"name": name, "file": df_filename, "storage": "external"})
                            
                            pq_buffer = io.BytesIO()
                            if isinstance(data, pd.DataFrame):
                                data.to_parquet(pq_buffer, engine='pyarrow', index=False)
                            else:
                                data.write_parquet(pq_buffer)
                            zf.writestr(df_filename, pq_buffer.getvalue())
                    
                    zf.writestr("session.json", json.dumps(session_data, indent=2, ensure_ascii=False))
                
                with open(path, 'wb') as f:
                    f.write(buffer.getvalue())
                return True, "압축 프로젝트 저장 완료 (JSON 기반 .dmx)"
            
            else:
                # 2. Uncompressed Single JSON Path (Inline Base64)
                for name, data in variables.items():
                    if isinstance(data, (pd.DataFrame, pl.DataFrame)):
                        pq_buffer = io.BytesIO()
                        if isinstance(data, pd.DataFrame):
                            data.to_parquet(pq_buffer, engine='pyarrow', index=False)
                        else:
                            data.write_parquet(pq_buffer)
                            
                        b64_data = base64.b64encode(pq_buffer.getvalue()).decode('utf-8')
                        session_data["dataframes"].append({
                            "name": name, 
                            "storage": "inline",
                            "data": b64_data
                        })
                
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(session_data, f, indent=2, ensure_ascii=False)
                return True, "단일 JSON 프로젝트 저장 완료 (.dmx)"

        except Exception as e:
            return False, f"저장 실패: {str(e)}"

    @staticmethod
    def load_project(path):
        """Intelligently loads project by detecting binary ZIP vs plain JSON."""
        try:
            with open(path, 'rb') as f:
                header = f.read(4)
            
            if header.startswith(b"PK"): # ZIP Magic
                return SessionManager._load_zip(path)
            else:
                return SessionManager._load_json(path)
                
        except Exception as e:
            return False, f"프로젝트 로드 실패: {str(e)}"

    @staticmethod
    def _load_zip(path):
        with zipfile.ZipFile(path, 'r') as zf:
            json_text = zf.read("session.json").decode('utf-8')
            data = json.loads(json_text)
            
            ui_state = data.get("ui_state", {})
            variables = {}
            for df_info in data.get("dataframes", []):
                name = df_info["name"]
                file_in_zip = df_info["file"]
                pq_data = zf.read(file_in_zip)
                variables[name] = pd.read_parquet(io.BytesIO(pq_data))
            return True, {"variables": variables, "ui_state": ui_state}

    @staticmethod
    def _load_json(path):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        ui_state = data.get("ui_state", {})
        variables = {}
        for df_info in data.get("dataframes", []):
            name = df_info["name"]
            if df_info["storage"] == "inline":
                b64_data = df_info["data"]
                pq_data = base64.b64decode(b64_data)
                variables[name] = pd.read_parquet(io.BytesIO(pq_data))
                    
        return True, {"variables": variables, "ui_state": ui_state}
