import os
import zipfile
import io
import base64
import json
import pandas as pd
import polars as pl

class SessionManager:
    """Advanced Project Session Manager supporting .dmx (JSON) and .dmxz (ZIP+JSON)."""
    
    @staticmethod
    def save_project(path, variables, ui_state, compress=False):
        """Saves everything. If compress=True, uses ZIP (.dmxz). Else, raw JSON (.dmx)."""
        try:
            # Ensure correct extension
            ext = ".dmxz" if compress else ".dmx"
            if not path.lower().endswith(ext):
                path += ext
                
            session_data = {
                "format_version": "3.1",
                "app_id": "datamixer-enterprise",
                "compressed": compress,
                "ui_state": ui_state,
                "dataframes": []
            }
            
            if compress:
                # 1. .dmxz: Compressed ZIP Package
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for name, data in variables.items():
                        if isinstance(data, (pd.DataFrame, pl.DataFrame)):
                            df_filename = f"variables/{name}.parquet"
                            session_data["dataframes"].append({
                                "name": name, 
                                "file": df_filename, 
                                "storage": "external"
                            })
                            
                            pq_buffer = io.BytesIO()
                            if isinstance(data, pd.DataFrame):
                                data.to_parquet(pq_buffer, engine='pyarrow', index=False)
                            else:
                                data.write_parquet(pq_buffer)
                            zf.writestr(df_filename, pq_buffer.getvalue())
                    
                    # Store session metadata as JSON within ZIP
                    zf.writestr("session_metadata.json", json.dumps(session_data, indent=4, ensure_ascii=False))
                
                with open(path, 'wb') as f:
                    f.write(buffer.getvalue())
                return True, f"고밀도 압축 프로젝트 저장 완료: {os.path.basename(path)}"
            
            else:
                # 2. .dmx: Human-Readable Single JSON
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
                    json.dump(session_data, f, indent=4, ensure_ascii=False)
                return True, f"텍스트 기반 JSON 프로젝트 저장 완료: {os.path.basename(path)}"

        except Exception as e:
            return False, f"세션 저장 중 오류 발생: {str(e)}"

    @staticmethod
    def load_project(path):
        """Unified loader for .dmx and .dmxz."""
        try:
            with open(path, 'rb') as f:
                header = f.read(4)
            
            if header.startswith(b"PK"): # ZIP signature
                return SessionManager._load_zip(path)
            else:
                return SessionManager._load_json(path)
                
        except Exception as e:
            return False, f"세션 로드 실패: {str(e)}"

    @staticmethod
    def _load_zip(path):
        with zipfile.ZipFile(path, 'r') as zf:
            json_text = zf.read("session_metadata.json").decode('utf-8')
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
