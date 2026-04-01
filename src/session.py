import os
import zipfile
import io
import base64
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom

class SessionManager:
    """Enterprise project persistence (Smart ZIP + XML package)."""
    
    @staticmethod
    def save_project(path, variables, ui_state, compress=True):
        """Saves project. If compress=True, saves as ZIP. Else, saves as single XML."""
        try:
            # Prepare XML
            root = ET.Element("DatamixerSession", version="2.0", compressed="true" if compress else "false")
            
            # UI State
            ui_elem = ET.SubElement(root, "UIState")
            for key, value in ui_state.items():
                child = ET.SubElement(ui_elem, key)
                child.text = str(value)
            
            data_elem = ET.SubElement(root, "DataFrames")

            if compress:
                # 1. Compressed ZIP Path
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for name, data in variables.items():
                        if isinstance(data, pd.DataFrame):
                            df_filename = f"data/{name}.parquet"
                            ET.SubElement(data_elem, "DataFrame", name=name, file=df_filename, storage="external")
                            
                            pq_buffer = io.BytesIO()
                            data.to_parquet(pq_buffer, engine='pyarrow', index=False)
                            zf.writestr(df_filename, pq_buffer.getvalue())
                    
                    xml_str = ET.tostring(root, encoding='utf-8')
                    pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="  ")
                    zf.writestr("session.xml", pretty_xml)
                
                with open(path, 'wb') as f:
                    f.write(buffer.getvalue())
                return True, "압축된 Enterprise 프로젝트 저장 완료 (.dmx)"
            
            else:
                # 2. Uncompressed Single XML Path
                for name, data in variables.items():
                    if isinstance(data, pd.DataFrame):
                        pq_buffer = io.BytesIO()
                        data.to_parquet(pq_buffer, engine='pyarrow', index=False)
                        b64_data = base64.b64encode(pq_buffer.getvalue()).decode('utf-8')
                        
                        df_node = ET.SubElement(data_elem, "DataFrame", name=name, storage="inline")
                        df_node.text = b64_data
                
                xml_str = ET.tostring(root, encoding='utf-8')
                pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="  ")
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(pretty_xml)
                return True, "단일 XML 프로젝트 저장 완료 (.dmx)"

        except Exception as e:
            return False, f"저장 실패: {e}"

    @staticmethod
    def load_project(path):
        """Intelligently loads project by detecting ZIP vs XML format."""
        try:
            # Detection: Read first few bytes
            with open(path, 'rb') as f:
                header = f.read(4)
            
            if header.startswith(b"PK"): # ZIP Magic Number
                return SessionManager._load_zip(path)
            else:
                return SessionManager._load_xml(path)
                
        except Exception as e:
            return False, f"프로젝트 로드 실패: {e}"

    @staticmethod
    def _load_zip(path):
        with zipfile.ZipFile(path, 'r') as zf:
            xml_data = zf.read("session.xml")
            root = ET.fromstring(xml_data)
            
            ui_state = SessionManager._parse_ui_elem(root.find("UIState"))
            variables = {}
            data_elem = root.find("DataFrames")
            if data_elem is not None:
                for df_node in data_elem.findall("DataFrame"):
                    name = df_node.get("name")
                    file_in_zip = df_node.get("file")
                    pq_data = zf.read(file_in_zip)
                    variables[name] = pd.read_parquet(io.BytesIO(pq_data))
            return True, {"variables": variables, "ui_state": ui_state}

    @staticmethod
    def _load_xml(path):
        tree = ET.parse(path)
        root = tree.getroot()
        
        ui_state = SessionManager._parse_ui_elem(root.find("UIState"))
        variables = {}
        data_elem = root.find("DataFrames")
        if data_elem is not None:
            for df_node in data_elem.findall("DataFrame"):
                name = df_node.get("name")
                storage = df_node.get("storage")
                
                if storage == "inline":
                    b64_data = df_node.text
                    pq_data = base64.b64decode(b64_data)
                    variables[name] = pd.read_parquet(io.BytesIO(pq_data))
                    
        return True, {"variables": variables, "ui_state": ui_state}

    @staticmethod
    def _parse_ui_elem(ui_elem):
        ui_state = {}
        if ui_elem is not None:
            for child in ui_elem:
                ui_state[child.tag] = child.text
        return ui_state
