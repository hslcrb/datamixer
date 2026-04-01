import os
import zipfile
import io
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidoc

class SessionManager:
    """Manages Enterprise project persistence (XML + ZIP package)."""
    
    @staticmethod
    def save_project(path, variables, ui_state):
        """Saves current state and DataFrames into a .dmx ZIP package."""
        try:
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                # 1. Create XML structure
                root = ET.Element("DatamixerSession", version="2.0")
                
                # UI State
                ui_elem = ET.SubElement(root, "UIState")
                for key, value in ui_state.items():
                    child = ET.SubElement(ui_elem, key)
                    child.text = str(value)
                
                # DataFrames
                data_elem = ET.SubElement(root, "DataFrames")
                for name, data in variables.items():
                    if isinstance(data, pd.DataFrame):
                        df_filename = f"data/{name}.parquet"
                        df_elem = ET.SubElement(data_elem, "DataFrame", name=name, file=df_filename)
                        
                        # Save Parquet to ZIP
                        pq_buffer = io.BytesIO()
                        data.to_parquet(pq_buffer, engine='pyarrow', index=False)
                        zf.writestr(df_filename, pq_buffer.getvalue())
                
                # 2. Add XML to ZIP
                xml_str = ET.tostring(root, encoding='utf-8')
                pretty_xml = minidoc.parseString(xml_str).toprettyxml(indent="  ")
                zf.writestr("session.xml", pretty_xml)
                
            with open(path, 'wb') as f:
                f.write(buffer.getvalue())
            return True, "Enterprise 프로젝트 저장 완료"
        except Exception as e:
            return False, f"저장 실패: {e}"

    @staticmethod
    def load_project(path):
        """Loads Enterprise project package (.dmx)."""
        try:
            with zipfile.ZipFile(path, 'r') as zf:
                # 1. Parse XML
                xml_data = zf.read("session.xml")
                root = ET.fromstring(xml_data)
                
                # Load UI State
                ui_state = {}
                ui_elem = root.find("UIState")
                if ui_elem is not None:
                    for child in ui_elem:
                        ui_state[child.tag] = child.text
                
                # Load DataFrames
                variables = {}
                data_elem = root.find("DataFrames")
                if data_elem is not None:
                    for df_node in data_elem.findall("DataFrame"):
                        name = df_node.get("name")
                        file_in_zip = df_node.get("file")
                        
                        pq_data = zf.read(file_in_zip)
                        df = pd.read_parquet(io.BytesIO(pq_data))
                        variables[name] = df
                        
            return True, {"variables": variables, "ui_state": ui_state}
        except Exception as e:
            return False, f"프로젝트 로드 실패: {e}"
