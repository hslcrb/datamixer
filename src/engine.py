import pandas as pd
import os

class DataEngine:
    """Manages data processing using Pandas."""
    
    @staticmethod
    def load_data(path, encoding='utf-8'):
        """Loads data using Pandas."""
        try:
            if path.endswith(".csv"):
                df = pd.read_csv(path, encoding=encoding)
                return True, df, "Pandas CSV 로드 성공"
            elif path.endswith((".xlsx", ".xls")):
                df = pd.read_excel(path)
                return True, df, "Pandas Excel 로드 성공"
            return False, None, "지원하지 않는 파일 형식"
        except Exception as e:
            return False, None, f"데이터 로드 실패: {e}"

    @staticmethod
    def run_query(df, query_str):
        """Query execution."""
        try:
            return True, df.query(query_str), "Query 성공"
        except Exception as e:
            return False, f"쿼리 오류: {e}"
