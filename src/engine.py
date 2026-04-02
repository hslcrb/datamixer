import pandas as pd
import polars as pl
import os

class DataEngine:
    """Manages high-performance data processing using Polars and Pandas."""
    
    @staticmethod
    def load_data(path, encoding='utf-8', engine="Pandas"):
        """Loads data with the specified engine, falling back if necessary."""
        try:
            if "Polars" in engine:
                # Polars Loading
                if path.endswith(".csv"):
                    df = pl.read_csv(path, encoding=encoding)
                    return True, df, "Polars CSV 로드 성공"
                elif path.endswith(".parquet"):
                    df = pl.read_parquet(path)
                    return True, df, "Polars Parquet 로드 성공"
                elif path.endswith((".xlsx", ".xls")):
                    df = pl.read_excel(path)
                    return True, df, "Polars Excel(fastexcel) 로드 성공"
            
            # Default Pandas Loading
            if path.endswith(".csv"):
                df = pd.read_csv(path, encoding=encoding)
                return True, df, "Pandas CSV 로드 성공"
            elif path.endswith((".xlsx", ".xls")):
                df = pd.read_excel(path)
                return True, df, "Pandas Excel 로드 성공"
            elif path.endswith(".parquet"):
                df = pd.read_parquet(path)
                return True, df, "Pandas Parquet 로드 성공"
                
            return False, None, "지원하지 않는 파일 형식"
        except Exception as e:
            return False, None, f"데이터 로드 실패: {e}"

    @staticmethod
    def run_query(df, query_str):
        """Standard query execution."""
        try:
            if isinstance(df, pd.DataFrame):
                return True, df.query(query_str), "Pandas Query 성공"
            elif isinstance(df, pl.DataFrame):
                # Polars uses SQL or Filter, simplified here as filter
                return True, df.filter(pl.Expr(query_str)), "Polars Filter 성공"
            return False, None, "지원되지 않는 객체 타입"
        except Exception as e:
            return False, None, f"쿼리 오류: {e}"
