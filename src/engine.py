import pandas as pd
import polars as pl
import os

class DataEngine:
    """Manages high-performance data processing (Pandas + Polars)."""
    
    @staticmethod
    def load_data(path, encoding='utf-8'):
        """Loads data using Polars for high performance."""
        try:
            if path.endswith(".csv"):
                # Polars reads much faster
                try:
                    # In Polars, common encodings must be handled
                    df = pl.read_csv(path, encoding=encoding)
                    # Convert to pandas for PySide6 models compat (for now)
                    return True, df.to_pandas(), "Polars 엔진 로드 성공"
                except Exception as e:
                    # Fallback to pandas
                    df = pd.read_csv(path, encoding=encoding)
                    return True, df, f"Pandas 엔진 폴백 로드 성공 (Polars 실패: {e})"
            elif path.endswith((".xlsx", ".xls")):
                df = pd.read_excel(path)
                return True, df, "Pandas Excel 로드 성공"
            return False, None, "지원하지 않는 파일 형식"
        except Exception as e:
            return False, None, f"엔진 로드 실패: {e}"

    @staticmethod
    def run_query(df, query_str):
        """High-performance query execution."""
        try:
            # If large, use Polars
            if len(df) > 100000:
                pldf = pl.from_pandas(df)
                # Polars query has different syntax, but for simple queries we can try
                # For now, let's keep pandas for query consistency
                return True, df.query(query_str), "Pandas Query 성공"
            else:
                return True, df.query(query_str), "Pandas Query 성공"
        except Exception as e:
            return False, f"쿼리 엔진 오류: {e}"
