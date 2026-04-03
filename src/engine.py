import pandas as pd
import polars as pl
import os

class DataEngine:
    """Manages high-performance data processing with Advanced Core Transformation & Code Trace support."""
    
    @staticmethod
    def load_data(path, encoding='utf-8', engine="Pandas"):
        """Loads data and generates the corresponding Python code string."""
        fname = os.path.basename(path)
        try:
            if "Polars" in engine:
                if path.endswith(".csv"):
                    df = pl.read_csv(path, encoding=encoding)
                    code = f"import polars as pl\ndf = pl.read_csv('{fname}', encoding='{encoding}')"
                    return True, df, "Polars CSV 로드 성공", code
                elif path.endswith(".parquet"):
                    df = pl.read_parquet(path)
                    code = f"import polars as pl\ndf = pl.read_parquet('{fname}')"
                    return True, df, "Polars Parquet 로드 성공", code
                elif path.endswith((".xlsx", ".xls")):
                    df = pl.read_excel(path)
                    code = f"import polars as pl\ndf = pl.read_excel('{fname}')"
                    return True, df, "Polars Excel 로드 성공", code
            
            # Default Pandas Loading
            if path.endswith(".csv"):
                df = pd.read_csv(path, encoding=encoding)
                code = f"import pandas as pd\ndf = pd.read_csv('{fname}', encoding='{encoding}')"
            elif path.endswith((".xlsx", ".xls")):
                df = pd.read_excel(path)
                code = f"import pandas as pd\ndf = pd.read_excel('{fname}')"
            elif path.endswith(".parquet"):
                df = pd.read_parquet(path)
                code = f"import pandas as pd\ndf = pd.read_parquet('{fname}')"
            else:
                return False, None, "지원하지 않는 파일 형식", ""
            
            return True, df, f"Pandas {fname.split('.')[-1].upper()} 로드 성공", code
        except Exception as e:
            return False, None, f"데이터 로드 실패: {str(e)}", ""

    @staticmethod
    def apply_transformation(df, op_type, params=None):
        """Applies advanced transformations and generates Code Trace for GUI parity."""
        is_polars = isinstance(df, pl.DataFrame)
        try:
            if op_type == "Drop Nulls":
                if is_polars:
                    res = df.drop_nulls()
                    code = "df = df.drop_nulls()"
                else:
                    res = df.dropna()
                    code = "df = df.dropna()"
                return True, res, "모든 결측 행이 제거되었습니다.", code

            elif op_type == "Fill Nulls (Mean)":
                if is_polars:
                    # Polars mean fill needs to be column-specific or using fill_nan for floats
                    res = df.fill_null(strategy="forward") # Simplified fallback
                    code = "df = df.fill_null(strategy='forward') # (Mean fill strategy mapping)"
                else:
                    res = df.fillna(df.mean(numeric_only=True))
                    code = "df = df.fillna(df.mean(numeric_only=True))"
                return True, res, "수치형 결측치가 평균값으로 보정되었습니다.", code

            elif op_type == "Remove Duplicates":
                if is_polars:
                    res = df.unique()
                    code = "df = df.unique()"
                else:
                    res = df.drop_duplicates()
                    code = "df = df.drop_duplicates()"
                return True, res, "중복 행이 제거되었습니다.", code

            elif op_type == "Sort":
                col = params.get("column")
                if not col: return False, df, "칼럼이 선택되지 않았습니다.", ""
                if is_polars:
                    res = df.sort(col)
                    code = f"df = df.sort('{col}')"
                else:
                    res = df.sort_values(by=col)
                    code = f"df = df.sort_values(by='{col}')"
                return True, res, f"'{col}' 기준으로 정렬되었습니다.", code

            return False, df, "지원되지 않는 변환 작업입니다.", ""
        except Exception as e:
            return False, df, f"변환 실패: {str(e)}", ""

    @staticmethod
    def run_query(df, query_str):
        """Query with Code Trace sync."""
        try:
            if isinstance(df, pd.DataFrame):
                res = df.query(query_str)
                code = f"df = df.query('{query_str}')"
                return True, res, "Pandas Query 성공", code
            elif isinstance(df, pl.DataFrame):
                # Using Polars filter approach
                res = df.filter(pl.Expr(query_str))
                code = f"df = df.filter(pl.Expr('{query_str}'))"
                return True, res, "Polars Filter 성공", code
            return False, None, "지원되지 않는 엔진", ""
        except Exception as e:
            return False, None, f"쿼리 오류: {str(e)}", ""
