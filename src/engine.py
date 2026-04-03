import pandas as pd
import polars as pl
import os
import numpy as np

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
        """Applies nuclear-level advanced transformations with Code Trace sync."""
        is_polars = isinstance(df, pl.DataFrame)
        cols = params.get("columns", []) if params else []
        col = params.get("column") if params else None
        
        try:
            # 1. BASIC CLEANING
            if op_type == "Drop Nulls":
                res = df.drop_nulls() if is_polars else df.dropna()
                code = "df = df.drop_nulls()" if is_polars else "df = df.dropna()"
                return True, res, "모든 결측 행이 제거되었습니다.", code

            elif op_type == "Fill Nulls (Mean)":
                if is_polars:
                    res = df.fill_null(strategy="forward") # Simplified forward fill for Polars
                    code = "df = df.fill_null(strategy='forward')"
                else:
                    res = df.fillna(df.mean(numeric_only=True))
                    code = "df = df.fillna(df.mean(numeric_only=True))"
                return True, res, "수치형 결측치가 보정되었습니다.", code

            elif op_type == "Remove Duplicates":
                res = df.unique() if is_polars else df.drop_duplicates()
                code = "df = df.unique()" if is_polars else "df = df.drop_duplicates()"
                return True, res, "중복 데이터가 정리되었습니다.", code

            # 2. SCALING & NORMALIZATION
            elif op_type == "Standardize (Z-Score)":
                if not col: return False, df, "대상 칼럼을 선택하세요.", ""
                if is_polars:
                    res = df.with_columns([(pl.col(col) - pl.col(col).mean()) / pl.col(col).std()])
                    code = f"df = df.with_columns([(pl.col('{col}') - pl.col('{col}').mean()) / pl.col('{col}').std()])"
                else:
                    res = df.copy(); res[col] = (df[col] - df[col].mean()) / df[col].std()
                    code = f"df['{col}'] = (df['{col}'] - df['{col}'].mean()) / df['{col}'].std()"
                return True, res, f"'{col}' 칼럼 정규화 완료 (Z-Score)", code

            elif op_type == "Normalize (Min-Max)":
                if not col: return False, df, "대상 칼럼을 선택하세요.", ""
                if is_polars:
                    res = df.with_columns([(pl.col(col) - pl.col(col).min()) / (pl.col(col).max() - pl.col(col).min())])
                    code = f"df = df.with_columns([(pl.col('{col}') - pl.col('{col}').min()) / (pl.col('{col}').max() - pl.col('{col}').min())])"
                else:
                    res = df.copy(); res[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
                    code = f"df['{col}'] = (df['{col}'] - df['{col}'].min()) / (df['{col}'].max() - df['{col}'].min())"
                return True, res, f"'{col}' 칼럼 정규화 완료 (Min-Max)", code

            # 3. OUTLIER MANAGEMENT
            elif op_type == "IQR Outlier Removal":
                if not col: return False, df, "대상 칼럼을 선택하세요.", ""
                if is_polars:
                    q1 = df[col].quantile(0.25); q3 = df[col].quantile(0.75); iqr = q3 - q1
                    lower = q1 - 1.5 * iqr; upper = q3 + 1.5 * iqr
                    res = df.filter((pl.col(col) >= lower) & (pl.col(col) <= upper))
                    code = f"q1, q3 = df['{col}'].quantile(0.25), df['{col}'].quantile(0.75)\nlower, upper = q1 - 1.5*(q3-q1), q3 + 1.5*(q3-q1)\ndf = df.filter((pl.col('{col}') >= lower) & (pl.col('{col} <= upper)))"
                else:
                    q1 = df[col].quantile(0.25); q3 = df[col].quantile(0.75); iqr = q3 - q1
                    res = df[(df[col] >= q1 - 1.5 * iqr) & (df[col] <= q3 + 1.5 * iqr)]
                    code = f"Q1, Q3 = df['{col}'].quantile(0.25), df['{col}'].quantile(0.75)\nIQR = Q3 - Q1\ndf = df[(df['{col}'] >= Q1 - 1.5 * IQR) & (df['{col}'] <= Q3 + 1.5 * IQR)]"
                return True, res, "IQR 기준 이상치가 제거되었습니다.", code

            # 4. FEATURE ENGINEERING
            elif op_type == "One-Hot Encoding":
                if not col: return False, df, "대상 칼럼을 선택하세요.", ""
                if is_polars:
                    res = df.to_dummies(columns=[col])
                    code = f"df = df.to_dummies(columns=['{col}'])"
                else:
                    res = pd.get_dummies(df, columns=[col])
                    code = f"df = pd.get_dummies(df, columns=['{col}'])"
                return True, res, f"'{col}' 원-핫 인코딩 완료", code

            elif op_type == "Log Transform":
                if not col: return False, df, "대상 칼럼을 선택하세요.", ""
                if is_polars:
                    res = df.with_columns([pl.col(col).log()])
                    code = f"df = df.with_columns([pl.col('{col}').log()])"
                else:
                    res = df.copy(); res[col] = np.log1p(df[col])
                    code = f"import numpy as np\ndf['{col}'] = np.log1p(df['{col}'])"
                return True, res, f"'{col}' 로그 변환 완료", code

            elif op_type == "Sort":
                if not col: return False, df, "칼럼이 선택되지 않았습니다.", ""
                res = df.sort(col) if is_polars else df.sort_values(by=col)
                code = f"df = df.sort('{col}')" if is_polars else f"df = df.sort_values(by='{col}')"
                return True, res, f"'{col}' 기준 정렬 완료", code

            return False, df, "지원되지 않는 변환 작업입니다.", ""
        except Exception as e:
            return False, df, f"핵심 엔진 가동 실패: {str(e)}", ""

    @staticmethod
    def run_query(df, query_str):
        """Query with Code Trace sync."""
        try:
            if isinstance(df, pd.DataFrame):
                res = df.query(query_str)
                code = f"df = df.query('{query_str}')"
                return True, res, "Pandas Query 성공", code
            elif isinstance(df, pl.DataFrame):
                res = df.filter(pl.Expr(query_str))
                code = f"df = df.filter(pl.Expr('{query_str}'))"
                return True, res, "Polars Filter 성공", code
            return False, None, "지원되지 않는 엔진", ""
        except Exception as e:
            return False, None, f"쿼리 오류: {str(e)}", ""
