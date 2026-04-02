import pandas as pd
import numpy as np
import polars as pl
from scipy import stats

class IntelligenceCore:
    """Expert System for advanced statistical data profiling - AI Core V7 (Pd/Pl Multi-Engine)."""
    
    @staticmethod
    def analyze_full_profile(df):
        """High-performance 360 profiling with Multi-Engine Support."""
        # Normalize to Pandas for complex statistical operations that Scipy/Numpy expect
        if isinstance(df, pl.DataFrame):
            # For profiling, we use a sampled pandas version if it's huge, or just convert
            pf_df = df.to_pandas()
            engine_name = "Polars [Fast]"
        else:
            pf_df = df
            engine_name = "Pandas [Standard]"

        if pf_df.empty:
            return {"summary": {}, "insights": ["분석 가능한 데이터가 없습니다."], "suggestions": []}

        report = {
            "summary": {
                "rows": len(pf_df), 
                "cols": len(pf_df.columns), 
                "engine": engine_name
            },
            "insights": [f"<b>[Engine]</b> 현재 <span style='color: #bb9af7;'>{engine_name}</span> 엔진으로 분석 중입니다."],
            "suggestions": []
        }
        
        nums = pf_df.select_dtypes(include=[np.number])
        cats = pf_df.select_dtypes(include=['object', 'category', 'bool'])
        
        # 1. Advanced Statistical Profiling
        for col in pf_df.columns:
            series = pf_df[col]
            null_count = series.isnull().sum()
            unique_count = series.nunique()
            unique_ratio = unique_count / len(pf_df) if len(pf_df) > 0 else 0
            
            # Identify ID columns
            if unique_ratio > 0.98 and len(pf_df) > 10:
                report["insights"].append(f"<b>[구조 지능]</b> '{col}'은(는) 고유 식별자(ID) 가능성이 매우 높습니다.")
            
            # Statistical Intelligence for Numbers
            if col in nums.columns:
                try:
                    # Skewness
                    skew = series.skew()
                    if abs(skew) > 2.0:
                        direction = "Positive" if skew > 0 else "Negative"
                        report["insights"].append(f"<b>[분포 패턴]</b> '{col}'은(는) <span style='color: #f7768e;'>{direction} 편항</span>이 관찰됩니다.")
                    
                    # Outliers
                    valid_data = series.dropna()
                    if len(valid_data) > 3:
                        z_scores = np.abs(stats.zscore(valid_data))
                        outliers = np.sum(z_scores > 3.0)
                        if outliers > 0:
                            report["insights"].append(f"<b>[이상치]</b> '{col}'에서 <span style='color: #f7768e;'>{outliers}개</span>의 극단적 변동을 감지했습니다.")
                            report["suggestions"].append({"type": "박스 플롯", "x": col, "y": None, "desc": f"'{col}' 이상치 상세 분석"})
                except: pass

            # Intelligence for Categories
            elif col in cats.columns:
                if 1 < unique_count < 15:
                    top_val = series.value_counts().idxmax()
                    report["insights"].append(f"<b>[범주 Intelligence]</b> '{col}'의 지배적 속성은 <span style='color: #9ece6a;'>'{top_val}'</span>입니다.")

        # 2. Correlation Intelligence
        if len(nums.columns) >= 2:
            try:
                corr = nums.corr()
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        val = corr.iloc[i, j]
                        if abs(val) > 0.9:
                            c1, c2 = corr.columns[i], corr.columns[j]
                            report["insights"].append(f"<b>[상관 지능]</b> '{c1}' - '{c2}' 간 초고밀도 상관성(<span style='color: #7aa2f7;'>{val:.2f}</span>) 포착.")
                            report["suggestions"].append({"type": "산점도", "x": c1, "y": c2, "desc": "강력한 선형 관계 시각화"})
            except: pass
        
        if not report["suggestions"] and len(nums.columns) > 0:
            report["suggestions"].append({"type": "히스토그램", "x": nums.columns[0], "y": None, "desc": "기본 데이터 분포"})

        return report
