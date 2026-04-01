import pandas as pd
import numpy as np
from scipy import stats

class IntelligenceCore:
    """Expert System for advanced statistical data profiling (No API dependence)."""
    
    @staticmethod
    def analyze_full_profile(df):
        """High-performance 360 profiling using precise statistical algorithms."""
        if df.empty:
            return {"summary": {}, "insights": ["No data available."], "suggestions": []}

        report = {
            "summary": {
                "rows": len(df), "cols": len(df.columns), 
                "memory": f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
            },
            "insights": [],
            "suggestions": []
        }
        
        nums = df.select_dtypes(include=[np.number])
        cats = df.select_dtypes(include=['object', 'category', 'bool'])
        
        # 1. Advanced Column-wise Profiling
        for col in df.columns:
            series = df[col]
            null_count = series.isnull().sum()
            unique_count = series.nunique()
            unique_ratio = unique_count / len(df)
            
            # Identify ID columns (High unique ratio)
            if unique_ratio > 0.95 and len(df) > 10:
                report["insights"].append(f"<b>[구조 분석]</b> '{col}'은(는) 고유값이 매우 많아 <span style='color: #7aa2f7;'>ID/고유 식별자</span>일 확률이 높습니다.")
            
            # Intelligence for Numbers
            if col in nums.columns:
                try:
                    # Skewness Analysis
                    skew = series.skew()
                    if abs(skew) > 1.5:
                        direction = "오른쪽" if skew > 0 else "왼쪽"
                        report["insights"].append(f"<b>[분포 패턴]</b> '{col}'의 분포가 <span style='color: #f7768e;'>{direction}으로 심하게 치우쳐</span> 있습니다 (왜도: {skew:.2f}).")
                    
                    # Outlier with Z-Score
                    z_scores = np.abs(stats.zscore(series.dropna()))
                    outliers_z = np.sum(z_scores > 3)
                    if outliers_z > 0:
                        report["insights"].append(f"<b>[지능형 감지]</b> '{col}'에서 통계적으로 유의미한 이상치({outliers_z}개)를 발견했습니다.")
                        report["suggestions"].append({"type": "박스 플롯", "x": col, "y": None, "desc": f"'{col}' 정밀 이상치 분석"})
                except: pass

            # Intelligence for Categories
            elif col in cats.columns:
                # Cardinality check
                if unique_count < 10:
                    top_val = series.value_counts().index[0]
                    top_pct = (series.value_counts().iloc[0] / len(df)) * 100
                    if top_pct > 60:
                        report["insights"].append(f"<b>[편중 기조]</b> '{col}'은(는) <span style='color: #9ece6a;'>'{top_val}'</span> 데이터가 과반수 이상({top_pct:.1f}%)을 차지합니다.")

        # 2. Relationship Analysis (Correlation)
        if len(nums.columns) >= 2:
            try:
                corr = nums.corr()
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        c_val = corr.iloc[i, j]
                        if abs(c_val) > 0.85:
                            c1, c2 = corr.columns[i], corr.columns[j]
                            report["insights"].append(f"<b>[강력한 상관성]</b> '{c1}' - '{c2}' 간의 초고밀도 상관성(<span style='color: #bb9af7;'>{c_val:.2f}</span>) 감지.")
                            report["suggestions"].append({"type": "산점도", "x": c1, "y": c2, "desc": "상관관계 정밀 시각화"})
            except: pass

        # 3. Final Multi-variate Suggestion
        if not report["suggestions"]:
            if len(nums.columns) >= 1:
                report["suggestions"].append({"type": "히스토그램", "x": nums.columns[0], "y": None, "desc": "가장 핵심적인 데이터 분포 확인"})
        
        return report
