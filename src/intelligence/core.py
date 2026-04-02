import pandas as pd
import numpy as np
from scipy import stats

class IntelligenceCore:
    """Expert System for advanced statistical data profiling - AI Core V7."""
    
    @staticmethod
    def analyze_full_profile(df):
        """High-performance 360 profiling using precise statistical algorithms."""
        if df.empty:
            return {"summary": {}, "insights": ["분류 데이터가 없습니다."], "suggestions": []}

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
        
        # 1. Advanced Statistical Profiling
        for col in df.columns:
            series = df[col]
            null_count = series.isnull().sum()
            unique_count = series.nunique()
            unique_ratio = unique_count / len(df)
            
            # Identify ID columns
            if unique_ratio > 0.95 and len(df) > 10:
                report["insights"].append(f"<b>[구조 지능]</b> '{col}'은(는) <span style='color: #7aa2f7;'>고유 식별자(ID)</span> 계열로 판명되었습니다.")
            
            # Statistical Intelligence for Numbers
            if col in nums.columns:
                try:
                    skew = series.skew()
                    if abs(skew) > 1.5:
                        direction = "양" if skew > 0 else "음"
                        report["insights"].append(f"<b>[분포 패턴]</b> '{col}'의 데이터가 <span style='color: #f7768e;'>{direction}의 방향으로 편향</span>되어 있습니다 (skew: {skew:.2f}).")
                    
                    # Outlier detection with Z-Score
                    z_scores = np.abs(stats.zscore(series.dropna()))
                    outliers_z = np.sum(z_scores > 3.0)
                    if outliers_z > 0:
                        report["insights"].append(f"<b>[지능형 감지]</b> '{col}'에서 신뢰수준 99% 이상의 <span style='color: #f7768e;'>이상치({outliers_z}개)</span>를 포착했습니다.")
                        report["suggestions"].append({"type": "박스 플롯", "x": col, "y": None, "desc": f"'{col}' 정밀 이상치 분석"})
                except: pass

            # Intelligence for Categories
            elif col in cats.columns:
                if unique_count < 10 and len(df) > 0:
                    counts = series.value_counts()
                    top_pct = (counts.iloc[0] / len(df)) * 100
                    if top_pct > 60:
                        report["insights"].append(f"<b>[범주 지능]</b> '{col}'은(는) <span style='color: #9ece6a;'>'{counts.index[0]}'</span> 값에 과반수 이상({top_pct:.1f}%) 집중되어 있습니다.")

        # 2. Advanced Relationship Matrix (Correlation)
        if len(nums.columns) >= 2:
            try:
                corr = nums.corr()
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        c_val = corr.iloc[i, j]
                        if abs(c_val) > 0.85:
                            c1, c2 = corr.columns[i], corr.columns[j]
                            report["insights"].append(f"<b>[초거대 상관성]</b> '{c1}' - '{c2}' 간의 강력한 상관계수(<span style='color: #bb9af7;'>{c_val:.2f}</span>)를 연산해냈습니다.")
                            report["suggestions"].append({"type": "산점도", "x": c1, "y": c2, "desc": "상관관계 정밀 분석 시각화"})
            except: pass

        if not report["suggestions"] and len(nums.columns) > 0:
            report["suggestions"].append({"type": "히스토그램", "x": nums.columns[0], "y": None, "desc": "기본 분포 보고서"})
        
        return report
