import pandas as pd
import numpy as np

class IntelligenceCore:
    """Enterprise AI Engine for data pattern analysis and automated insights."""
    
    @staticmethod
    def analyze_full_profile(df):
        """Perform 360-degree data profiling and pattern recognition."""
        report = {
            "summary": {
                "rows": len(df), "cols": len(df.columns), 
                "memory": f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
            },
            "insights": [],
            "suggestions": []
        }
        
        nums = df.select_dtypes(include=[np.number])
        cats = df.select_dtypes(include=['object', 'category'])
        
        # 1. Missing Pattern Insight
        nulls = df.isnull().sum()
        for col, count in nulls.items():
            if count > 0:
                pct = (count / len(df)) * 100
                report["insights"].append(f"<b>[결측치 경고]</b> '{col}' 필드에 {count}개({pct:.1f}%)의 데이터가 비어 있습니다.")

        # 2. Correlation Pattern (High Corel Only)
        if len(nums.columns) >= 2:
            corr = nums.corr()
            high_corr_found = False
            for i in range(len(corr.columns)):
                for j in range(i + 1, len(corr.columns)):
                    c_val = corr.iloc[i, j]
                    if abs(c_val) > 0.75:
                        c1, c2 = corr.columns[i], corr.columns[j]
                        report["insights"].append(f"<b>[상관관계 발견]</b> '{c1}'와 '{c2}'가 강력한 양의 상관관계({c_val:.2f})를 보입니다.")
                        report["suggestions"].append({"type": "산점도", "x": c1, "y": c2, "desc": "강력한 연관 데이터 분석"})
                        high_corr_found = True
            
        # 3. Distribution & Outlier Insight
        for col in nums.columns:
            q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
            iqr = q3 - q1
            outliers = df[(df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)]
            if len(outliers) > 0:
                report["insights"].append(f"<b>[이상치 감지]</b> '{col}' 필드에서 극단적인 값({len(outliers)}개)이 발견되었습니다.")
                report["suggestions"].append({"type": "박스 플롯", "x": col, "y": None, "desc": "이상치 분포 정밀 분석"})

        # 4. Final Best Suggestion (If nothing major, default to standard)
        if not report["suggestions"]:
            if len(nums.columns) > 0:
                report["suggestions"].append({"type": "히스토그램", "x": nums.columns[0], "y": None, "desc": "기본 수치 분포 분석"})
            elif len(cats.columns) > 0:
                report["suggestions"].append({"type": "박스 플롯", "x": cats.columns[0], "y": nums.columns[0] if len(nums.columns)>0 else None, "desc": "범주별 데이터 분포"})

        return report
