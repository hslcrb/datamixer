import pandas as pd
import numpy as np

class IntelligenceCore:
    """Enterprise AI Engine for data pattern analysis and automated insights."""
    
    @staticmethod
    def analyze_full_profile(df):
        """Perform 360-degree data profiling and pattern recognition."""
        if df.empty:
            return {"summary": {}, "insights": ["데이터가 비어 있습니다."], "suggestions": []}

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
        
        # 1. Missing Pattern Insight
        nulls = df.isnull().sum()
        has_nulls = False
        for col, count in nulls.items():
            if count > 0:
                pct = (count / len(df)) * 100
                report["insights"].append(f"<b>[데이터 무결성]</b> '{col}' 필드에 <span style='color: #e74c3c;'>{count:,}개({pct:.1f}%)</span>의 결측치가 발견되었습니다.")
                has_nulls = True
        if not has_nulls:
            report["insights"].append("<b>[데이터 무결성]</b> 결측치가 없는 깨끗한 데이터셋입니다.")

        # 2. Correlation Pattern (High Corel Only)
        if len(nums.columns) >= 2:
            try:
                corr = nums.corr()
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        c_val = corr.iloc[i, j]
                        if abs(c_val) > 0.70:
                            c1, c2 = corr.columns[i], corr.columns[j]
                            color = "#2ecc71" if c_val > 0 else "#e67e22"
                            report["insights"].append(f"<b>[상관관계 발견]</b> '{c1}' - '{c2}': <span style='color: {color};'>{c_val:.2f}</span>")
                            report["suggestions"].append({"type": "산점도", "x": c1, "y": c2, "desc": "강력한 연관 분석"})
            except: pass
            
        # 3. Distribution & Outlier Insight
        for col in nums.columns:
            try:
                q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
                iqr = q3 - q1
                outliers = df[(df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)]
                if len(outliers) > 0:
                    pct = (len(outliers) / len(df)) * 100
                    if pct < 10: # Only report if it's "unusual"
                        report["insights"].append(f"<b>[이상치 감지]</b> '{col}' 필드에서 {len(outliers):,}개의 특이값이 발견되었습니다.")
                        report["suggestions"].append({"type": "박스 플롯", "x": col, "y": None, "desc": f"'{col}' 이상치 분포"})
            except: pass

        # 4. Final Best Suggestion (If nothing major, default to standard)
        if not report["suggestions"]:
            if len(nums.columns) > 0:
                report["suggestions"].append({"type": "히스토그램", "x": nums.columns[0], "y": None, "desc": "기본 수치 분포"})
            elif len(cats.columns) > 0:
                # Use value counts for categorical
                report["suggestions"].append({"type": "히스토그램", "x": cats.columns[0], "y": None, "desc": "범주별 빈도 분석"})

        return report
