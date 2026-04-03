import pandas as pd
import numpy as np
import polars as pl
from scipy import stats

class IntelligenceCore:
    """Expert System for advanced statistical data profiling - AI Core V7 (Pd/Pl Multi-Engine)."""
    
    @staticmethod
    def analyze_full_profile(df):
        """High-performance 360 profiling with Multi-Engine Support (Optimized)."""
        is_polars = isinstance(df, pl.DataFrame)
        
        # 1. Base Intelligence (Native Polars/Pandas handling)
        if is_polars:
            rows = df.height; cols = len(df.columns)
            engine_name = "Polars [Fast]"
            # Lightweight null check natively in Polars
            nulls = df.null_count().to_pandas().iloc[0].to_dict() if rows < 5000000 else {c: "N/A" for c in df.columns}
        else:
            rows = len(df); cols = len(df.columns)
            engine_name = "Pandas [Standard]"
            nulls = df.isnull().sum().to_dict()

        if rows == 0:
            return {"summary": {}, "insights": ["분석 가능한 데이터가 없습니다."], "suggestions": []}

        report = {
            "summary": {"rows": rows, "cols": cols, "engine": engine_name},
            "insights": [f"<b>[Engine]</b> <span style='color: #bb9af7;'>{engine_name}</span> 엔진으로 매끄럽게 처리 중입니다."],
            "suggestions": []
        }
        
        # 2. Intelligent Sampling for Stats
        if rows > 100000:
            # Stats won't change much with 100k samples
            pf_df = df.sample(n=100000).to_pandas() if is_polars else df.sample(100000)
            report["insights"].append("<b>[Performance]</b> 대용량 데이터로 인해 지능형 샘플링(100k) 분석이 적용되었습니다.")
        else:
            pf_df = df.to_pandas() if is_polars else df

        nums = pf_df.select_dtypes(include=[np.number])
        cats = pf_df.select_dtypes(include=['object', 'category', 'bool'])
        
        # 3. Enhanced Statistical Intelligence
        for col in pf_df.columns:
            null_count = nulls.get(col, 0)
            series = pf_df[col]
            unique_count = series.nunique()
            unique_ratio = unique_count / len(pf_df) if len(pf_df) > 0 else 0
            
            if unique_ratio > 0.98 and len(pf_df) > 10:
                report["insights"].append(f"<b>[구조 지능]</b> '{col}'은(는) 고유 식별자(PK)입니다.")
            
            if col in nums.columns:
                try:
                    skew = series.skew()
                    if abs(skew) > 2.0:
                        dir_str = "양(+)의" if skew > 0 else "음(-)"
                        report["insights"].append(f"<b>[분포 패턴]</b> '{col}'은(는) {dir_str} 편향이 뚜렷합니다.")
                    
                    # Outlier Intelligence via Z-Score
                    valid_data = series.dropna()
                    if len(valid_data) > 10:
                        z_scores = np.abs(stats.zscore(valid_data.head(10000))) # Limit Z-score compute
                        outliers = np.sum(z_scores > 3.0)
                        if outliers > 3:
                            report["insights"].append(f"<b>[변동 감지]</b> '{col}'에서 특이 행동({outliers}+개)을 감지했습니다.")
                            report["suggestions"].append({"type": "박스 플롯", "x": col, "y": None, "desc": "이상 데이터 분포 상세 확인"})
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
