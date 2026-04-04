import re
from collections import Counter

import pandas as pd
import numpy as np
import polars as pl
from scipy import stats

# ── NLP graceful import ───────────────────────────────────────────────────────
try:
    from textblob import TextBlob
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False

# ── Column-name semantic dictionary ──────────────────────────────────────────
_SEMANTIC_MAP = [
    # (keyword_substring, label, icon)
    ("email",       "이메일 식별자",        "📧"),
    ("mail",        "이메일 식별자",        "📧"),
    ("phone",       "전화번호",             "📞"),
    ("tel",         "전화번호",             "📞"),
    ("mobile",      "전화번호",             "📞"),
    ("date",        "날짜 컬럼",            "📅"),
    ("time",        "시간 컬럼",            "⏱️"),
    ("year",        "연도 컬럼",            "📅"),
    ("month",       "월(月) 컬럼",          "📅"),
    ("day",         "일(日) 컬럼",          "📅"),
    ("age",         "연령 컬럼",            "🎂"),
    ("price",       "가격 컬럼",            "💰"),
    ("cost",        "비용 컬럼",            "💰"),
    ("amount",      "금액 컬럼",            "💰"),
    ("salary",      "급여 컬럼",            "💰"),
    ("revenue",     "매출 컬럼",            "💰"),
    ("_id",         "식별자(ID) 컬럼",      "🔑"),
    ("id",          "식별자(ID) 컬럼",      "🔑"),
    ("key",         "키(Key) 컬럼",         "🔑"),
    ("name",        "이름/명칭 컬럼",       "🏷️"),
    ("address",     "주소 컬럼",            "🗺️"),
    ("city",        "도시 컬럼",            "🏙️"),
    ("country",     "국가 컬럼",            "🌍"),
    ("gender",      "성별 컬럼",            "👤"),
    ("sex",         "성별 컬럼",            "👤"),
    ("score",       "점수 컬럼",            "📊"),
    ("rating",      "평점 컬럼",            "⭐"),
    ("review",      "리뷰/텍스트 컬럼",     "💬"),
    ("comment",     "코멘트 컬럼",          "💬"),
    ("description", "설명 텍스트 컬럼",     "📝"),
    ("desc",        "설명 컬럼",            "📝"),
    ("text",        "텍스트 컬럼",          "📝"),
    ("label",       "레이블 컬럼",          "🏷️"),
    ("category",    "카테고리 컬럼",        "📂"),
    ("cat",         "카테고리 컬럼",        "📂"),
    ("status",      "상태 컬럼",            "🔘"),
    ("flag",        "플래그 컬럼",          "🚩"),
    ("is_",         "불리언 플래그",        "✔️"),
    ("has_",        "불리언 플래그",        "✔️"),
    ("lat",         "위도 컬럼",            "🗺️"),
    ("lon",         "경도 컬럼",            "🗺️"),
    ("longitude",   "경도 컬럼",            "🗺️"),
    ("latitude",    "위도 컬럼",            "🗺️"),
    ("weight",      "무게/가중치 컬럼",     "⚖️"),
    ("height",      "높이 컬럼",            "📏"),
    ("count",       "카운트 컬럼",          "🔢"),
    ("qty",         "수량 컬럼",            "🔢"),
    ("quantity",    "수량 컬럼",            "🔢"),
    ("url",         "URL 컬럼",             "🔗"),
    ("link",        "링크 컬럼",            "🔗"),
    ("image",       "이미지 경로 컬럼",     "🖼️"),
    ("img",         "이미지 경로 컬럼",     "🖼️"),
    ("zip",         "우편번호 컬럼",        "📮"),
    ("grade",       "등급 컬럼",            "📊"),
    ("rank",        "순위 컬럼",            "🏅"),
    ("user",        "사용자 컬럼",          "👤"),
    ("product",     "상품 컬럼",            "📦"),
    ("item",        "아이템 컬럼",          "📦"),
]

# Basic stopwords (EN + KR)
_STOPWORDS = {
    "the", "a", "an", "in", "of", "and", "or", "is", "are", "to", "for",
    "with", "on", "at", "by", "it", "this", "that", "i", "you", "we",
    "he", "she", "they", "was", "be", "have", "has", "had", "not", "but",
    "이", "가", "을", "를", "에", "의", "은", "는", "도", "와", "과",
    "로", "으로", "한", "하다", "있다", "합니다", "그", "이다", "에서",
}


def _detect_column_semantic(col_name: str):
    lower = col_name.lower().replace("-", "_").replace(" ", "_")
    for keyword, label, icon in _SEMANTIC_MAP:
        if keyword in lower:
            return label, icon
    return None, None


class IntelligenceCore:
    """Statistical + NLP Data Profiling Engine — AI Core V7 (Multi-Engine)."""

    @staticmethod
    def analyze_full_profile(df):
        """360° profiling: statistics + NLP text analysis + semantic column detection."""
        is_polars = isinstance(df, pl.DataFrame)

        # ── Base metadata ─────────────────────────────────────────────────────
        if is_polars:
            rows = df.height
            cols = len(df.columns)
            engine_name = "Polars [Fast]"
            try:
                nulls = df.null_count().to_pandas().iloc[0].to_dict()
            except Exception:
                nulls = {c: 0 for c in df.columns}
        else:
            rows = len(df)
            cols = len(df.columns)
            engine_name = "Pandas [Standard]"
            nulls = df.isnull().sum().to_dict()

        if rows == 0:
            return {
                "summary": {},
                "insights": ["<b>[경고]</b> 분석 가능한 데이터가 없습니다."],
                "suggestions": [],
            }

        null_total = int(sum(v for v in nulls.values() if isinstance(v, (int, float))))
        nlp_status = (
            "<span style='color:#9ece6a;'>NLP 활성화 (TextBlob)</span>"
            if NLP_AVAILABLE
            else "<span style='color:#565f89;'>NLP 비활성화 (pip install textblob)</span>"
        )

        report = {
            "summary": {
                "rows": rows, "cols": cols,
                "engine": engine_name,
                "null_total": null_total,
                "nlp_available": NLP_AVAILABLE,
            },
            "insights": [
                f"<b>[엔진]</b> <span style='color:#bb9af7;'>{engine_name}</span> · {nlp_status}",
            ],
            "suggestions": [],
        }

        # ── Sampling for large DataFrames ─────────────────────────────────────
        if rows > 100_000:
            try:
                pf_df = df.sample(100_000).to_pandas() if is_polars else df.sample(100_000)
            except Exception:
                pf_df = df.to_pandas() if is_polars else df
            report["insights"].append(
                f"<b>[성능]</b> {rows:,}행 대용량 데이터 → 100,000행 지능형 샘플링 적용."
            )
        else:
            pf_df = df.to_pandas() if is_polars else df

        nums = pf_df.select_dtypes(include=[np.number])
        cats = pf_df.select_dtypes(include=["object", "category", "bool"])

        # ── Global null report ────────────────────────────────────────────────
        serious_nulls = {
            c: int(v) for c, v in nulls.items()
            if isinstance(v, (int, float)) and v > 0
        }
        if serious_nulls:
            worst = max(serious_nulls, key=lambda c: serious_nulls[c])
            worst_pct = serious_nulls[worst] / rows * 100
            report["insights"].append(
                f"<b>[결측 경고]</b> <span style='color:#f7768e;'>{len(serious_nulls)}개 컬럼</span>에 "
                f"결측치 존재. 최다: '<b>{worst}</b>' ({worst_pct:.1f}%)"
            )
            if worst_pct > 30:
                report["suggestions"].append({
                    "type": "히스토그램", "x": worst, "y": None,
                    "desc": f"'{worst}' 결측 {worst_pct:.0f}% — 분포 확인 권장",
                })

        # ── Per-column analysis ───────────────────────────────────────────────
        for col in pf_df.columns:
            series = pf_df[col]
            n_valid = series.count()
            unique_count = series.nunique()
            unique_ratio = unique_count / n_valid if n_valid > 0 else 0

            # 1) Semantic column name detection
            sem_label, sem_icon = _detect_column_semantic(col)
            if sem_label:
                report["insights"].append(
                    f"<b>[컬럼 의미]</b> {sem_icon} '<b>{col}</b>' "
                    f"→ <span style='color:#e0af68;'>{sem_label}</span>"
                )

            # 2) PK/unique identifier
            if unique_ratio > 0.98 and n_valid > 10:
                report["insights"].append(
                    f"<b>[구조]</b> '<b>{col}</b>' 고유 식별자(PK) 추정 "
                    f"(유니크 {unique_ratio*100:.1f}%)"
                )

            # ── Numeric column ────────────────────────────────────────────────
            if col in nums.columns:
                null_cnt = nulls.get(col, 0)
                if isinstance(null_cnt, (int, float)) and null_cnt / rows > 0.05:
                    report["insights"].append(
                        f"<b>[결측]</b> '<b>{col}</b>': "
                        f"<span style='color:#f7768e;'>{int(null_cnt)}개 "
                        f"({null_cnt/rows*100:.1f}%)</span> 결측"
                    )

                try:
                    skew = float(series.skew())
                    if abs(skew) > 2.0:
                        dir_str = "양(+)의" if skew > 0 else "음(-)의"
                        report["insights"].append(
                            f"<b>[분포]</b> '<b>{col}</b>': {dir_str} 편향 "
                            f"(왜도={skew:.2f}) → Log 변환 권장."
                        )
                        if skew > 0:
                            report["suggestions"].append({
                                "type": "히스토그램", "x": col, "y": None,
                                "desc": f"'{col}' 편향 분포 확인",
                            })
                except Exception as e:
                    report["insights"].append(
                        f"<b>[경고]</b> '<b>{col}</b>' 왜도 계산 실패: {type(e).__name__}"
                    )

                try:
                    valid = series.dropna()
                    if len(valid) > 10:
                        z = np.abs(stats.zscore(valid.head(10_000)))
                        outlier_n = int(np.sum(z > 3.0))
                        if outlier_n > 3:
                            report["insights"].append(
                                f"<b>[이상치]</b> '<b>{col}</b>': Z-Score 3σ 초과 "
                                f"<span style='color:#f7768e;'>{outlier_n}개</span> 감지."
                            )
                            report["suggestions"].append({
                                "type": "박스 플롯", "x": col, "y": None,
                                "desc": f"'{col}' 이상치 분포 확인",
                            })
                except Exception as e:
                    report["insights"].append(
                        f"<b>[경고]</b> '<b>{col}</b>' 이상치 분석 실패: {type(e).__name__}"
                    )

            # ── Categorical / Text column ─────────────────────────────────────
            elif col in cats.columns:
                if 1 < unique_count < 15:
                    try:
                        vc = series.value_counts()
                        top_val = vc.idxmax()
                        top_pct = vc.iloc[0] / n_valid * 100
                        report["insights"].append(
                            f"<b>[범주]</b> '<b>{col}</b>': 지배 값 = "
                            f"<span style='color:#9ece6a;'>'{top_val}'</span> ({top_pct:.1f}%)"
                        )
                    except Exception:
                        pass

                # NLP text column detection
                try:
                    non_null = series.dropna()
                    if len(non_null) > 0:
                        avg_len = non_null.astype(str).apply(len).mean()
                        if avg_len > 20 and unique_ratio > 0.3:
                            report["insights"].append(
                                f"<b>[NLP 감지]</b> '<b>{col}</b>': 텍스트 컬럼 추정 "
                                f"(평균 {avg_len:.0f}자) → NLP 분석 실행."
                            )
                            IntelligenceCore._analyze_text_column(
                                col, non_null, report, NLP_AVAILABLE
                            )
                except Exception:
                    pass

        # ── Correlation matrix ────────────────────────────────────────────────
        if len(nums.columns) >= 2:
            try:
                corr = nums.corr()
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        val = float(corr.iloc[i, j])
                        if not np.isnan(val) and abs(val) > 0.9:
                            c1, c2 = corr.columns[i], corr.columns[j]
                            report["insights"].append(
                                f"<b>[상관]</b> '<b>{c1}</b>' ↔ '<b>{c2}</b>': "
                                f"<span style='color:#7aa2f7;'>{val:.2f}</span> — 다중공선성 위험."
                            )
                            report["suggestions"].append({
                                "type": "산점도", "x": c1, "y": c2,
                                "desc": f"'{c1}' vs '{c2}' 강상관 시각화",
                            })
            except Exception as e:
                report["insights"].append(
                    f"<b>[경고]</b> 상관 분석 실패: {type(e).__name__}"
                )

        # ── Fallback suggestion ───────────────────────────────────────────────
        if not report["suggestions"]:
            if len(nums.columns) > 0:
                report["suggestions"].append({
                    "type": "히스토그램", "x": nums.columns[0], "y": None,
                    "desc": "기본 수치 분포 확인",
                })
            elif len(cats.columns) > 0:
                report["suggestions"].append({
                    "type": "바 차트", "x": cats.columns[0], "y": None,
                    "desc": "범주 빈도 분포 확인",
                })

        return report

    # ── NLP text-column analyser ──────────────────────────────────────────────
    @staticmethod
    def _analyze_text_column(col_name: str, series: pd.Series, report: dict, use_nlp: bool):
        """NLP deep-dive on a detected text column."""
        try:
            sample = series.astype(str).head(500)

            # Basic stats
            word_counts = sample.apply(lambda x: len(x.split()))
            avg_words = word_counts.mean()
            max_words = int(word_counts.max())

            # Top keywords (lightweight stopword filter)
            all_words = []
            for text in sample:
                for w in re.findall(r"[가-힣a-zA-Z]{2,}", text):
                    if w.lower() not in _STOPWORDS:
                        all_words.append(w.lower())

            top_words = Counter(all_words).most_common(5)
            top_str = ", ".join(
                f"<span style='color:#73daca;'>{w}</span>({c})" for w, c in top_words
            ) if top_words else "—"

            report["insights"].append(
                f"<b>[NLP 텍스트]</b> '<b>{col_name}</b>': "
                f"평균 {avg_words:.1f}단어/셀 (최대 {max_words}단어). "
                f"빈출어: {top_str}"
            )

            # Avg char length distribution
            char_counts = sample.apply(len)
            q25, q75 = char_counts.quantile(0.25), char_counts.quantile(0.75)
            report["insights"].append(
                f"<b>[NLP 길이]</b> '<b>{col_name}</b>': "
                f"문자 길이 Q1={q25:.0f} / Q3={q75:.0f} (IQR={q75-q25:.0f})"
            )

            # Sentiment via TextBlob (EN-optimised)
            if use_nlp:
                try:
                    from textblob import TextBlob
                    polarities = sample.apply(lambda x: TextBlob(x).sentiment.polarity)
                    avg_pol = float(polarities.mean())
                    pos_pct = int((polarities > 0.05).sum() / len(polarities) * 100)
                    neg_pct = int((polarities < -0.05).sum() / len(polarities) * 100)

                    if avg_pol > 0.05:
                        pol_label = f"<span style='color:#9ece6a;'>긍정적 ({avg_pol:+.2f})</span>"
                    elif avg_pol < -0.05:
                        pol_label = f"<span style='color:#f7768e;'>부정적 ({avg_pol:+.2f})</span>"
                    else:
                        pol_label = f"<span style='color:#e0af68;'>중립적 ({avg_pol:+.2f})</span>"

                    report["insights"].append(
                        f"<b>[NLP 감성]</b> '<b>{col_name}</b>': {pol_label} "
                        f"(긍정 {pos_pct}% / 부정 {neg_pct}%)"
                    )
                    report["suggestions"].append({
                        "type": "히스토그램", "x": col_name, "y": None,
                        "desc": f"'{col_name}' 텍스트 길이 분포 시각화",
                    })
                except Exception as e:
                    report["insights"].append(
                        f"<b>[NLP]</b> '<b>{col_name}</b>' 감성 분석 실패: {type(e).__name__}"
                    )

        except Exception as e:
            report["insights"].append(
                f"<b>[NLP 경고]</b> '<b>{col_name}</b>' 텍스트 분석 오류: {type(e).__name__}"
            )
