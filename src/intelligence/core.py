"""
src/intelligence/core.py
Neuro-Level NLP + Statistical Profiling Engine — Datamixer AI Core V7
12-Stage NLP Pipeline: Structure → Language → Lexical → Pattern → N-gram
                      → TF-IDF → Topics → POS → Readability → Sentiment
                      → NER → Anomalies
"""
import re, math
from collections import Counter
import pandas as pd
import numpy as np
import polars as pl
from scipy import stats

# ── Graceful optional imports ──────────────────────────────────────────────────
def _imp(mod, attr=None):
    try:
        m = __import__(mod, fromlist=[attr] if attr else [])
        return getattr(m, attr) if attr else m
    except ImportError:
        return None

_TB   = _imp('textblob', 'TextBlob')
_LD   = _imp('langdetect')
_TFIDF= _imp('sklearn.feature_extraction.text', 'TfidfVectorizer')
_CVec = _imp('sklearn.feature_extraction.text', 'CountVectorizer')
_LDA  = _imp('sklearn.decomposition', 'LatentDirichletAllocation')

# NLTK setup with auto-download
_NLTK_OK = {}
try:
    import nltk as _nltk
    def _ndl(pkg, cat='tokenizers'):
        try:
            _nltk.data.find(f'{cat}/{pkg}')
            return True
        except LookupError:
            try:
                _nltk.download(pkg, quiet=True)
                return True
            except Exception:
                return False
    _NLTK_OK = {
        'punkt':  _ndl('punkt_tab') or _ndl('punkt'),
        'pos':    _ndl('averaged_perceptron_tagger_eng', 'taggers') or _ndl('averaged_perceptron_tagger', 'taggers'),
        'ne':     _ndl('maxent_ne_chunker', 'chunkers') and _ndl('words', 'corpora'),
        'sw':     _ndl('stopwords', 'corpora'),
    }
    _NLTK = True
except ImportError:
    _NLTK = False

F = {  # capability flags
    'tb': _TB is not None,
    'ld': _LD is not None,
    'sk': _TFIDF is not None,
    'nltk': _NLTK,
    'punkt': _NLTK_OK.get('punkt', False),
    'pos':   _NLTK_OK.get('pos',   False),
    'ne':    _NLTK_OK.get('ne',    False),
}

# ── Stopwords (EN + KR) ────────────────────────────────────────────────────────
_SW = frozenset({
    'the','a','an','in','of','and','or','is','are','was','were','to','for',
    'with','on','at','by','it','this','that','be','have','has','had','not',
    'but','i','you','we','he','she','they','my','your','our','its','as',
    'if','so','do','did','from','can','will','would','could','should',
    '이','가','을','를','에','의','은','는','도','와','과','로','으로','한',
    '있다','합니다','그','이다','에서','이고','하다','하고','에게','이런',
})

# ── Semantic column-name dictionary ───────────────────────────────────────────
_SEM = [
    ('email','이메일','📧'),('mail','이메일','📧'),('phone','전화번호','📞'),
    ('tel','전화번호','📞'),('mobile','휴대폰','📞'),('date','날짜','📅'),
    ('time','시간','⏱️'),('year','연도','📅'),('month','월','📅'),('day','일','📅'),
    ('age','연령','🎂'),('price','가격','💰'),('cost','비용','💰'),
    ('amount','금액','💰'),('salary','급여','💰'),('revenue','매출','💰'),
    ('_id','식별자','🔑'),('id','식별자','🔑'),('uuid','UUID','🔑'),
    ('name','이름/명칭','🏷️'),('title','제목','📌'),('address','주소','🗺️'),
    ('city','도시','🏙️'),('country','국가','🌍'),('zip','우편번호','📮'),
    ('lat','위도','📍'),('lon','경도','📍'),('gender','성별','👤'),
    ('score','점수','📊'),('rating','평점','⭐'),('rank','순위','🏅'),
    ('review','리뷰','💬'),('comment','댓글','💬'),('text','텍스트','📝'),
    ('content','본문','📝'),('body','본문','📝'),('message','메시지','📝'),
    ('description','설명','📝'),('desc','설명','📝'),('label','레이블','🏷️'),
    ('category','카테고리','📂'),('type','유형','📂'),('status','상태','🔘'),
    ('flag','플래그','🚩'),('is_','불리언','✔️'),('has_','불리언','✔️'),
    ('url','URL','🔗'),('link','링크','🔗'),('image','이미지','🖼️'),
    ('user','사용자','👤'),('customer','고객','👤'),('product','상품','📦'),
    ('order','주문','🛒'),('weight','가중치','⚖️'),('count','카운트','🔢'),
]

POS_KR = {
    'NN':'명사','NNS':'복수명사','NNP':'고유명사','NNPS':'복수고유명사',
    'VB':'동사','VBD':'과거동사','VBG':'현재분사','VBN':'과거분사',
    'VBP':'현재동사','VBZ':'3인칭동사',
    'JJ':'형용사','JJR':'비교형용사','JJS':'최상형용사',
    'RB':'부사','RBR':'비교부사','IN':'전치사','CC':'접속사',
    'PRP':'대명사','DT':'한정사','CD':'숫자','UH':'감탄사','FW':'외래어',
}

LANG_KR = {
    'ko':'🇰🇷 한국어','en':'🇺🇸 영어','ja':'🇯🇵 일본어','zh-cn':'🇨🇳 중국어',
    'fr':'🇫🇷 프랑스어','de':'🇩🇪 독일어','es':'🇪🇸 스페인어',
    'ar':'🇸🇦 아랍어','pt':'🇵🇹 포르투갈어','ru':'🇷🇺 러시아어',
    'it':'🇮🇹 이탈리아어','vi':'🇻🇳 베트남어','th':'🇹🇭 태국어',
}

# ── HTML helpers ───────────────────────────────────────────────────────────────
def _c(text, color): return f"<span style='color:{color};'>{text}</span>"
def _b(text): return f"<b>{text}</b>"
def _sec(title, icon=''):
    return (f"<div style='color:#7aa2f7;font-weight:bold;border-left:3px solid #7aa2f7;"
            f"padding-left:8px;margin:10px 0 4px 0;'>{icon} {title}</div>")


# ── Semantic lookup ────────────────────────────────────────────────────────────
def _sem(col):
    low = col.lower().replace('-','_').replace(' ','_')
    for kw, lbl, ico in _SEM:
        if kw in low:
            return lbl, ico
    return None, None


def _tokens(texts, n=200):
    joined = ' '.join(str(t) for t in texts[:n])
    if F['punkt']:
        from nltk import word_tokenize
        return [w.lower() for w in word_tokenize(joined)
                if re.fullmatch(r'[가-힣a-zA-Z]{2,}', w) and w.lower() not in _SW]
    return [w.lower() for w in re.findall(r'[가-힣a-zA-Z]{2,}', joined) if w.lower() not in _SW]


# ══════════════════════════════════════════════════════════════════════════════
class IntelligenceCore:
    """Neuro-Level NLP + Statistical Profiling — AI Core V7."""

    @staticmethod
    def analyze_full_profile(df):
        is_pl = isinstance(df, pl.DataFrame)
        if is_pl:
            rows, cn = df.height, len(df.columns)
            eng = "Polars [Fast]"
            try:    nulls = df.null_count().to_pandas().iloc[0].to_dict()
            except: nulls = {c: 0 for c in df.columns}
        else:
            rows, cn = len(df), len(df.columns)
            eng = "Pandas [Standard]"
            nulls = df.isnull().sum().to_dict()

        if rows == 0:
            return {"summary": {}, "insights": [_b("[경고]") + " 데이터가 비어 있습니다."], "suggestions": []}

        null_tot = int(sum(v for v in nulls.values() if isinstance(v, (int, float))))
        nlp_caps = " · ".join(
            _c(n + "✓", "#9ece6a") if ok else _c(n + "✗", "#565f89")
            for n, ok in [("TextBlob", F['tb']), ("langdetect", F['ld']),
                          ("sklearn", F['sk']), ("NLTK", F['nltk'])]
        )

        report = {
            "summary": {"rows": rows, "cols": cn, "engine": eng, "null_total": null_tot},
            "insights": [
                f"{_b('[시스템]')} 엔진: {_c(eng, '#bb9af7')} | {nlp_caps}",
                f"{_b('[개요]')} {_c(f'{rows:,}행', '#9ece6a')} × {_c(f'{cn}열', '#9ece6a')} | "
                f"총 결측: {_c(str(null_tot), '#f7768e')}개",
            ],
            "suggestions": [],
        }

        # Sampling
        if rows > 100_000:
            try:    pf = df.sample(100_000).to_pandas() if is_pl else df.sample(100_000)
            except: pf = df.to_pandas() if is_pl else df
            report["insights"].append(f"{_b('[성능]')} {rows:,}행 → 100k 샘플링 적용.")
        else:
            pf = df.to_pandas() if is_pl else df

        nums = pf.select_dtypes(include=[np.number])
        cats = pf.select_dtypes(include=['object', 'category', 'bool'])

        # Global null summary
        bad = {c: int(v) for c, v in nulls.items() if isinstance(v, (int, float)) and v > 0}
        if bad:
            worst = max(bad, key=lambda c: bad[c])
            wpct = bad[worst] / rows * 100
            report["insights"].append(
                f"{_b('[결측 경고]')} {_c(f'{len(bad)}개 컬럼', '#f7768e')}에 결측 존재. "
                f"최다: {_b(worst)} ({_c(f'{wpct:.1f}%', '#f7768e')})"
            )

        # Per-column
        for col in pf.columns:
            s = pf[col]
            nv = s.count()
            un = s.nunique()
            ur = un / nv if nv > 0 else 0

            lbl, ico = _sem(col)
            if lbl:
                report["insights"].append(f"{_b('[컬럼 의미]')} {ico} {_b(col)} → {_c(lbl, '#e0af68')}")

            if ur > 0.98 and nv > 10:
                report["insights"].append(
                    f"{_b('[구조]')} {_b(col)}: PK 추정 ({_c(f'{ur*100:.1f}%', '#9ece6a')} unique)")

            if col in nums.columns:
                IntelligenceCore._num(col, s, nulls, rows, report)
            elif col in cats.columns:
                IntelligenceCore._cat(col, s, un, ur, report)

        # Correlation matrix
        if len(nums.columns) >= 2:
            try:
                corr = nums.corr()
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        v = float(corr.iloc[i, j])
                        if not math.isnan(v) and abs(v) > 0.9:
                            c1, c2 = corr.columns[i], corr.columns[j]
                            report["insights"].append(
                                f"{_b('[상관]')} {_b(c1)} ↔ {_b(c2)}: "
                                f"{_c(f'{v:.2f}', '#7aa2f7')} — 다중공선성 위험.")
                            report["suggestions"].append({"type": "산점도", "x": c1, "y": c2,
                                                          "desc": f"'{c1}' vs '{c2}' 강상관"})
            except Exception as e:
                report["insights"].append(f"{_b('[경고]')} 상관 분석 실패: {type(e).__name__}")

        if not report["suggestions"]:
            if len(nums.columns) > 0:
                report["suggestions"].append({"type": "히스토그램", "x": nums.columns[0],
                                              "y": None, "desc": "기본 수치 분포"})
            elif len(cats.columns) > 0:
                report["suggestions"].append({"type": "바 차트", "x": cats.columns[0],
                                              "y": None, "desc": "범주 빈도"})
        return report

    # ── Numeric ───────────────────────────────────────────────────────────────
    @staticmethod
    def _num(col, s, nulls, rows, report):
        nc = nulls.get(col, 0)
        if isinstance(nc, (int, float)) and nc / rows > 0.05:
            report["insights"].append(
                f"{_b('[결측]')} {_b(col)}: {_c(f'{int(nc)}개 ({nc/rows*100:.1f}%)', '#f7768e')}")
        valid = s.dropna()
        if len(valid) < 5: return
        try:
            d = valid.describe()
            mean_, std_ = float(d['mean']), float(d['std'])
            
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                skew_ = float(valid.skew())
                kurt_ = float(valid.kurtosis())
            
            cv = abs(std_ / mean_ * 100) if mean_ != 0 else 0
            report["insights"].append(
                f"{_b('[수치]')} {_b(col)}: μ={_c(f'{mean_:.3g}','#73daca')} "
                f"σ={_c(f'{std_:.3g}','#73daca')} CV={_c(f'{cv:.1f}%','#e0af68')} "
                f"왜도={_c(f'{skew_:.2f}','#bb9af7')} 첨도={_c(f'{kurt_:.2f}','#bb9af7')}")
            if abs(skew_) > 2.0:
                report["insights"].append(
                    f"{_b('[분포]')} {_b(col)}: {'양(+)의' if skew_>0 else '음(-)의'} 편향 "
                    f"(왜도={skew_:.2f}) → {_c('Log 변환 권장','#e0af68')}")
                report["suggestions"].append({"type": "히스토그램", "x": col,
                                              "y": None, "desc": f"'{col}' 편향 분포"})
            if abs(kurt_) > 3:
                report["insights"].append(
                    f"{_b('[첨도]')} {_b(col)}: {'뾰족한' if kurt_>3 else '납작한'} 분포 "
                    f"(첨도={kurt_:.2f})")
        except Exception as e:
            report["insights"].append(f"{_b('[경고]')} {_b(col)} 통계 실패: {type(e).__name__}")
        try:
            if len(valid) > 10:
                v_s = valid.head(10_000)
                if v_s.std() < 1e-10:
                    report["insights"].append(
                        f"{_b('[균일]')} {_b(col)}: 모든 값이 동일 — 이상치 없음.")
                else:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", RuntimeWarning)
                        z = np.abs(stats.zscore(v_s))
                    on = int(np.sum(z > 3.0))
                    if on > 3:
                        report["insights"].append(
                            f"{_b('[이상치]')} {_b(col)}: Z>3σ {_c(f'{on}개','#f7768e')} 감지.")
                        report["suggestions"].append({"type": "박스 플롯", "x": col,
                                                      "y": None, "desc": f"'{col}' 이상치"})
        except Exception as e:
            report["insights"].append(f"{_b('[경고]')} {_b(col)} 이상치 실패: {type(e).__name__}")

    # ── Categorical ───────────────────────────────────────────────────────────
    @staticmethod
    def _cat(col, s, un, ur, report):
        nn = s.dropna()
        if len(nn) == 0: return
        avg_len = nn.astype(str).apply(len).mean()
        if un == 2:
            vals = nn.unique().tolist()
            report["insights"].append(
                f"{_b('[이진]')} {_b(col)}: {_c(str(vals[0]),'#9ece6a')} / {_c(str(vals[1]),'#f7768e')}")
            return
        if 2 < un < 20:
            try:
                vc = nn.value_counts()
                top, pct = vc.idxmax(), vc.iloc[0] / len(nn) * 100
                report["insights"].append(
                    f"{_b('[범주]')} {_b(col)}: 지배값 = {_c(repr(top),'#9ece6a')} ({pct:.1f}%) | {un}개 유형")
                if pct > 80:
                    report["insights"].append(
                        f"{_b('[경고]')} {_b(col)}: 단일값 {_c(f'{pct:.0f}%','#f7768e')} 점유 — 불균형!")
                report["suggestions"].append({"type": "바 차트", "x": col,
                                              "y": None, "desc": f"'{col}' 범주 빈도"})
            except Exception: pass
            return
        if avg_len > 15 and ur > 0.2:
            report["insights"].append(
                f"{_b('[NLP 감지]')} {_b(col)}: 텍스트 컬럼 추정 (평균 {avg_len:.0f}자, unique {ur*100:.0f}%) "
                f"→ {_c('12단계 NLP 파이프라인 가동','#7aa2f7')}")
            IntelligenceCore._nlp(col, nn, report)

    # ══════════════════════════════════════════════════════════════════════════
    # 12-STAGE NLP PIPELINE
    # ══════════════════════════════════════════════════════════════════════════
    @staticmethod
    def _nlp(col, series, report):
        s500 = series.astype(str).head(500)
        s200 = series.astype(str).head(200)
        IntelligenceCore._nlp_structure(col, s500, series, report)   # Stage 1
        IntelligenceCore._nlp_language(col, s500, report)             # Stage 2
        IntelligenceCore._nlp_lexical(col, s500, report)              # Stage 3
        IntelligenceCore._nlp_patterns(col, s500, report)             # Stage 4
        IntelligenceCore._nlp_ngrams(col, s500, report)               # Stage 5
        IntelligenceCore._nlp_tfidf(col, s500, report)                # Stage 6
        IntelligenceCore._nlp_topics(col, s200, report)               # Stage 7
        IntelligenceCore._nlp_pos(col, s200, report)                  # Stage 8
        IntelligenceCore._nlp_readability(col, s500, report)          # Stage 9
        IntelligenceCore._nlp_sentiment(col, s500, report)            # Stage 10
        IntelligenceCore._nlp_ner(col, s200, report)                  # Stage 11
        IntelligenceCore._nlp_anomalies(col, series, report)          # Stage 12

    # Stage 1: Text structure
    @staticmethod
    def _nlp_structure(col, sample, full, report):
        try:
            wc = sample.apply(lambda x: len(x.split()))
            cc = sample.apply(len)
            sc = sample.apply(lambda x: max(1, len(re.split(r'[.!?。！？\n]', x)) - 1))
            wps = (wc / sc.clip(lower=1)).mean()
            q1, q3 = cc.quantile(0.25), cc.quantile(0.75)
            report["insights"].append(
                f"{_sec('📐 텍스트 구조 분석', '')}"
                f"&nbsp;&nbsp;{_b(col)}: 평균 {_c(f'{cc.mean():.0f}자','#73daca')} / "
                f"{_c(f'{wc.mean():.1f}단어','#73daca')} / {_c(f'{sc.mean():.1f}문장','#73daca')} | "
                f"문장당 {_c(f'{wps:.1f}단어','#e0af68')} | IQR: {q1:.0f}~{q3:.0f}자 "
                f"(범위 {q3-q1:.0f})")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-1]')} 구조 실패: {type(e).__name__}")

    # Stage 2: Language detection
    @staticmethod
    def _nlp_language(col, sample, report):
        if not F['ld']: return
        try:
            cnt = Counter()
            for t in sample.head(150):
                try: cnt[_LD.detect(str(t))] += 1
                except Exception: pass
            if not cnt: return
            total = sum(cnt.values())
            parts = [f"{LANG_KR.get(l, l)} {_c(f'{c/total*100:.0f}%','#9ece6a')}"
                     for l, c in cnt.most_common(3)]
            report["insights"].append(
                f"{_sec('🌐 언어 감지', '')}&nbsp;&nbsp;{_b(col)}: {' | '.join(parts)}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-2]')} 언어 감지 실패: {type(e).__name__}")

    # Stage 3: Lexical richness
    @staticmethod
    def _nlp_lexical(col, sample, report):
        try:
            words = [w.lower() for t in sample
                     for w in re.findall(r'[가-힣a-zA-Z]{2,}', str(t)) if w.lower() not in _SW]
            if not words: return
            V, N = len(set(words)), len(words)
            ttr = V / N
            freq = Counter(words)
            hapax = sum(1 for c in freq.values() if c == 1)
            herdan = math.log(V) / math.log(N) if N > 1 else 0
            # Yule's K
            m1 = N
            m2 = sum(c * c for c in freq.values())
            yule_k = 10_000 * (m2 - m1) / (m1 * m1) if m1 > 0 else 0
            report["insights"].append(
                f"{_sec('📚 어휘 풍부도', '')}"
                f"&nbsp;&nbsp;{_b(col)}: TTR={_c(f'{ttr:.3f}','#bb9af7')} | "
                f"총 단어={_c(str(N),'#7aa2f7')}개 | 고유={_c(str(V),'#9ece6a')}개 | "
                f"Hapax={_c(f'{hapax}개','#e0af68')} | "
                f"Herdan C={_c(f'{herdan:.3f}','#73daca')} | "
                f"Yule K={_c(f'{yule_k:.1f}','#bb9af7')}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-3]')} 어휘 분석 실패: {type(e).__name__}")

    # Stage 4: Pattern detection
    @staticmethod
    def _nlp_patterns(col, sample, report):
        try:
            def cp(pat): return sum(len(re.findall(pat, str(t))) for t in sample)
            patterns = [
                ('URL',       r'https?://\S+|www\.\S+',            '#7aa2f7'),
                ('이메일',    r'\b[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}\b', '#9ece6a'),
                ('전화번호',  r'(\+?\d[\d\s\-]{7,}\d)',            '#e0af68'),
                ('해시태그',  r'#\w+',                             '#73daca'),
                ('@멘션',     r'@\w+',                             '#bb9af7'),
                ('이모지',    r'[\U0001F300-\U0001FFFF]',           '#f7768e'),
                ('숫자포함',  r'\d+',                              '#565f89'),
            ]
            found = [(lbl, cp(pat), clr) for lbl, pat, clr in patterns if cp(pat) > 0]
            if found:
                parts = " | ".join(f"{lbl} {_c(str(n)+'개', clr)}" for lbl, n, clr in found)
                report["insights"].append(
                    f"{_sec('🔍 패턴 감지', '')}&nbsp;&nbsp;{_b(col)}: {parts}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-4]')} 패턴 실패: {type(e).__name__}")

    # Stage 5: N-gram analysis
    @staticmethod
    def _nlp_ngrams(col, sample, report):
        try:
            toks = _tokens(sample.tolist())
            if len(toks) < 4: return
            def ngrams(seq, n): return [tuple(seq[i:i+n]) for i in range(len(seq)-n+1)]
            bg = Counter(ngrams(toks, 2)).most_common(5)
            tg = Counter(ngrams(toks, 3)).most_common(3)
            bg_str = " | ".join(f"{_c(' '.join(g),'#73daca')}({c})" for g, c in bg) or '—'
            tg_str = " | ".join(f"{_c(' '.join(g),'#9ece6a')}({c})" for g, c in tg) or '—'
            report["insights"].append(
                f"{_sec('🔗 N-gram 분석', '')}"
                f"&nbsp;&nbsp;{_b('Bigram')}: {bg_str}<br>"
                f"&nbsp;&nbsp;{_b('Trigram')}: {tg_str}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-5]')} N-gram 실패: {type(e).__name__}")

    # Stage 6: TF-IDF
    @staticmethod
    def _nlp_tfidf(col, sample, report):
        if not F['sk']: return
        try:
            texts = sample.tolist()
            tf = _TFIDF(max_features=60, min_df=2, token_pattern=r'[가-힣a-zA-Z]{2,}')
            X = tf.fit_transform(texts)
            terms = tf.get_feature_names_out()
            scores = np.array(X.mean(axis=0)).flatten()
            top = sorted(zip(terms, scores), key=lambda x: -x[1])[:8]
            if top:
                parts = " | ".join(f"{_c(t,'#7aa2f7')}({s:.3f})" for t, s in top if s > 0)
                report["insights"].append(
                    f"{_sec('🧬 TF-IDF 핵심 키워드', '')}&nbsp;&nbsp;{_b(col)}: {parts}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-6]')} TF-IDF 실패: {type(e).__name__}")

    # Stage 7: LDA Topic Modeling
    @staticmethod
    def _nlp_topics(col, sample_200, report):
        if not (F['sk'] and _LDA and _CVec): return
        try:
            texts = sample_200.tolist()
            if len(texts) < 10: return
            cv = _CVec(max_features=50, min_df=2, token_pattern=r'[가-힣a-zA-Z]{2,}')
            X = cv.fit_transform(texts)
            if X.shape[1] < 5: return
            n = min(3, max(2, X.shape[1] // 8))
            lda = _LDA(n_components=n, random_state=42, max_iter=25)
            lda.fit(X)
            feat = cv.get_feature_names_out()
            parts = []
            for i, topic in enumerate(lda.components_):
                top_w = [feat[j] for j in topic.argsort()[:-6:-1]]
                parts.append(f"Topic{i+1}: {_c(', '.join(top_w), '#e0af68')}")
            report["insights"].append(
                f"{_sec('🧠 잠재 토픽 모델링 (LDA)', '')}&nbsp;&nbsp;{_b(col)}: "
                + " | ".join(parts))
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-7]')} LDA 실패: {type(e).__name__}")

    # Stage 8: POS distribution
    @staticmethod
    def _nlp_pos(col, sample, report):
        if not (F['nltk'] and F['pos'] and F['punkt']): return
        try:
            from nltk import pos_tag, word_tokenize
            joined = ' '.join(str(t) for t in sample.head(80))
            toks = word_tokenize(joined)[:600]
            tags = pos_tag(toks)
            cnt = Counter(tag[:2] for _, tag in tags)
            total = sum(cnt.values())
            top = cnt.most_common(6)
            parts = " | ".join(
                f"{_c(POS_KR.get(t, t),'#bb9af7')}({_c(f'{c/total*100:.0f}%','#73daca')})"
                for t, c in top)
            report["insights"].append(
                f"{_sec('🔠 품사(POS) 분포', '')}&nbsp;&nbsp;{_b(col)}: {parts}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-8]')} POS 실패: {type(e).__name__}")

    # Stage 9: Readability
    @staticmethod
    def _nlp_readability(col, sample, report):
        try:
            def syl(word):
                w = word.lower()
                count = len(re.findall(r'[aeiou]+', w))
                if w.endswith('e') and count > 1: count -= 1
                # Korean: count syllable blocks (each char = 1 syllable)
                count += len(re.findall(r'[가-힣]', word))
                return max(1, count)

            scores = []
            for text in sample.head(100):
                sents = [s for s in re.split(r'[.!?]', str(text)) if s.strip()]
                words = str(text).split()
                if not sents or not words: continue
                syllables = sum(syl(w) for w in words)
                fre = 206.835 - 1.015 * (len(words)/len(sents)) - 84.6 * (syllables/len(words))
                scores.append(fre)
            if not scores: return
            avg_fre = np.mean(scores)
            if avg_fre >= 90:   level = _c("매우 쉬움 (초등)", "#9ece6a")
            elif avg_fre >= 70: level = _c("쉬움 (중학)", "#9ece6a")
            elif avg_fre >= 50: level = _c("보통 (고등)", "#e0af68")
            elif avg_fre >= 30: level = _c("어려움 (대학)", "#f7768e")
            else:               level = _c("매우 어려움 (전문)", "#f7768e")
            report["insights"].append(
                f"{_sec('📖 가독성 분석 (Flesch RE)', '')}"
                f"&nbsp;&nbsp;{_b(col)}: 점수={_c(f'{avg_fre:.1f}','#7aa2f7')} → {level}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-9]')} 가독성 실패: {type(e).__name__}")

    # Stage 10: Sentiment analysis
    @staticmethod
    def _nlp_sentiment(col, sample, report):
        if not F['tb']: return
        try:
            pols, subs = [], []
            for t in sample.head(300):
                blob = _TB(str(t))
                pols.append(blob.sentiment.polarity)
                subs.append(blob.sentiment.subjectivity)
            if not pols: return
            avg_p, avg_s = np.mean(pols), np.mean(subs)
            pos_pct = int(np.sum(np.array(pols) > 0.05) / len(pols) * 100)
            neg_pct = int(np.sum(np.array(pols) < -0.05) / len(pols) * 100)
            neu_pct = 100 - pos_pct - neg_pct
            if avg_p > 0.05:   pl_lbl = _c(f"긍정 ({avg_p:+.2f})", "#9ece6a")
            elif avg_p < -0.05: pl_lbl = _c(f"부정 ({avg_p:+.2f})", "#f7768e")
            else:               pl_lbl = _c(f"중립 ({avg_p:+.2f})", "#e0af68")
            sub_lbl = _c("주관적", "#bb9af7") if avg_s > 0.5 else _c("객관적", "#73daca")
            report["insights"].append(
                f"{_sec('💡 감성 분석 (Sentiment)', '')}"
                f"&nbsp;&nbsp;{_b(col)}: {pl_lbl} | 주관성={_c(f'{avg_s:.2f}',  '#bb9af7')} ({sub_lbl}) | "
                f"긍정 {_c(f'{pos_pct}%','#9ece6a')} / 중립 {_c(f'{neu_pct}%','#e0af68')} / "
                f"부정 {_c(f'{neg_pct}%','#f7768e')}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-10]')} 감성 실패: {type(e).__name__}")

    # Stage 11: Named Entity Recognition
    @staticmethod
    def _nlp_ner(col, sample, report):
        if not (F['nltk'] and _NLTK_OK.get('ne') and F['punkt'] and F['pos']): return
        try:
            from nltk import word_tokenize, pos_tag, ne_chunk
            joined = ' '.join(str(t) for t in sample.head(50))[:3000]
            tree = ne_chunk(pos_tag(word_tokenize(joined)), binary=False)
            entities = Counter()
            for subtree in tree:
                if hasattr(subtree, 'label'):
                    entities[subtree.label()] += 1
            if entities:
                ner_labels = {
                    'PERSON': '👤 인물', 'ORGANIZATION': '🏢 기관', 'GPE': '🌍 지정학',
                    'LOCATION': '📍 장소', 'FACILITY': '🏗️ 시설', 'GSP': '🌐 지리사회',
                }
                parts = " | ".join(
                    f"{ner_labels.get(k, k)} {_c(str(v)+'개','#9ece6a')}"
                    for k, v in entities.most_common(5))
                report["insights"].append(
                    f"{_sec('🏷️ 개체명 인식 (NER)', '')}&nbsp;&nbsp;{_b(col)}: {parts}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-11]')} NER 실패: {type(e).__name__}")

    # Stage 12: Text anomaly detection
    @staticmethod
    def _nlp_anomalies(col, series, report):
        try:
            lens = series.astype(str).apply(len)
            if len(lens) < 10: return
            z = np.abs(stats.zscore(lens))
            anom_n = int((z > 3).sum())
            empty_n = int((lens == 0).sum())
            vshort = int((lens < 5).sum())
            vlong = int((lens > lens.quantile(0.95) * 3).sum())
            parts = []
            if anom_n:  parts.append(f"길이 이상치={_c(str(anom_n)+'개','#f7768e')}")
            if empty_n: parts.append(f"빈 텍스트={_c(str(empty_n)+'개','#f7768e')}")
            if vshort:  parts.append(f"초단문(5자↓)={_c(str(vshort)+'개','#e0af68')}")
            if vlong:   parts.append(f"초장문(P95×3↑)={_c(str(vlong)+'개','#e0af68')}")
            if parts:
                report["insights"].append(
                    f"{_sec('⚠️ 텍스트 이상 감지', '')}&nbsp;&nbsp;{_b(col)}: " + " | ".join(parts))
            else:
                report["insights"].append(
                    f"{_sec('✅ 텍스트 품질', '')}&nbsp;&nbsp;{_b(col)}: "
                    f"{_c('이상 텍스트 없음 — 균일한 품질','#9ece6a')}")
        except Exception as e:
            report["insights"].append(f"{_b('[NLP-12]')} 이상 감지 실패: {type(e).__name__}")
