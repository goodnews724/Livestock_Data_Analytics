"""검역량 데이터 시각화
================================
데이터 소스: Google Sheets 요약 시트
  - 돈육_검역량_요약  (Sheet ID: 14yyGCTu0c0o1fRj9Rl28aAhuf4s8DUC1o-1XrKkzNcw)
  - 우육_검역량_요약  (Sheet ID: 1ioMr3eKMWwq-4dbBihglW5zWGaxLTBiv4l8JImlOIk4)

요약 시트 형식 (GAS 생성):
  헤더: 연도 | 품명 | country | 1월 | 2월 | ... | 12월 | 월평균
  값  : 톤(ton) 단위
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import gspread
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.oauth2.service_account import Credentials

# ── 설정 ──────────────────────────────────────────────────────────────────
SHEET_ID        = "13m-3z2LgX4BQ7JMT0dgOmKCHt_VXx71rFoSzBcMLFjE"
PORK_SHEET_NAME = "돈육_검역량_요약"
BEEF_SHEET_NAME = "우육_검역량_요약"

LOCAL_SA_PATH = Path("/opt/secrets/service_account.json")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

TODAY_YEAR = date.today().year
LAST_YEAR  = TODAY_YEAR - 1

COLOR_THIS  = "#e53e3e"
COLOR_LAST  = "#3182ce"
GRAY_SHADES = ["#aaaaaa", "#c4c4c4", "#dedede"]
COLOR_AVG   = "#718096"

PALETTE = [
    "#e53e3e", "#3182ce", "#38a169", "#d69e2e", "#805ad5",
    "#dd6b20", "#319795", "#b83280", "#2b6cb0", "#276749",
]

YEAR_PALETTE = [
    "#e53e3e", "#3182ce", "#38a169", "#d69e2e", "#805ad5",
    "#dd6b20", "#319795", "#b83280",
]

DEFAULT_SPECIES = "우육"
DEFAULT_COUNTRY = "미국"
DEFAULT_ITEM    = "갈비"


def _idx(lst: list, val: str) -> int:
    return lst.index(val) if val in lst else 0


_OTHER_PALETTE = ["#38a169", "#d69e2e", "#805ad5", "#dd6b20", "#319795", "#b83280", "#2b6cb0", "#276749"]

LINE_DASHES = ["solid", "dot", "dash", "dashdot", "longdash", "longdashdot"]


def _year_style(yr: int) -> tuple:
    """연도별 (width, opacity)."""
    if yr == TODAY_YEAR:
        return 3.0, 1.0
    if yr == LAST_YEAR:
        return 2.0, 0.85
    return 1.5, 0.5


def _year_colors(years: list) -> dict:
    """올해=빨강, 작년=파랑, 나머지=기타 색상."""
    result = {}
    other_idx = 0
    for yr in sorted(years, reverse=True):
        if yr == TODAY_YEAR:
            result[yr] = COLOR_THIS
        elif yr == LAST_YEAR:
            result[yr] = COLOR_LAST
        else:
            result[yr] = _OTHER_PALETTE[other_idx % len(_OTHER_PALETTE)]
            other_idx += 1
    return result

MONTHS       = list(range(1, 13))
MONTH_LABELS = [f"{m:02d}" for m in MONTHS]
MONTH_COLS   = [f"{m}월" for m in MONTHS]   # 요약 시트 컬럼명

# 국가명 오타 / 이표기 정규화
COUNTRY_ALIASES: dict[str, str] = {
    # 네덜란드
    "네덜랜드": "네덜란드",
    "화란": "네덜란드",
    # 포르투갈
    "포루투갈": "포르투갈",
    "포르투칼": "포르투갈",
    "포루투칼": "포르투갈",
    # 캐나다
    "카나다": "캐나다",
    "캐나나": "캐나다",
    # 덴마크
    "덴말크": "덴마크",
    # 벨기에
    "벨지움": "벨기에",
    "벨지에": "벨기에",
    # 오스트리아
    "오지리": "오스트리아",
    # 스페인
    "에스파냐": "스페인",
    # 영국
    "영국(UK)": "영국",
    # 뉴질랜드
    "뉴질렌드": "뉴질랜드",
    "뉴질랜": "뉴질랜드",
    # 아일랜드
    "아이랜드": "아일랜드",
    "아이얼랜드": "아일랜드",
    # 호주
    "오스트레일리아": "호주",
    "호주(AUS)": "호주",
    # 미국
    "미국(US)": "미국",
    "미국(USA)": "미국",
    # 브라질
    "브라질(BRA)": "브라질",
    # 헝가리
    "헝가이": "헝가리",
    # 핀란드
    "핀란": "핀란드",
    # 스웨덴
    "스웨": "스웨덴",
    # 프랑스
    "프랑": "프랑스",
}


def normalize_country(name: str) -> str:
    s = name.strip()
    return COUNTRY_ALIASES.get(s, s)

# ── 페이지 설정 ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="검역량 시각화",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
.block-container { padding: 0.6rem 1rem 1.5rem !important; max-width: 100% !important; }
#MainMenu, header, footer { visibility: hidden; }
[data-testid="stSidebar"] { display: none; }
.stApp { background: #f0f2f5; }
h2 { font-size: 1.15rem !important; margin: 0 0 4px !important; }
.sec-title {
  font-size: 1rem; font-weight: 700; color: #1e3a5f;
  border-left: 4px solid #3182ce; padding-left: 8px;
  margin: 1.4rem 0 0.6rem;
}
</style>
""", unsafe_allow_html=True)


# ── 인증 ─────────────────────────────────────────────────────────────────

@st.cache_resource
def _get_client() -> gspread.Client:
    if LOCAL_SA_PATH.exists():
        creds = Credentials.from_service_account_file(str(LOCAL_SA_PATH), scopes=SCOPES)
    else:
        info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


# ── 요약 시트 로드 & melt ──────────────────────────────────────────────────

def _load_summary_sheet(sheet_name: str, species: str) -> pd.DataFrame:
    """요약 시트 1개를 읽어 long-format DataFrame 반환."""
    gc = _get_client()
    ws = gc.open_by_key(SHEET_ID).worksheet(sheet_name)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame()

    headers = [h.strip() for h in rows[0]]
    df = pd.DataFrame(rows[1:], columns=headers)

    # 필수 컬럼 확인
    for col in ["연도", "품명", "country"]:
        if col not in df.columns:
            return pd.DataFrame()

    # 월 컬럼만 추출 (1월~12월)
    month_cols_present = [c for c in MONTH_COLS if c in df.columns]
    if not month_cols_present:
        return pd.DataFrame()

    # 값 정리
    df["연도"] = pd.to_numeric(df["연도"].str.replace("년", "", regex=False).str.strip(), errors="coerce")
    df = df[df["연도"].notna()].copy()
    df["연도"] = df["연도"].astype(int)
    df["품명"]   = df["품명"].str.strip()
    df["country"] = df["country"].str.strip().map(
        lambda x: COUNTRY_ALIASES.get(x, x)
    )
    df = df[df["country"] != "소계"].copy()

    for c in month_cols_present:
        df[c] = pd.to_numeric(
            df[c].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

    # wide → long
    df_long = df.melt(
        id_vars=["연도", "품명", "country"],
        value_vars=month_cols_present,
        var_name="월_label",
        value_name="ton",
    )
    df_long["month"] = df_long["월_label"].str.replace("월", "", regex=False).astype(int)
    df_long["year"]  = df_long["연도"]
    df_long["species"] = species
    df_long["period"]  = pd.to_datetime(
        df_long["year"].astype(str) + "-" + df_long["month"].astype(str).str.zfill(2) + "-01"
    )

    return df_long[["species", "year", "month", "period", "품명", "country", "ton"]].copy()


@st.cache_data(ttl=300, show_spinner=False)
def load_all() -> pd.DataFrame:
    pork = _load_summary_sheet(PORK_SHEET_NAME, "돈육")
    beef = _load_summary_sheet(BEEF_SHEET_NAME, "우육")
    df = pd.concat([pork, beef], ignore_index=True)
    # 0 또는 NaN은 데이터 없음 — NaN으로 통일
    df.loc[df["ton"] == 0, "ton"] = pd.NA
    return df


# ── 헬퍼 ─────────────────────────────────────────────────────────────────

def section(title: str) -> None:
    st.markdown(f'<div class="sec-title">{title}</div>', unsafe_allow_html=True)


CFG = {"displayModeBar": False}

LAYOUT_BASE = dict(
    plot_bgcolor="#fafafa",
    paper_bgcolor="#ffffff",
    hovermode="x unified",
)


# ══════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════

hdr, rbtn = st.columns([6, 1])
with hdr:
    st.markdown("## 📊 검역량 시각화")
with rbtn:
    if st.button("🔄 새로고침", width="stretch"):
        load_all.clear()
        st.rerun()

with st.spinner("데이터 불러오는 중..."):
    try:
        df_all   = load_all()
        load_err = None
    except Exception as e:
        df_all   = pd.DataFrame()
        load_err = str(e)

if load_err:
    st.error(f"데이터 로드 실패: {load_err}")
    st.stop()
if df_all.empty:
    st.info("데이터가 없습니다.")
    st.stop()

SPECIES_OPTS = ["돈육", "우육"]

# 기본값 세션 초기화 (이전 세션 상태가 남아있어도 최초 방문 시 기본값 적용)
for _k, _v in [("sp1", DEFAULT_SPECIES), ("sp2", DEFAULT_SPECIES)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ══════════════════════════════════════════════════════════════════════════
# ① 연도별 월별 비교 (연도 선택)
# ══════════════════════════════════════════════════════════════════════════

section("① 연도별 월별 비교  (연도·국가·품목 복수 선택 + 합산)")

r1c1, r1c2 = st.columns([1, 3])
sp1    = r1c1.selectbox("구분", SPECIES_OPTS, key="sp1")
df1_sp = df_all[df_all["species"] == sp1]

all_years1   = sorted(df1_sp["year"].dropna().unique(), reverse=True)
default_yrs1 = all_years1[:min(5, len(all_years1))]
sel_years1   = r1c2.multiselect("연도 선택", all_years1, default=default_yrs1, key="years1")

cnt1_all   = sorted(df1_sp["country"].dropna().unique())
items1_all = sorted(df1_sp["품명"].dropna().unique())
default_cnt1  = [DEFAULT_COUNTRY] if DEFAULT_COUNTRY in cnt1_all else cnt1_all[:1]
default_item1 = [DEFAULT_ITEM]    if DEFAULT_ITEM    in items1_all else items1_all[:1]

r2c1, r2c2 = st.columns(2)
countries1  = r2c1.multiselect("국가", cnt1_all, default=default_cnt1, key="cnt1_ms")
merge_cnt1  = r2c1.checkbox("국가 합산", value=False, key="merge_cnt1")
items1      = r2c2.multiselect("품목", items1_all, default=default_item1, key="item1_ms")
merge_item1 = r2c2.checkbox("품목 합산", value=False, key="merge_item1")

if not sel_years1 or not countries1 or not items1:
    st.info("연도, 국가, 품목을 각각 1개 이상 선택하세요.")
else:
    mask1 = (
        (df_all["species"] == sp1) &
        (df_all["year"].isin(sel_years1)) &
        (df_all["country"].isin(countries1)) &
        (df_all["품명"].isin(items1))
    )
    df1_raw = df_all[mask1].copy()

    group_keys1 = ["year", "month"]
    if not merge_cnt1:
        group_keys1.append("country")
    if not merge_item1:
        group_keys1.append("품명")
    df1_g = df1_raw.groupby(group_keys1)["ton"].sum(min_count=1).reset_index()

    # ── 차원별 인코딩 자동 결정 ──────────────────────────────────────────
    multi_cnt  = len(countries1) > 1 and not merge_cnt1
    multi_item = len(items1) > 1    and not merge_item1
    multi_yr   = len(sel_years1) > 1
    varying = [d for d, m in [("country", multi_cnt), ("item", multi_item), ("year", multi_yr)] if m]
    n_dims     = len(varying)
    color_dim   = varying[0] if n_dims >= 1 else None
    opacity_dim = varying[1] if n_dims >= 2 else None
    style_dim   = varying[2] if n_dims >= 3 else None

    year_color = _year_colors(sel_years1)
    cnt_color  = {c:  PALETTE[i % len(PALETTE)]     for i, c  in enumerate(countries1)}
    item_color = {it: PALETTE[i % len(PALETTE)]     for i, it in enumerate(items1)}
    cnt_dash   = {c:  LINE_DASHES[i % len(LINE_DASHES)] for i, c  in enumerate(countries1)}
    item_dash  = {it: LINE_DASHES[i % len(LINE_DASHES)] for i, it in enumerate(items1)}
    yr_sorted  = sorted(sel_years1)
    yr_dash    = {yr: LINE_DASHES[i % len(LINE_DASHES)] for i, yr in enumerate(yr_sorted)}

    def _enc(yr, c, it):
        """(color, dash, width, opacity)"""
        def _col():
            if color_dim == "country": return cnt_color.get(c, PALETTE[0])
            if color_dim == "item":    return item_color.get(it, PALETTE[0])
            if color_dim == "year":    return year_color.get(yr, PALETTE[0])
            return PALETTE[0]
        def _dash():
            if style_dim == "country": return cnt_dash.get(c, "solid")
            if style_dim == "item":    return item_dash.get(it, "solid")
            if style_dim == "year":    return yr_dash.get(yr, "solid")
            return "solid"
        w, op = _year_style(yr) if opacity_dim == "year" else (2.5, 0.9)
        return _col(), _dash(), w, op

    # ── 시리즈 빌딩 ────────────────────────────────────────────────────────
    series1 = []  # (label, df_sub, color, dash, width, opacity, yr)
    cnt_list  = ["합산"] if merge_cnt1  else countries1
    item_list = ["합산"] if merge_item1 else items1

    for yr in yr_sorted:
        df_yr = df1_g[df1_g["year"] == yr]
        for c in cnt_list:
            for it in item_list:
                if merge_cnt1 and merge_item1:
                    sub = df_yr
                elif merge_cnt1:
                    sub = df_yr[df_yr["품명"] == it]
                elif merge_item1:
                    sub = df_yr[df_yr["country"] == c]
                else:
                    sub = df_yr[(df_yr["country"] == c) & (df_yr["품명"] == it)]
                parts = ([c] if not merge_cnt1 else []) + ([it] if not merge_item1 else []) + [str(yr)]
                label = " / ".join(parts)
                color, dash, w, op = _enc(yr, c, it)
                series1.append((label, sub, color, dash, w, op, yr))

    fig1 = go.Figure()
    max_yr = max(sel_years1)
    for label, sub, color, dash, width, opacity, yr in series1:
        sub = sub.sort_values("month")
        sub = sub[sub["ton"].notna()]
        if sub.empty:
            continue
        is_latest = (yr == max_yr)
        vals = [sub.set_index("month")["ton"].get(m) for m in MONTHS]
        text = [f"{v:,.1f}t" if pd.notna(v) and v is not None else "" for v in vals] if is_latest else []
        fig1.add_trace(go.Scatter(
            x=MONTH_LABELS, y=vals, name=label,
            mode="lines+markers+text" if is_latest else "lines+markers",
            line=dict(color=color, width=width, dash=dash),
            marker=dict(size=6 if is_latest else 4, color=color),
            text=text, textposition="top center", textfont=dict(size=10, color=color),
            opacity=opacity,
        ))

    avg_val = df1_g["ton"].mean(skipna=True)
    if pd.notna(avg_val):
        fig1.add_hline(
            y=avg_val, line_dash="dot", line_color=COLOR_AVG, line_width=1.5,
            annotation_text=f"선택평균 {avg_val:,.1f}t",
            annotation_position="bottom right",
            annotation_font_size=10, annotation_font_color=COLOR_AVG,
        )

    fig1.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(tickmode="array", tickvals=MONTH_LABELS, ticktext=MONTH_LABELS,
                   title="월", title_font_size=14, tickfont_size=13, fixedrange=True),
        yaxis=dict(title="검역량 (톤)", title_font_size=14, tickfont_size=13, fixedrange=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font_size=13),
        font=dict(size=13),
        margin=dict(l=0, r=10, t=50, b=30),
        height=450,
    )
    st.plotly_chart(fig1, width="stretch", config=CFG)

    with st.expander(f"🔍 원본 데이터 확인 ({mask1.sum()}행)", expanded=False):
        st.dataframe(
            df_all[mask1].sort_values(["year", "month"])
            [["species", "year", "month", "country", "품명", "ton"]],
            width="stretch", height=250,
        )


# ══════════════════════════════════════════════════════════════════════════
# ② 추이 그래프
# ══════════════════════════════════════════════════════════════════════════

section("② 추이 그래프  (최근 3개월 / 1년 / 5년 / 직접 설정)")

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
sp2    = c1.selectbox("구분", SPECIES_OPTS, key="sp2")
df2_sp = df_all[df_all["species"] == sp2]
cnt2   = sorted(df2_sp["country"].dropna().unique())
cntry2 = c2.selectbox("국가", cnt2, index=_idx(cnt2, DEFAULT_COUNTRY), key="cntry2")
df2_c  = df2_sp[df2_sp["country"] == cntry2]
items2 = sorted(df2_c["품명"].dropna().unique())
item2  = c3.selectbox("품목", items2, index=_idx(items2, DEFAULT_ITEM), key="item2")
period2 = c4.radio("기간", ["최근 3개월", "최근 1년", "최근 5년", "직접 설정"], key="period2")

if period2 == "직접 설정":
    dr = st.date_input(
        "기간 범위",
        value=(date(TODAY_YEAR - 2, 1, 1), date.today()),
        min_value=date(2010, 1, 1),
        max_value=date.today(),
        key="dr2",
    )
    if isinstance(dr, (list, tuple)) and len(dr) == 2:
        first_p = pd.Timestamp(dr[0].replace(day=1))
        last_p  = pd.Timestamp(dr[1].replace(day=1))
    else:
        first_p = last_p = pd.Timestamp(date.today().replace(day=1))
else:
    last_p   = pd.Timestamp(date.today().replace(day=1))
    n_months = {"최근 3개월": 3, "최근 1년": 12, "최근 5년": 60}[period2]
    first_p  = last_p - pd.DateOffset(months=n_months - 1)

mask2 = (
    (df_all["species"] == sp2) &
    (df_all["country"] == cntry2) &
    (df_all["품명"] == item2)
)
df2_g = (
    df_all[mask2]
    .groupby("period")["ton"].sum(min_count=1)
    .reset_index()
    .sort_values("period")
)

if not df2_g.empty:
    df2_g = df2_g[(df2_g["period"] >= first_p) & (df2_g["period"] <= last_p)]
    full_range = pd.DataFrame({"period": pd.date_range(first_p, last_p, freq="MS")})
    df2_g = full_range.merge(df2_g, on="period", how="left")
    df2_g["label"] = df2_g["period"].dt.strftime("%y.%m")

if df2_g.empty:
    st.info("해당 조건의 데이터가 없습니다.")
else:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df2_g["period"], y=df2_g["ton"],
        mode="lines+markers",
        line=dict(color=COLOR_LAST, width=2.5), marker=dict(size=5),
        connectgaps=False,
        fill="tozeroy", fillcolor="rgba(49,130,206,0.08)",
        name="검역량",
        hovertemplate="%{x|%y.%m}  %{y:,.1f}t<extra></extra>",
    ))
    fig2.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(
            tickmode="array",
            tickvals=df2_g["period"].tolist(),
            ticktext=df2_g["label"].tolist(),
            tickangle=-45,
            title="날짜 (YY.MM)",
            fixedrange=True,
        ),
        yaxis=dict(title="검역량 (톤)", fixedrange=True),
        font=dict(size=11),
        margin=dict(l=0, r=10, t=30, b=70),
        height=380,
    )
    st.plotly_chart(fig2, width="stretch", config=CFG)
