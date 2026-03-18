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
PORK_SHEET_ID = "14yyGCTu0c0o1fRj9Rl28aAhuf4s8DUC1o-1XrKkzNcw"
BEEF_SHEET_ID = "1ioMr3eKMWwq-4dbBihglW5zWGaxLTBiv4l8JImlOIk4"
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

MONTHS       = list(range(1, 13))
MONTH_LABELS = [f"{m:02d}" for m in MONTHS]
MONTH_COLS   = [f"{m}월" for m in MONTHS]   # 요약 시트 컬럼명

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

def _load_summary_sheet(sheet_id: str, sheet_name: str, species: str) -> pd.DataFrame:
    """요약 시트 1개를 읽어 long-format DataFrame 반환."""
    gc = _get_client()
    ws = gc.open_by_key(sheet_id).worksheet(sheet_name)
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
    df["품명"] = df["품명"].str.strip()
    df["country"] = df["country"].str.strip()

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
    pork = _load_summary_sheet(PORK_SHEET_ID, PORK_SHEET_NAME, "돈육")
    beef = _load_summary_sheet(BEEF_SHEET_ID, BEEF_SHEET_NAME, "우육")
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
    margin=dict(l=0, r=10, t=50, b=30),
    hovermode="x unified",
    font=dict(size=11),
)


# ══════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════

hdr, rbtn = st.columns([6, 1])
with hdr:
    st.markdown("## 📊 검역량 시각화")
with rbtn:
    if st.button("🔄 새로고침", use_container_width=True):
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


# ══════════════════════════════════════════════════════════════════════════
# ① 연도별 월별 비교 (최근 5년)
# ══════════════════════════════════════════════════════════════════════════

section("① 연도별 월별 비교  (최근 5년 · 올해=빨강, 작년=파랑, 나머지=회색)")

c1, c2, c3 = st.columns(3)
sp1    = c1.selectbox("구분", SPECIES_OPTS, key="sp1")
df1_sp = df_all[df_all["species"] == sp1]
cnt1   = sorted(df1_sp["country"].dropna().unique())
cntry1 = c2.selectbox("국가", cnt1, key="cntry1")
df1_c  = df1_sp[df1_sp["country"] == cntry1]
items1 = sorted(df1_c["품명"].dropna().unique())
item1  = c3.selectbox("품목", items1, key="item1")

years5 = list(range(TODAY_YEAR - 4, TODAY_YEAR + 1))
mask1  = (
    (df_all["species"] == sp1) &
    (df_all["country"] == cntry1) &
    (df_all["품명"] == item1) &
    (df_all["year"].isin(years5))
)
df1_g = (
    df_all[mask1]
    .groupby(["year", "month"])["ton"]
    .sum(min_count=1)
    .reset_index()
)

avg_val = df1_g["ton"].mean(skipna=True) if not df1_g.empty else 0

fig1 = go.Figure()

for i, yr in enumerate(sorted(y for y in years5 if y not in (TODAY_YEAR, LAST_YEAR))):
    sub  = df1_g[df1_g["year"] == yr].set_index("month")
    vals = [sub["ton"].get(m) for m in MONTHS]
    fig1.add_trace(go.Scatter(
        x=MONTH_LABELS, y=vals, name=str(yr),
        mode="lines+markers",
        line=dict(color=GRAY_SHADES[i % 3], width=1.5),
        marker=dict(size=4), opacity=0.7,
    ))

sub  = df1_g[df1_g["year"] == LAST_YEAR].set_index("month")
vals = [sub["ton"].get(m) for m in MONTHS]
fig1.add_trace(go.Scatter(
    x=MONTH_LABELS, y=vals, name=str(LAST_YEAR),
    mode="lines+markers",
    line=dict(color=COLOR_LAST, width=2.5), marker=dict(size=5),
))

sub  = df1_g[df1_g["year"] == TODAY_YEAR].set_index("month")
vals = [sub["ton"].get(m) for m in MONTHS]
text = [f"{v:,.1f}t" if pd.notna(v) and v is not None else "" for v in vals]
fig1.add_trace(go.Scatter(
    x=MONTH_LABELS, y=vals, name=str(TODAY_YEAR),
    mode="lines+markers+text",
    line=dict(color=COLOR_THIS, width=3), marker=dict(size=6, color=COLOR_THIS),
    text=text, textposition="top center", textfont=dict(size=10, color=COLOR_THIS),
))

if pd.notna(avg_val):
    fig1.add_hline(
        y=avg_val, line_dash="dot", line_color=COLOR_AVG, line_width=1.5,
        annotation_text=f"5년평균 {avg_val:,.1f}t",
        annotation_position="bottom right",
        annotation_font_size=10, annotation_font_color=COLOR_AVG,
    )

fig1.update_layout(
    **LAYOUT_BASE,
    xaxis=dict(tickmode="array", tickvals=MONTH_LABELS, ticktext=MONTH_LABELS,
               title="월", fixedrange=True),
    yaxis=dict(title="검역량 (톤)", fixedrange=True),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    height=430,
)
st.plotly_chart(fig1, use_container_width=True, config=CFG)

with st.expander(f"🔍 원본 데이터 확인 ({mask1.sum()}행)", expanded=False):
    st.dataframe(
        df_all[mask1].sort_values(["year", "month"])
        [["species", "year", "month", "country", "품명", "ton"]],
        use_container_width=True, height=250,
    )


# ══════════════════════════════════════════════════════════════════════════
# ② 추이 그래프
# ══════════════════════════════════════════════════════════════════════════

section("② 추이 그래프  (최근 3개월 / 1년 / 5년)")

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
sp2    = c1.selectbox("구분", SPECIES_OPTS, key="sp2")
df2_sp = df_all[df_all["species"] == sp2]
cntry2 = c2.selectbox("국가", sorted(df2_sp["country"].dropna().unique()), key="cntry2")
df2_c  = df2_sp[df2_sp["country"] == cntry2]
item2  = c3.selectbox("품목", sorted(df2_c["품명"].dropna().unique()), key="item2")
period2 = c4.radio("기간", ["최근 3개월", "최근 1년", "최근 5년"], key="period2")

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
    last_p  = df2_g["period"].max()
    months  = {"최근 3개월": 3, "최근 1년": 12, "최근 5년": 60}[period2]
    df2_g   = df2_g[df2_g["period"] >= last_p - pd.DateOffset(months=months)]

df2_g = df2_g[df2_g["ton"].notna()]
df2_g["label"] = df2_g["period"].dt.strftime("%y.%m")

if df2_g.empty:
    st.info("해당 조건의 데이터가 없습니다.")
else:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df2_g["label"], y=df2_g["ton"],
        mode="lines+markers",
        line=dict(color=COLOR_LAST, width=2.5), marker=dict(size=5),
        fill="tozeroy", fillcolor="rgba(49,130,206,0.08)",
        name="검역량",
        hovertemplate="%{x}  %{y:,.1f}t<extra></extra>",
    ))
    labels = df2_g["label"].tolist()
    fig2.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(
            tickmode="array", tickvals=labels, ticktext=labels,
            tickangle=-45, title="날짜 (YY.MM)", fixedrange=True,
        ),
        yaxis=dict(title="검역량 (톤)", fixedrange=True),
        margin=dict(l=0, r=10, t=30, b=70),
        height=380,
    )
    st.plotly_chart(fig2, use_container_width=True, config=CFG)


# ══════════════════════════════════════════════════════════════════════════
# ③ 비교 그래프
# ══════════════════════════════════════════════════════════════════════════

section("③ 비교 그래프  (국가·품목 복수 선택 → 월별 비교)")

c1, c2, c3, c4 = st.columns([1, 1, 2, 2])
sp3_opts = ["전체"] + SPECIES_OPTS
sp3      = c1.selectbox("구분", sp3_opts, key="sp3")
df3_sp   = df_all if sp3 == "전체" else df_all[df_all["species"] == sp3]

all_yrs = sorted(df3_sp["year"].dropna().unique(), reverse=True)
year3   = c2.selectbox("년도", all_yrs, key="year3")
df3_yr  = df3_sp[df3_sp["year"] == year3]

cnt3_all   = sorted(df3_yr["country"].dropna().unique())
items3_all = sorted(df3_yr["품명"].dropna().unique())

countries3 = c3.multiselect(
    "국가 (1개 이상)", cnt3_all,
    default=cnt3_all[:2] if len(cnt3_all) >= 2 else cnt3_all,
    key="cnt3",
)
items3 = c4.multiselect(
    "품목 (1개 이상)", items3_all,
    default=items3_all[:2] if len(items3_all) >= 2 else items3_all,
    key="items3",
)

if not countries3 or not items3:
    st.info("국가와 품목을 각각 1개 이상 선택하세요.")
else:
    mask3 = (
        (df_all["year"] == year3) &
        (df_all["country"].isin(countries3)) &
        (df_all["품명"].isin(items3))
    )
    if sp3 != "전체":
        mask3 &= (df_all["species"] == sp3)

    df3_g = (
        df_all[mask3]
        .groupby(["month", "country", "품명"])["ton"]
        .sum(min_count=1)
        .reset_index()
    )

    fig3  = go.Figure()
    combos = [(c, it) for c in countries3 for it in items3]

    for i, (c, it) in enumerate(combos):
        sub = df3_g[(df3_g["country"] == c) & (df3_g["품명"] == it)].sort_values("month")
        sub = sub[sub["ton"].notna()]
        if sub.empty:
            continue
        fig3.add_trace(go.Scatter(
            x=[f"{m:02d}" for m in sub["month"]], y=sub["ton"],
            name=f"{c} / {it}",
            mode="lines+markers",
            line=dict(color=PALETTE[i % len(PALETTE)], width=2.2),
            marker=dict(size=5),
            hovertemplate=f"{c} / {it}<br>%{{x}}월: %{{y:,.1f}}t<extra></extra>",
        ))

    if not fig3.data:
        st.info("해당 조건의 데이터가 없습니다.")
    else:
        fig3.update_layout(
            **LAYOUT_BASE,
            xaxis=dict(tickmode="array", tickvals=MONTH_LABELS, ticktext=MONTH_LABELS,
                       title="월", fixedrange=True),
            yaxis=dict(title="검역량 (톤)", fixedrange=True),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(l=0, r=10, t=60, b=30),
            height=430,
        )
        st.plotly_chart(fig3, use_container_width=True, config=CFG)
