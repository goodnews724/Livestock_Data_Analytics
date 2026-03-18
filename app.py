"""검역량 데이터 시각화
================================
데이터 소스: Google Sheets 검역량_raw
  SHEET_ID  : 13m-3z2LgX4BQ7JMT0dgOmKCHt_VXx71rFoSzBcMLFjE
  시트명    : 검역량_raw
  컬럼      : date(YYYY-MM), source, product_category, item_name,
              storage_type, country, month_cumulative_kg
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
SHEET_ID      = "13m-3z2LgX4BQ7JMT0dgOmKCHt_VXx71rFoSzBcMLFjE"
SHEET_TITLE   = "검역량_raw"
LOCAL_SA_PATH = Path("/opt/secrets/service_account.json")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

TODAY_YEAR  = date.today().year
LAST_YEAR   = TODAY_YEAR - 1

COLOR_THIS  = "#e53e3e"   # 올해 — 빨간색
COLOR_LAST  = "#3182ce"   # 작년 — 파란색
GRAY_SHADES = ["#aaaaaa", "#c4c4c4", "#dedede"]  # 나머지 연도
COLOR_AVG   = "#718096"   # 평균 점선

PALETTE = [
    "#e53e3e", "#3182ce", "#38a169", "#d69e2e", "#805ad5",
    "#dd6b20", "#319795", "#b83280", "#2b6cb0", "#276749",
]

MONTHS       = list(range(1, 13))
MONTH_LABELS = [f"{m:02d}" for m in MONTHS]

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
div[data-testid="stTextInput"] input { font-size: 13px !important; }
</style>
""", unsafe_allow_html=True)


# ── 인증 & 데이터 로드 ───────────────────────────────────────────────────

@st.cache_resource
def _get_client() -> gspread.Client:
    if LOCAL_SA_PATH.exists():
        creds = Credentials.from_service_account_file(str(LOCAL_SA_PATH), scopes=SCOPES)
    else:
        info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_data(ttl=300, show_spinner=False)
def load_raw() -> pd.DataFrame:
    gc   = _get_client()
    ws   = gc.open_by_key(SHEET_ID).worksheet(SHEET_TITLE)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame()

    headers = [h.strip() for h in rows[0]]
    df = pd.DataFrame(rows[1:], columns=headers)

    df["date"] = df["date"].str.strip()
    df = df[df["date"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()
    df["year"]   = df["date"].str[:4].astype(int)
    df["month"]  = df["date"].str[5:7].astype(int)
    df["period"] = pd.to_datetime(df["date"] + "-01")
    # NaN 유지 — fillna(0) 하지 않음 (데이터 없는 월은 gap으로 표시)
    df["ton"] = pd.to_numeric(df["month_cumulative_kg"], errors="coerce") / 1_000

    for col in ["product_category", "item_name", "country", "storage_type"]:
        if col in df.columns:
            df[col] = df[col].str.strip()
            df[col] = df[col].replace("", pd.NA)

    return df


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
        load_raw.clear()
        st.rerun()

with st.spinner("데이터 불러오는 중..."):
    try:
        df_all   = load_raw()
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

all_cats = sorted(df_all["product_category"].dropna().unique())

# 데이터에 있는 구분 값 안내
with st.expander("📋 데이터 구분(product_category) 실제 값 확인", expanded=False):
    st.dataframe(
        df_all.groupby("product_category")["ton"].agg(["count", "sum"])
        .rename(columns={"count": "행 수", "sum": "합계(톤)"})
        .sort_values("행 수", ascending=False),
        use_container_width=True,
    )


# ══════════════════════════════════════════════════════════════════════════
# ① 연도별 월별 비교 (최근 5년)
# ══════════════════════════════════════════════════════════════════════════

section("① 연도별 월별 비교  (최근 5년 · 올해=빨강, 작년=파랑, 나머지=회색)")

c1, c2, c3 = st.columns(3)
cat1     = c1.selectbox("구분", all_cats, key="cat1")
df1_cat  = df_all[df_all["product_category"] == cat1]
cntry1   = c2.selectbox("국가", sorted(df1_cat["country"].dropna().unique()), key="cntry1")
df1_c    = df1_cat[df1_cat["country"] == cntry1]
item1    = c3.selectbox("품목", sorted(df1_c["item_name"].dropna().unique()), key="item1")

years5  = list(range(TODAY_YEAR - 4, TODAY_YEAR + 1))
mask1   = (
    (df_all["product_category"] == cat1) &
    (df_all["country"] == cntry1) &
    (df_all["item_name"] == item1) &
    (df_all["year"].isin(years5))
)
df1_raw = df_all[mask1]
df1_g   = (
    df1_raw
    .groupby(["year", "month"])["ton"]
    .sum(min_count=1)   # 모든 값이 NaN이면 0이 아닌 NaN 반환
    .reset_index()
)

avg_val = df1_g["ton"].mean(skipna=True) if not df1_g.empty else 0

# 데이터 확인용
with st.expander(f"🔍 원본 데이터 확인 ({len(df1_raw)}행)", expanded=False):
    st.dataframe(
        df1_raw[["date", "source", "product_category", "item_name", "storage_type", "country", "month_cumulative_kg", "ton"]]
        .sort_values("date"),
        use_container_width=True,
        height=250,
    )

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
text = [f"{v:,.0f}t" if v is not None else "" for v in vals]
fig1.add_trace(go.Scatter(
    x=MONTH_LABELS, y=vals, name=str(TODAY_YEAR),
    mode="lines+markers+text",
    line=dict(color=COLOR_THIS, width=3), marker=dict(size=6, color=COLOR_THIS),
    text=text, textposition="top center", textfont=dict(size=10, color=COLOR_THIS),
))

fig1.add_hline(
    y=avg_val, line_dash="dot", line_color=COLOR_AVG, line_width=1.5,
    annotation_text=f"5년평균 {avg_val:,.0f}t",
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


# ══════════════════════════════════════════════════════════════════════════
# ② 추이 그래프 (최근 3개월 / 1년 / 5년)
# ══════════════════════════════════════════════════════════════════════════

section("② 추이 그래프  (최근 3개월 / 1년 / 5년)")

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
cat2    = c1.selectbox("구분", all_cats, key="cat2")
df2_cat = df_all[df_all["product_category"] == cat2]
cntry2  = c2.selectbox("국가", sorted(df2_cat["country"].dropna().unique()), key="cntry2")
df2_c   = df2_cat[df2_cat["country"] == cntry2]
item2   = c3.selectbox("품목", sorted(df2_c["item_name"].dropna().unique()), key="item2")
period2 = c4.radio("기간", ["최근 3개월", "최근 1년", "최근 5년"], key="period2")

mask2 = (
    (df_all["product_category"] == cat2) &
    (df_all["country"] == cntry2) &
    (df_all["item_name"] == item2)
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
        hovertemplate="%{x}  %{y:,.0f}t<extra></extra>",
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
cat3_opts = ["전체"] + all_cats
cat3      = c1.selectbox("구분", cat3_opts, key="cat3")
df3_cat   = df_all if cat3 == "전체" else df_all[df_all["product_category"] == cat3]

all_yrs = sorted(df3_cat["year"].dropna().unique(), reverse=True)
year3   = c2.selectbox("년도", all_yrs, key="year3")
df3_yr  = df3_cat[df3_cat["year"] == year3]

cnt3_all   = sorted(df3_yr["country"].dropna().unique())
items3_all = sorted(df3_yr["item_name"].dropna().unique())

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
        (df_all["item_name"].isin(items3))
    )
    if cat3 != "전체":
        mask3 &= (df_all["product_category"] == cat3)
    df3   = df_all[mask3]
    df3_g = df3.groupby(["month", "country", "item_name"])["ton"].sum(min_count=1).reset_index()

    fig3  = go.Figure()
    combos = [(c, it) for c in countries3 for it in items3]

    for i, (c, it) in enumerate(combos):
        sub = df3_g[(df3_g["country"] == c) & (df3_g["item_name"] == it)].sort_values("month")
        if sub.empty:
            continue
        fig3.add_trace(go.Scatter(
            x=[f"{m:02d}" for m in sub["month"]], y=sub["ton"],
            name=f"{c} / {it}",
            mode="lines+markers",
            line=dict(color=PALETTE[i % len(PALETTE)], width=2.2),
            marker=dict(size=5),
            hovertemplate=f"{c} / {it}<br>%{{x}}월: %{{y:,.0f}}t<extra></extra>",
        ))

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
