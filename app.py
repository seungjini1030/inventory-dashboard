import re
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

st.set_page_config(page_title="부진/부동 재고 대시보드", layout="wide")

# -----------------------------
# 기본 설정
# -----------------------------
SHEET_NAME = "전체"
HEADER_ROW = 3  # 실제 헤더 시작 행(1-indexed)
SPREADSHEET_ID = st.secrets["SPREADSHEET_ID"]

st.title("부진/부동 재고 대시보드")


# -----------------------------
# 유틸 함수
# -----------------------------
def clean_header(text: str) -> str:
    """줄바꿈/복수공백 제거해서 헤더를 한 줄 텍스트로 정리"""
    if text is None:
        return ""
    text = str(text).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def week_num(col) -> int:
    m = re.match(r"^(\d{1,2})W$", str(col).strip())
    return int(m.group(1)) if m else 999


def to_num_series(s: pd.Series) -> pd.Series:
    """문자/콤마/% 등이 섞인 값을 숫자로 안전 변환 (Series 단위)"""
    s = s.astype(str).str.strip()
    s = s.str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    s = s.str.replace(r"[^0-9.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def fmt_int(x):
    return f"{int(round(float(x))):,}"


def fmt_pct(x, digits=1):
    return f"{float(x):.{digits}f}%"


# ============================================================
# [수정 1] gspread 인증 방식 업데이트
#   - gspread.authorize(creds) → gspread.Client(auth=creds)
#   - gspread 5.x/6.x 모두 호환
# ============================================================
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    return gspread.Client(auth=creds)


# -----------------------------
# 구글시트 로드 (데이터 캐시)
# -----------------------------
@st.cache_data(ttl=300)
def load_data():
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_NAME)
    values = ws.get_all_values()

    if len(values) < HEADER_ROW:
        return pd.DataFrame()

    raw_header = values[HEADER_ROW - 1]
    header = [clean_header(h) for h in raw_header]
    rows = values[HEADER_ROW:]
    df = pd.DataFrame(rows, columns=header)

    # 완전 빈 행 제거(공백 포함)
    df = df.loc[df.astype(str).apply(lambda r: "".join(r).strip() != "", axis=1)].copy()
    return df


df = load_data()

if df.empty:
    st.error("시트 데이터를 불러오지 못했습니다.")
    st.stop()

# -----------------------------
# 헤더 매핑
# -----------------------------
required_cols = {
    "dept": "소진 주관 부서",
    "sku": "상품 코드",
    "name": "상품명",
    "base": "1/1 기준재고",
    "avail": "가용재고",
    "rate": "소진율",
}

missing = [v for v in required_cols.values() if v not in df.columns]
if missing:
    st.error("필수 컬럼을 찾지 못했습니다.")
    for v in required_cols.values():
        st.write(f"- {v}: {'✅ 있음' if v in df.columns else '❌ 없음'}")
    st.write("현재 읽힌 컬럼명:", list(df.columns))
    st.stop()

COL_DEPT = required_cols["dept"]
COL_SKU = required_cols["sku"]
COL_NAME = required_cols["name"]
COL_BASE = required_cols["base"]
COL_AVAIL = required_cols["avail"]
COL_RATE = required_cols["rate"]

# -----------------------------
# 텍스트 컬럼 정리(탭/필터 안정화)
# -----------------------------
df[COL_DEPT] = df[COL_DEPT].astype(str).str.strip().replace({"": "(미기재)"}).fillna("(미기재)")
df[COL_SKU] = df[COL_SKU].astype(str).str.strip()
df[COL_NAME] = df[COL_NAME].astype(str).str.strip()

# -----------------------------
# 주차 컬럼 탐색 (1W~13W 등)
# -----------------------------
week_cols = [c for c in df.columns if re.match(r"^\d{1,2}W$", str(c).strip())]
week_cols = sorted(week_cols, key=week_num)

# -----------------------------
# 데이터 타입 정리 (벡터화)
# -----------------------------
df[COL_BASE] = to_num_series(df[COL_BASE])
df[COL_AVAIL] = to_num_series(df[COL_AVAIL])
df[COL_RATE] = to_num_series(df[COL_RATE])  # 시트 값(참고용)

if week_cols:
    df[week_cols] = df[week_cols].apply(to_num_series, axis=0)

# 계산 컬럼
df["소진수량"] = (df[COL_BASE] - df[COL_AVAIL]).clip(lower=0)
base_nonzero = df[COL_BASE].replace(0, pd.NA)
df["소진율(계산)"] = (df["소진수량"] / base_nonzero).fillna(0) * 100


# ============================================================
# [수정 2] 월-주차 매핑 동적 처리
#   - 하드코딩 제거, 주차 번호 기반 자동 매핑
#   - 4월 이후 데이터 추가 시에도 자동 반영
# ============================================================
def build_month_map(week_cols_list: list) -> dict:
    """
    주차 번호를 기반으로 월 매핑 자동 생성
    1~4W=1월, 5~8W=2월, 9~13W=3월, 14~17W=4월 ...
    (대략 4주=1개월 기준, 실무 관례에 맞춤)
    """
    week_to_month = {
        1: "1월", 2: "1월", 3: "1월", 4: "1월", 5: "1월",
        6: "2월", 7: "2월", 8: "2월", 9: "2월",
        10: "3월", 11: "3월", 12: "3월", 13: "3월",
        14: "4월", 15: "4월", 16: "4월", 17: "4월",
        18: "5월", 19: "5월", 20: "5월", 21: "5월", 22: "5월",
        23: "6월", 24: "6월", 25: "6월", 26: "6월",
        27: "7월", 28: "7월", 29: "7월", 30: "7월",
        31: "8월", 32: "8월", 33: "8월", 34: "8월", 35: "8월",
        36: "9월", 37: "9월", 38: "9월", 39: "9월",
        40: "10월", 41: "10월", 42: "10월", 43: "10월",
        44: "11월", 45: "11월", 46: "11월", 47: "11월",
        48: "12월", 49: "12월", 50: "12월", 51: "12월", 52: "12월",
    }
    month_map = {}
    for col in week_cols_list:
        wn = week_num(col)
        month = week_to_month.get(wn, f"{(wn - 1) // 4 + 1}월")
        month_map.setdefault(month, []).append(wn)
    return month_map


def month_rate_for_dept(ddf: pd.DataFrame, month_map: dict, week_cols_all: list) -> pd.DataFrame:
    dept_base = float(ddf[COL_BASE].sum())
    recs = []
    for month, weeks in month_map.items():
        cols = [c for c in week_cols_all if week_num(c) in weeks]
        month_cons = float(ddf[cols].sum().sum()) if cols else 0.0
        month_rate = (month_cons / dept_base * 100) if dept_base > 0 else 0.0
        recs.append({"월": month, "소진율(가중)": month_rate})
    out = pd.DataFrame(recs)
    month_order = [f"{i}월" for i in range(1, 13)]
    out["월"] = pd.Categorical(out["월"], categories=month_order, ordered=True)
    return out.sort_values("월")


# 한 번만 생성 (중복 호출 제거)
month_map = build_month_map(week_cols) if week_cols else {}


# -----------------------------
# 사이드바 필터
# -----------------------------
st.sidebar.header("필터")
search = st.sidebar.text_input("상품코드/상품명 검색", "")
dept_options = ["전체"] + sorted(df[COL_DEPT].dropna().astype(str).unique().tolist())
selected_dept = st.sidebar.selectbox("소진 주관 부서", dept_options, index=0)

f = df.copy()
if selected_dept != "전체":
    f = f[f[COL_DEPT].astype(str) == selected_dept]
if search.strip():
    s = search.strip().lower()
    f = f[
        f[COL_SKU].astype(str).str.lower().str.contains(s, na=False)
        | f[COL_NAME].astype(str).str.lower().str.contains(s, na=False)
    ]

# -----------------------------
# KPI
# -----------------------------
sku_cnt = int(f[COL_SKU].nunique())
base_sum = float(f[COL_BASE].sum())
avail_sum = float(f[COL_AVAIL].sum())
cons_sum = float(f["소진수량"].sum())
weighted_rate = (cons_sum / base_sum * 100) if base_sum > 0 else 0

k1, k2, k3, k4, k5 = st.columns([1, 1.2, 1.2, 1.2, 1.2])
k1.metric("SKU 수", f"{sku_cnt:,}")
k2.metric("1/1 기준재고 합", fmt_int(base_sum))
k3.metric("가용재고 합", fmt_int(avail_sum))
k4.metric("소진수량 합", fmt_int(cons_sum))
k5.markdown(
    f"""
    <div style="text-align:center;">
      <div style="font-size:0.95rem; color:#666; margin-bottom:0.35rem;">소진율(가중)</div>
      <div style="font-size:2.2rem; font-weight:800; color:#D80000; line-height:1.1;">
        {fmt_pct(weighted_rate, 2)}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.caption("참고: 소진율(가중) = 소진수량합 / 1/1기준재고합")

# -----------------------------
# 1W~13W 소진수량 추이 (벡터화)
# -----------------------------
if week_cols:
    st.divider()
    st.subheader("1W~13W 소진수량 추이")

    wk_sum = f[week_cols].sum(axis=0).astype(float)
    wk_df = pd.DataFrame({"주차": wk_sum.index, "소진수량": wk_sum.values})
    wk_df["주차번호"] = wk_df["주차"].apply(week_num)
    wk_df = wk_df.sort_values("주차번호")

    fig_w = px.line(wk_df, x="주차", y="소진수량", markers=True)
    fig_w.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig_w.update_xaxes(categoryorder="array", categoryarray=wk_df["주차"].tolist())
    st.plotly_chart(fig_w, use_container_width=True, config={"displayModeBar": False})

# -----------------------------
# 월별 소진 현황(부서별) - 소진율(가중)
# -----------------------------
if week_cols and month_map:
    st.divider()
    st.subheader("월별 소진 현황 (부서별)")

    recs = []
    for dept, ddf in f.groupby(COL_DEPT, dropna=False):
        dept = str(dept).strip() if str(dept).strip() else "(미기재)"
        mdf_dept = month_rate_for_dept(ddf, month_map, week_cols)
        mdf_dept["소진 주관 부서"] = dept
        recs.append(mdf_dept)

    mdf = pd.concat(recs, ignore_index=True) if recs else pd.DataFrame()

    if not mdf.empty:
        fig_m = px.line(
            mdf,
            x="월",
            y="소진율(가중)",
            color="소진 주관 부서",
            markers=True,
        )
        fig_m.update_yaxes(ticksuffix="%")
        fig_m.update_traces(
            hovertemplate=(
                "소진 주관 부서=%{legendgroup}<br>"
                "월=%{x}<br>"
                "<b><span style='color:#D80000'>소진율(가중)=%{y:.1f}%</span></b>"
                "<extra></extra>"
            )
        )
        fig_m.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_m, use_container_width=True, config={"displayModeBar": False})

# -----------------------------
# 부서(국가)별 요약
# -----------------------------
st.divider()
st.subheader("부서(국가)별 요약")

grp = (
    f.groupby(COL_DEPT, dropna=False)
    .agg(
        SKU=(COL_SKU, "nunique"),
        기준재고합=(COL_BASE, "sum"),
        가용재고합=(COL_AVAIL, "sum"),
        소진수량합=("소진수량", "sum"),
    )
    .reset_index()
)
grp[COL_DEPT] = grp[COL_DEPT].astype(str).str.strip().replace({"": "(미기재)"}).fillna("(미기재)")
grp["소진율(가중)"] = grp.apply(
    lambda r: (r["소진수량합"] / r["기준재고합"] * 100) if r["기준재고합"] > 0 else 0,
    axis=1,
)

grp_view = grp.copy()
grp_view["SKU"] = grp_view["SKU"].map(lambda x: f"{int(x):,}")
grp_view["기준재고합"] = grp_view["기준재고합"].map(fmt_int)
grp_view["가용재고합"] = grp_view["가용재고합"].map(fmt_int)
grp_view["소진수량합"] = grp_view["소진수량합"].map(fmt_int)
grp_view["소진율(가중)"] = grp["소진율(가중)"].map(lambda x: fmt_pct(x, 1))

st.dataframe(grp_view, use_container_width=True, hide_index=True)

# -----------------------------
# 상세 영역: 부서별 탭 + (월별 소진율 그래프) + (상/하위 5품목)
# -----------------------------
st.divider()
st.subheader("상세 분석 (부서별)")

dept_list = sorted(f[COL_DEPT].dropna().astype(str).unique().tolist())
if not dept_list:
    st.info("표시할 데이터가 없습니다.")
    st.stop()

# 선택 부서가 '전체'가 아니면 탭도 해당 부서만
if selected_dept != "전체":
    dept_list = [selected_dept]

tabs = st.tabs([f"{d}" for d in dept_list])

for tab, dept in zip(tabs, dept_list):
    with tab:
        ddf = f[f[COL_DEPT].astype(str) == dept].copy()

        # (1) 월별 소진율(가중) 그래프
        if week_cols and month_map:
            mdf_dept = month_rate_for_dept(ddf, month_map, week_cols)
            fig_dept_m = px.line(
                mdf_dept,
                x="월",
                y="소진율(가중)",
                markers=True,
                title=f"{dept} 월별 소진율(가중)",
            )
            fig_dept_m.update_yaxes(ticksuffix="%")
            fig_dept_m.update_layout(margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(fig_dept_m, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("주차(1W~) 컬럼이 없어 월별 그래프를 표시할 수 없습니다.")

        st.markdown("#### 소진율 상/하위 5 품목")

        # base=0 제외(소진율 왜곡 방지)
        items = ddf[ddf[COL_BASE] > 0].copy()
        show_cols = [COL_SKU, COL_NAME, COL_BASE, COL_AVAIL, "소진수량", "소진율(계산)"]

        left, right = st.columns(2)

        with left:
            st.markdown("**상위 5 (잘 팔린 품목)**")
            top5 = (
                items.sort_values("소진율(계산)", ascending=False)
                .head(5)[show_cols]
                .copy()
            )
            if top5.empty:
                st.write("- 데이터 없음")
            else:
                top5[COL_BASE] = top5[COL_BASE].map(fmt_int)
                top5[COL_AVAIL] = top5[COL_AVAIL].map(fmt_int)
                top5["소진수량"] = top5["소진수량"].map(fmt_int)
                top5["소진율(계산)"] = top5["소진율(계산)"].map(lambda x: fmt_pct(x, 1))
                st.dataframe(top5, use_container_width=True, hide_index=True)

        with right:
            st.markdown("**하위 5 (푸시 판매 후보)**")
            bottom_pool = items[items[COL_AVAIL] > 0].copy()
            bottom5 = (
                bottom_pool.sort_values("소진율(계산)", ascending=True)
                .head(5)[show_cols]
                .copy()
            )
            if bottom5.empty:
                st.write("- 데이터 없음")
            else:
                bottom5[COL_BASE] = bottom5[COL_BASE].map(fmt_int)
                bottom5[COL_AVAIL] = bottom5[COL_AVAIL].map(fmt_int)
                bottom5["소진수량"] = bottom5["소진수량"].map(fmt_int)
                bottom5["소진율(계산)"] = bottom5["소진율(계산)"].map(lambda x: fmt_pct(x, 1))
                st.dataframe(bottom5, use_container_width=True, hide_index=True)

        # (옵션) 부서 상세 원본 테이블
        with st.expander("부서 상세 데이터 전체 보기"):
            detail_cols = [COL_DEPT, COL_SKU, COL_NAME, COL_BASE, COL_AVAIL, "소진수량", COL_RATE, "소진율(계산)"]
            detail = ddf[detail_cols].copy()
            detail[COL_BASE] = detail[COL_BASE].map(fmt_int)
            detail[COL_AVAIL] = detail[COL_AVAIL].map(fmt_int)
            detail["소진수량"] = detail["소진수량"].map(fmt_int)
            detail[COL_RATE] = detail[COL_RATE].map(lambda x: fmt_pct(x, 1))
            detail["소진율(계산)"] = detail["소진율(계산)"].map(lambda x: fmt_pct(x, 1))
            st.dataframe(detail, use_container_width=True, hide_index=True)
