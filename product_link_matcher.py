"""Product Link Matcher Pro (Catalog-first, 2026 format).

핵심 전략:
- 실무 정확도를 위해 '검색 크롤링'보다 '플랫폼 카탈로그 소스 업로드 + 유사도 매칭'을 기본으로 사용
- 무신사/29CM 링크, 가격, 재고를 wide 테이블로 출력

실행:
    streamlit run product_link_matcher.py
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import re
from typing import Iterable

import pandas as pd
import streamlit as st


NAME_ALIASES = ["name", "product_name", "상품명", "title", "product"]
URL_ALIASES = ["url", "product_url", "link", "상품url", "상품링크"]
PRICE_ALIASES = ["price", "current_price", "판매가", "가격"]
STOCK_ALIASES = ["stock", "stock_status", "재고", "품절여부"]
INPUT_ALIASES = ["name", "product_name", "상품명", "full_name", "input_name"]


@dataclass
class CatalogSchema:
    name_col: str
    url_col: str | None
    price_col: str | None
    stock_col: str | None


@dataclass
class MatchRow:
    input_name: str
    normalized_name: str
    cm29_name: str
    cm29_url: str
    cm29_price: str
    cm29_stock: str
    cm29_score: float
    musinsa_name: str
    musinsa_url: str
    musinsa_price: str
    musinsa_stock: str
    musinsa_score: float
    match_status: str


def normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\(w\)|\(m\)|\[w\]|\[m\]", " ", text)
    text = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def token_sort_key(value: str) -> str:
    tokens = normalize_text(value).split()
    return " ".join(sorted(tokens))


def similarity_score(a: str, b: str) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    if not a_norm or not b_norm:
        return 0.0

    seq = SequenceMatcher(None, a_norm, b_norm).ratio()

    a_tokens = set(a_norm.split())
    b_tokens = set(b_norm.split())
    inter = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens) or 1
    jaccard = inter / union

    a_sort = token_sort_key(a_norm)
    b_sort = token_sort_key(b_norm)
    token_seq = SequenceMatcher(None, a_sort, b_sort).ratio()

    score = (seq * 0.45) + (jaccard * 0.25) + (token_seq * 0.30)
    return round(score, 4)


def detect_column(df: pd.DataFrame, aliases: list[str], required: bool = False) -> str | None:
    mapping = {c.lower().strip(): c for c in df.columns}
    for alias in aliases:
        if alias in mapping:
            return mapping[alias]
    if required:
        raise ValueError(f"필수 컬럼 누락: {aliases}")
    return None


def infer_catalog_schema(df: pd.DataFrame) -> CatalogSchema:
    return CatalogSchema(
        name_col=detect_column(df, NAME_ALIASES, required=True),
        url_col=detect_column(df, URL_ALIASES),
        price_col=detect_column(df, PRICE_ALIASES),
        stock_col=detect_column(df, STOCK_ALIASES),
    )


def infer_input_names(df: pd.DataFrame) -> list[str]:
    col = detect_column(df, INPUT_ALIASES, required=True)
    return [str(v).strip() for v in df[col].fillna("").tolist() if str(v).strip()]


def preprocess_catalog(df: pd.DataFrame, schema: CatalogSchema) -> pd.DataFrame:
    out = df.copy()
    out["_name"] = out[schema.name_col].astype(str).fillna("").str.strip()
    out["_norm"] = out["_name"].map(normalize_text)
    out = out[out["_norm"].str.len() > 0].drop_duplicates(subset=["_norm"])
    return out


def find_best_match(name: str, catalog: pd.DataFrame, schema: CatalogSchema, threshold: float) -> dict:
    if catalog.empty:
        return {}

    norm = normalize_text(name)

    # 1차 후보 필터: 앞 토큰 일부 공유
    tokens = set(norm.split())
    if tokens:
        candidate = catalog[catalog["_norm"].apply(lambda x: len(tokens & set(x.split())) > 0)]
        if candidate.empty:
            candidate = catalog
    else:
        candidate = catalog

    best_score = -1.0
    best_row = None

    for _, row in candidate.iterrows():
        score = similarity_score(norm, row["_norm"])
        if score > best_score:
            best_score = score
            best_row = row

    if best_row is None or best_score < threshold:
        return {}

    return {
        "name": str(best_row.get("_name", "")),
        "url": str(best_row.get(schema.url_col, "")) if schema.url_col else "",
        "price": str(best_row.get(schema.price_col, "")) if schema.price_col else "",
        "stock": str(best_row.get(schema.stock_col, "")) if schema.stock_col else "",
        "score": round(best_score, 4),
    }


def match_products(
    input_names: Iterable[str],
    cm29_catalog: pd.DataFrame,
    cm29_schema: CatalogSchema,
    musinsa_catalog: pd.DataFrame,
    musinsa_schema: CatalogSchema,
    threshold: float,
) -> list[MatchRow]:
    rows: list[MatchRow] = []

    for name in input_names:
        cm29 = find_best_match(name, cm29_catalog, cm29_schema, threshold)
        musinsa = find_best_match(name, musinsa_catalog, musinsa_schema, threshold)

        if cm29 and musinsa:
            status = "both_matched"
        elif cm29 or musinsa:
            status = "partial_matched"
        else:
            status = "not_matched"

        rows.append(
            MatchRow(
                input_name=name,
                normalized_name=normalize_text(name),
                cm29_name=cm29.get("name", ""),
                cm29_url=cm29.get("url", ""),
                cm29_price=cm29.get("price", ""),
                cm29_stock=cm29.get("stock", ""),
                cm29_score=float(cm29.get("score", 0.0)),
                musinsa_name=musinsa.get("name", ""),
                musinsa_url=musinsa.get("url", ""),
                musinsa_price=musinsa.get("price", ""),
                musinsa_stock=musinsa.get("stock", ""),
                musinsa_score=float(musinsa.get("score", 0.0)),
                match_status=status,
            )
        )

    return rows


def render_requirements_notice() -> None:
    st.warning(
        "정확한 링크/가격/재고를 원하면 반드시 플랫폼 소스 CSV가 필요합니다.\n"
        "필수: 상품명 컬럼\n"
        "강력권장: 상품URL, 가격, 재고 컬럼"
    )
    st.markdown(
        "**필요 데이터 요청(꼭 전달 부탁):**\n"
        "1) 29CM 전체 상품 CSV(상품명, URL, 가격, 재고)\n"
        "2) 무신사 전체 상품 CSV(상품명, URL, 가격, 재고)\n"
        "3) 입력 상품명 목록 CSV 또는 텍스트"
    )


def render_app() -> None:
    st.set_page_config(page_title="Product Link Matcher Pro", layout="wide", page_icon="🧠")
    st.title("🧠 Product Link Matcher Pro (Catalog-first)")
    st.caption("실패 많은 크롤링 방식 대신, 실제 운영 데이터 기반 고정밀 매칭 포맷")

    render_requirements_notice()

    with st.sidebar:
        st.subheader("매칭 설정")
        threshold = st.slider("최소 유사도 임계치", min_value=0.50, max_value=0.99, value=0.78, step=0.01)
        st.caption("낮추면 매칭수↑ / 오매칭 위험↑")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("### 1) 입력 상품 목록")
        input_text = st.text_area("한 줄에 1개", height=240)
        input_csv = st.file_uploader("입력 CSV", type=["csv"], key="input")

    with c2:
        st.markdown("### 2) 29CM 카탈로그")
        cm29_csv = st.file_uploader("29CM CSV", type=["csv"], key="29cm")

    with c3:
        st.markdown("### 3) 무신사 카탈로그")
        musinsa_csv = st.file_uploader("무신사 CSV", type=["csv"], key="musinsa")

    run = st.button("매칭 실행", type="primary", use_container_width=True)

    if not run:
        return

    names: list[str] = []
    if input_text.strip():
        names.extend([x.strip() for x in input_text.splitlines() if x.strip()])

    if input_csv is not None:
        try:
            df_input = pd.read_csv(input_csv)
            names.extend(infer_input_names(df_input))
        except Exception as exc:
            st.error(f"입력 CSV 파싱 실패: {exc}")
            st.stop()

    names = list(dict.fromkeys(names))
    if not names:
        st.error("입력 상품명이 없습니다.")
        st.stop()

    if cm29_csv is None or musinsa_csv is None:
        st.error("29CM/무신사 카탈로그 CSV 둘 다 업로드해주세요.")
        st.stop()

    try:
        df_29 = pd.read_csv(cm29_csv)
        schema_29 = infer_catalog_schema(df_29)
        cat_29 = preprocess_catalog(df_29, schema_29)

        df_m = pd.read_csv(musinsa_csv)
        schema_m = infer_catalog_schema(df_m)
        cat_m = preprocess_catalog(df_m, schema_m)
    except Exception as exc:
        st.error(f"카탈로그 파싱 실패: {exc}")
        st.stop()

    with st.spinner(f"{len(names)}개 상품 매칭 중..."):
        rows = match_products(names, cat_29, schema_29, cat_m, schema_m, threshold)

    result_df = pd.DataFrame([r.__dict__ for r in rows])

    m1, m2, m3 = st.columns(3)
    m1.metric("입력 상품수", len(result_df))
    m2.metric("양플랫폼 매칭", int((result_df["match_status"] == "both_matched").sum()))
    m3.metric("부분/미매칭", int((result_df["match_status"] != "both_matched").sum()))

    ordered_cols = [
        "input_name",
        "cm29_name",
        "cm29_url",
        "cm29_price",
        "cm29_stock",
        "cm29_score",
        "musinsa_name",
        "musinsa_url",
        "musinsa_price",
        "musinsa_stock",
        "musinsa_score",
        "match_status",
        "normalized_name",
    ]
    result_df = result_df[ordered_cols]

    st.dataframe(result_df, use_container_width=True, height=600)

    st.download_button(
        "결과 CSV 다운로드",
        data=result_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="matched_products_catalog_first.csv",
        mime="text/csv",
        use_container_width=True,
    )


if __name__ == "__main__":
    render_app()
