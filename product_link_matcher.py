"""Product Link Matcher Pro (Streamlit).

실행:
    streamlit run product_link_matcher.py

목표:
- 한 줄에 하나씩 '상품명 전체'를 입력하면 브랜드/제품/컬러를 자동 추정
- 무신사/29CM 검색 및 상위 결과를 매칭
- 실무에서 필요한 핵심 데이터(실제 상품 링크/가격/재고상태/매칭신뢰도)를 표로 제공
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import quote_plus, urljoin
import re

import pandas as pd
import requests
import streamlit as st


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

DEFAULT_TIMEOUT = 9

# 실무용 최소 사전(필요 시 계속 확장)
KNOWN_COLORS = {
    "black",
    "white",
    "gray",
    "grey",
    "melange gray",
    "navy",
    "beige",
    "brown",
    "charcoal",
    "khaki",
    "green",
    "blue",
    "red",
    "pink",
    "ivory",
    "cream",
    "silver",
    "gold",
    "multi",
    "블랙",
    "화이트",
    "그레이",
    "멜란지 그레이",
    "네이비",
    "베이지",
    "브라운",
    "차콜",
    "카키",
    "그린",
    "블루",
    "레드",
    "핑크",
    "아이보리",
    "크림",
    "실버",
    "골드",
    "멀티",
}

GRAY_FAMILY = {"gray", "grey", "그레이", "회색", "charcoal", "차콜", "챠콜"}
GRAY_EXCLUSION_TERMS = ["-melange", "-mélange", "-멜란지", "-멜란", "-믈란지"]


@dataclass
class ParsedProduct:
    original_name: str
    brand: str
    product_name: str
    color: str
    normalized_query: str


@dataclass
class PlatformMatch:
    input_name: str
    platform: str
    brand: str
    product_name: str
    color: str
    query: str
    search_url: str
    product_url: str
    product_title: str
    current_price: str
    stock_status: str
    confidence: float
    matched_at_utc: str
    error: str


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def split_name_tokens(raw_name: str) -> list[str]:
    cleaned = re.sub(r"[|/,_]+", " ", raw_name)
    cleaned = normalize_space(cleaned)
    return cleaned.split(" ") if cleaned else []


def detect_color(raw_name: str, tokens: list[str]) -> str:
    lowered = raw_name.lower()

    # 길이가 긴 컬러명을 우선 매칭
    for color in sorted(KNOWN_COLORS, key=len, reverse=True):
        if color.lower() in lowered:
            return color

    # 못 찾으면 마지막 토큰 기반 추정
    if tokens:
        tail = tokens[-1].lower()
        if tail in {c.lower() for c in KNOWN_COLORS}:
            return tokens[-1]

    return ""


def parse_product_name(raw_name: str) -> ParsedProduct:
    original = normalize_space(raw_name)
    tokens = split_name_tokens(original)

    if not tokens:
        raise ValueError("빈 상품명은 처리할 수 없습니다.")

    brand = tokens[0]
    color = detect_color(original, tokens)

    # 제품명 = 전체 - brand - color(있을 때)
    product_tokens = tokens[1:] if len(tokens) > 1 else [tokens[0]]

    if color:
        color_tokens = [t.lower() for t in color.split(" ")]
        product_tail = [t.lower() for t in product_tokens[-len(color_tokens):]]
        if product_tail == color_tokens:
            product_tokens = product_tokens[:-len(color_tokens)]

    product_name = normalize_space(" ".join(product_tokens)) or original
    query = normalize_space(f"{brand} {product_name} {color}") if color else normalize_space(f"{brand} {product_name}")

    if color and color.lower() in GRAY_FAMILY and "melange" not in query.lower() and "멜란지" not in query:
        query = f"{query} {' '.join(GRAY_EXCLUSION_TERMS)}"

    return ParsedProduct(
        original_name=original,
        brand=brand,
        product_name=product_name,
        color=color,
        normalized_query=query,
    )


def _extract_price_text(text: str) -> str:
    match = re.search(r"([₩￦]\s?[\d,]+|\d{1,3}(?:,\d{3})+\s?원)", text)
    return match.group(1) if match else ""


def _infer_stock_status(text: str) -> str:
    lowered = text.lower()
    soldout_keywords = ["sold out", "품절", "일시품절", "out of stock"]
    if any(k in lowered for k in soldout_keywords):
        return "품절"
    return "판매중(추정)"


def _score_match(query: str, title: str) -> float:
    query_set = set(normalize_space(query).lower().split(" "))
    title_set = set(normalize_space(title).lower().split(" "))
    if not query_set:
        return 0.0
    return round(len(query_set & title_set) / len(query_set), 3)


def _extract_first_anchor(html: str, href_pattern: str) -> tuple[str, str]:
    pattern = re.compile(
        rf"<a[^>]*href=[\"'](?P<href>{href_pattern})[\"'][^>]*>(?P<text>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(html)
    if not match:
        return "", ""

    href = match.group("href")
    raw_text = re.sub(r"<[^>]+>", " ", match.group("text"))
    text = normalize_space(raw_text)
    return href, text


def _build_result(parsed: ParsedProduct, platform: str, search_url: str, error: str = "") -> PlatformMatch:
    return PlatformMatch(
        input_name=parsed.original_name,
        platform=platform,
        brand=parsed.brand,
        product_name=parsed.product_name,
        color=parsed.color,
        query=parsed.normalized_query,
        search_url=search_url,
        product_url="",
        product_title="",
        current_price="",
        stock_status="확인불가",
        confidence=0.0,
        matched_at_utc=datetime.now(timezone.utc).isoformat(),
        error=error,
    )


def fetch_musinsa_match(parsed: ParsedProduct, timeout: int = DEFAULT_TIMEOUT) -> PlatformMatch:
    search_url = f"https://www.musinsa.com/search/musinsa/integration?q={quote_plus(parsed.normalized_query)}"
    base = _build_result(parsed, "musinsa", search_url)

    try:
        response = requests.get(search_url, headers=REQUEST_HEADERS, timeout=timeout)
        response.raise_for_status()
        href, title = _extract_first_anchor(response.text, r"[^\"']*/products/[^\"']*")
        if not href:
            base.error = "검색결과 파싱 실패(상품 링크 미검출)"
            return base

        product_url = urljoin("https://www.musinsa.com", href)

        base.product_url = product_url
        base.product_title = title
        base.current_price = _extract_price_text(response.text)
        base.stock_status = _infer_stock_status(response.text)
        base.confidence = _score_match(parsed.normalized_query, title)
        return base
    except Exception as exc:  # 네트워크/구조변경 대응
        base.error = f"musinsa 요청 실패: {exc}"
        return base


def fetch_29cm_match(parsed: ParsedProduct, timeout: int = DEFAULT_TIMEOUT) -> PlatformMatch:
    search_url = f"https://www.29cm.co.kr/search?keyword={quote_plus(parsed.normalized_query)}"
    base = _build_result(parsed, "29cm", search_url)

    try:
        response = requests.get(search_url, headers=REQUEST_HEADERS, timeout=timeout)
        response.raise_for_status()
        href, title = _extract_first_anchor(
            response.text, r"[^\"']*/(?:catalog|products)/[^\"']*"
        )
        if not href:
            base.error = "검색결과 파싱 실패(상품 링크 미검출)"
            return base

        product_url = urljoin("https://www.29cm.co.kr", href)

        base.product_url = product_url
        base.product_title = title
        base.current_price = _extract_price_text(response.text)
        base.stock_status = _infer_stock_status(response.text)
        base.confidence = _score_match(parsed.normalized_query, title)
        return base
    except Exception as exc:
        base.error = f"29cm 요청 실패: {exc}"
        return base


def run_matching(full_names: Iterable[str], timeout: int = DEFAULT_TIMEOUT) -> list[PlatformMatch]:
    results: list[PlatformMatch] = []

    for name in full_names:
        if not normalize_space(name):
            continue

        parsed = parse_product_name(name)
        results.append(fetch_musinsa_match(parsed, timeout=timeout))
        results.append(fetch_29cm_match(parsed, timeout=timeout))

    return results


def load_names_from_csv(file) -> list[str]:
    df = pd.read_csv(file)
    lower_map = {c.lower(): c for c in df.columns}

    for candidate in ["product_name", "name", "상품명", "full_name"]:
        if candidate in lower_map:
            return [str(v) for v in df[lower_map[candidate]].fillna("").tolist()]

    raise ValueError("CSV에 상품명 컬럼이 필요합니다. (product_name / name / 상품명 / full_name)")


def render_app() -> None:
    st.set_page_config(page_title="Product Link Matcher Pro", page_icon="🔎", layout="wide")
    st.title("🔎 Product Link Matcher Pro (Musinsa / 29CM)")
    st.caption("2026 트렌드형 데이터 그리드 UX: 다건 입력 → 자동 매칭 → 링크/가격/재고/신뢰도 검토 → CSV 다운로드")

    with st.sidebar:
        st.subheader("입력 옵션")
        timeout = st.slider("요청 타임아웃(초)", min_value=5, max_value=20, value=DEFAULT_TIMEOUT)
        st.session_state["timeout"] = timeout
        st.info("권장: CSV 업로드 + 텍스트 입력 병행")

    col1, col2 = st.columns([1, 1])
    with col1:
        text_input = st.text_area(
            "상품명 전체 목록 (한 줄에 1개)",
            height=220,
            placeholder="예) Nike Air Force 1 White\nAdidas Samba OG Black\nThisisneverthat Hoodie Gray",
        )

    with col2:
        uploaded = st.file_uploader("CSV 업로드 (상품명 컬럼 포함)", type=["csv"])
        st.markdown("지원 컬럼명: `product_name`, `name`, `상품명`, `full_name`")

    run_btn = st.button("매칭 실행", type="primary", use_container_width=True)

    if run_btn:
        names: list[str] = [line for line in text_input.splitlines() if normalize_space(line)]

        if uploaded is not None:
            try:
                names.extend(load_names_from_csv(uploaded))
            except Exception as exc:
                st.error(f"CSV 로딩 실패: {exc}")
                st.stop()

        names = [normalize_space(n) for n in names if normalize_space(n)]
        names = list(dict.fromkeys(names))  # dedupe

        if not names:
            st.warning("최소 1개 이상의 상품명을 입력해주세요.")
            st.stop()

        timeout = int(st.session_state.get("timeout", DEFAULT_TIMEOUT))

        with st.spinner(f"총 {len(names)}개 상품 매칭 중..."):
            results = run_matching(names, timeout=timeout)

        if not results:
            st.warning("매칭 결과가 없습니다.")
            st.stop()

        df = pd.DataFrame(asdict(r) for r in results)

        success_count = int((df["product_url"].str.len() > 0).sum())
        error_count = int((df["error"].str.len() > 0).sum())
        avg_conf = float(df["confidence"].mean()) if len(df) else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("총 매칭 행수", len(df))
        c2.metric("실제 링크 확보", success_count)
        c3.metric("평균 신뢰도", f"{avg_conf:.2f}")

        if error_count > 0:
            st.info("일부 플랫폼에서 봇 차단/페이지 구조 변경으로 가격·재고 추출이 실패할 수 있습니다.")

        st.dataframe(df, use_container_width=True, height=420)

        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "결과 CSV 다운로드",
            data=csv_bytes,
            file_name=f"product_match_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )


if __name__ == "__main__":
    render_app()
