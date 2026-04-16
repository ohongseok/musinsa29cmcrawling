"""Realtime Product Link Matcher Pro (Musinsa / 29CM).

실행:
    streamlit run product_link_matcher.py

주의:
- 사이트 구조/봇 차단 정책에 따라 결과가 달라질 수 있음.
- 정확도 향상을 위해 세션 쿠키(User Cookie)를 선택적으로 입력 가능.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from datetime import datetime, timezone
import json
import re
from typing import Iterable
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
import streamlit as st


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

MUSINSA_SEARCH_URLS = [
    "https://www.musinsa.com/search/musinsa/integration?q={q}",
    "https://www.musinsa.com/search?q={q}",
]

CM29_SEARCH_URLS = [
    "https://www.29cm.co.kr/search?keyword={q}",
    "https://shop.29cm.co.kr/search?keyword={q}",
    "https://www.29cm.co.kr/search/products?keyword={q}",
]


@dataclass
class PlatformMatch:
    input_name: str
    platform: str
    matched_name: str
    product_url: str
    current_price: str
    stock_status: str
    confidence: float
    search_url: str
    matched_at_utc: str
    error: str


def normalize_text(text: str) -> str:
    text = str(text or "").lower().strip()
    text = re.sub(r"\(w\)|\(m\)|\[w\]|\[m\]", " ", text)
    text = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def similarity(a: str, b: str) -> float:
    a_norm, b_norm = normalize_text(a), normalize_text(b)
    if not a_norm or not b_norm:
        return 0.0
    seq = SequenceMatcher(None, a_norm, b_norm).ratio()
    ta, tb = set(a_norm.split()), set(b_norm.split())
    jac = len(ta & tb) / (len(ta | tb) or 1)
    return round((seq * 0.6) + (jac * 0.4), 4)


def build_session(cookie: str | None = None) -> requests.Session:
    s = requests.Session()
    s.headers.update(REQUEST_HEADERS)
    if cookie:
        s.headers.update({"Cookie": cookie.strip()})
    return s


def extract_links_and_titles(html: str, platform: str) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []

    if platform == "musinsa":
        href_pattern = r"(?P<href>/products/\d+|https?://(?:www\.)?musinsa\.com/products/\d+)"
    else:
        href_pattern = r"(?P<href>/products/\d+|/catalog/\d+|https?://(?:www\.|shop\.)?29cm\.co\.kr/(?:products|catalog)/\d+)"

    pattern = re.compile(
        rf"<a[^>]*href=[\"']{href_pattern}[\"'][^>]*>(?P<title>.*?)</a>",
        flags=re.IGNORECASE | re.DOTALL,
    )

    for m in pattern.finditer(html):
        href = m.group("href")
        title = re.sub(r"<[^>]+>", " ", m.group("title"))
        title = normalize_text(title)
        if href:
            results.append((href, title))

    # 중복 제거
    dedup = []
    seen = set()
    for href, title in results:
        key = (href, title)
        if key in seen:
            continue
        seen.add(key)
        dedup.append((href, title))

    return dedup


def parse_product_meta(html: str) -> tuple[str, str]:
    """상품 상세 HTML에서 가격/재고 추출."""
    price = ""
    stock = "확인불가"

    # JSON-LD 우선
    for block in re.findall(r"<script[^>]+application/ld\+json[^>]*>(.*?)</script>", html, flags=re.DOTALL | re.IGNORECASE):
        try:
            payload = json.loads(block.strip())
        except Exception:
            continue

        entries = payload if isinstance(payload, list) else [payload]
        for item in entries:
            if not isinstance(item, dict):
                continue
            offers = item.get("offers")
            if isinstance(offers, dict):
                if not price and offers.get("price"):
                    currency = offers.get("priceCurrency", "KRW")
                    price = f"{offers.get('price')} {currency}"
                availability = str(offers.get("availability", ""))
                if "InStock" in availability:
                    stock = "판매중"
                elif "OutOfStock" in availability:
                    stock = "품절"

    # fallback regex
    if not price:
        m = re.search(r"([₩￦]\s?[\d,]+|\d{1,3}(?:,\d{3})+\s?원)", html)
        if m:
            price = m.group(1)

    if stock == "확인불가":
        lowered = html.lower()
        if any(k in lowered for k in ["sold out", "품절", "일시품절", "out of stock"]):
            stock = "품절"
        elif any(k in lowered for k in ["장바구니", "구매하기", "buy now"]):
            stock = "판매중(추정)"

    return price, stock


def fetch_best_match(
    session: requests.Session,
    input_name: str,
    platform: str,
    timeout: int,
    min_confidence: float,
) -> PlatformMatch:
    search_templates = MUSINSA_SEARCH_URLS if platform == "musinsa" else CM29_SEARCH_URLS

    base = PlatformMatch(
        input_name=input_name,
        platform=platform,
        matched_name="",
        product_url="",
        current_price="",
        stock_status="확인불가",
        confidence=0.0,
        search_url="",
        matched_at_utc=datetime.now(timezone.utc).isoformat(),
        error="",
    )

    last_error = "검색 실패"

    for template in search_templates:
        search_url = template.format(q=quote_plus(input_name))
        base.search_url = search_url
        try:
            r = session.get(search_url, timeout=timeout)
            if r.status_code >= 400:
                last_error = f"{platform} search {r.status_code}"
                continue

            pairs = extract_links_and_titles(r.text, platform)
            if not pairs:
                last_error = "상품 링크 패턴 미검출"
                continue

            scored = []
            for href, title in pairs:
                score = similarity(input_name, title or input_name)
                scored.append((score, href, title))

            scored.sort(reverse=True, key=lambda x: x[0])
            best_score, best_href, best_title = scored[0]

            if best_score < min_confidence:
                last_error = f"유사도 부족({best_score:.2f})"
                continue

            if platform == "musinsa":
                full_url = urljoin("https://www.musinsa.com", best_href)
            else:
                full_url = urljoin("https://www.29cm.co.kr", best_href)

            base.matched_name = best_title or input_name
            base.product_url = full_url
            base.confidence = best_score

            # 상세 페이지로 가격/재고
            try:
                dr = session.get(full_url, timeout=timeout)
                if dr.status_code < 400:
                    price, stock = parse_product_meta(dr.text)
                    base.current_price = price
                    base.stock_status = stock
                else:
                    base.error = f"detail {dr.status_code}"
            except Exception as exc:
                base.error = f"detail error: {exc}"

            return base

        except Exception as exc:
            last_error = f"{platform} search error: {exc}"
            continue

    base.error = last_error
    return base


def run_realtime_matching(
    names: Iterable[str],
    timeout: int,
    min_confidence: float,
    cookie: str | None,
) -> pd.DataFrame:
    session = build_session(cookie)
    rows = []

    for name in names:
        n = name.strip()
        if not n:
            continue

        m = fetch_best_match(session, n, "musinsa", timeout, min_confidence)
        c = fetch_best_match(session, n, "29cm", timeout, min_confidence)

        status = "both_matched" if (m.product_url and c.product_url) else "partial_or_none"

        rows.append(
            {
                "input_name": n,
                "29cm_name": c.matched_name,
                "29cm_url": c.product_url,
                "29cm_price": c.current_price,
                "29cm_stock": c.stock_status,
                "29cm_confidence": c.confidence,
                "musinsa_name": m.matched_name,
                "musinsa_url": m.product_url,
                "musinsa_price": m.current_price,
                "musinsa_stock": m.stock_status,
                "musinsa_confidence": m.confidence,
                "match_status": status,
                "29cm_error": c.error,
                "musinsa_error": m.error,
            }
        )

    return pd.DataFrame(rows)


def render_app() -> None:
    st.set_page_config(page_title="Realtime Matcher Pro", layout="wide", page_icon="⚡")
    st.title("⚡ Realtime Product Matcher (Musinsa/29CM)")
    st.caption("실시간 사이트 조회 방식: 상품명 입력 → 링크/가격/재고 추출 (없으면 공란)")

    st.info(
        "차단/로그인 이슈가 있으면 쿠키가 필요할 수 있습니다.\n"
        "필요 시 브라우저 DevTools에서 Request Cookie를 전달해주세요."
    )

    with st.sidebar:
        timeout = st.slider("요청 타임아웃(초)", 5, 20, 10)
        min_conf = st.slider("최소 유사도", 0.10, 0.95, 0.45, 0.01)
        cookie = st.text_area("선택: Cookie 헤더", height=120, placeholder="ex) sessionid=...; other=...")

    c1, c2 = st.columns([2, 1])
    with c1:
        text = st.text_area("상품명 목록 (한 줄 1개)", height=260)
    with c2:
        up = st.file_uploader("입력 CSV", type=["csv"])
        st.markdown("지원 컬럼: `input_name`, `name`, `product_name`, `상품명`, `full_name`")

    if st.button("실시간 매칭 실행", type="primary", use_container_width=True):
        names = [x.strip() for x in text.splitlines() if x.strip()]

        if up is not None:
            df = pd.read_csv(up)
            cols = {c.lower().strip(): c for c in df.columns}
            found = None
            for c in ["input_name", "name", "product_name", "상품명", "full_name"]:
                if c in cols:
                    found = cols[c]
                    break
            if not found:
                st.error("입력 CSV 컬럼을 찾지 못했습니다.")
                st.stop()
            names.extend([str(v).strip() for v in df[found].fillna("").tolist() if str(v).strip()])

        names = list(dict.fromkeys(names))
        if not names:
            st.warning("입력된 상품명이 없습니다.")
            st.stop()

        with st.spinner(f"{len(names)}개 조회 중..."):
            out = run_realtime_matching(names, timeout=timeout, min_confidence=min_conf, cookie=cookie or None)

        if out.empty:
            st.warning("결과가 없습니다.")
            st.stop()

        st.dataframe(out, use_container_width=True, height=620)

        st.download_button(
            "결과 CSV 다운로드",
            data=out.to_csv(index=False).encode("utf-8-sig"),
            file_name="realtime_matched_products.csv",
            mime="text/csv",
            use_container_width=True,
        )


if __name__ == "__main__":
    render_app()

