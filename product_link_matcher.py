"""Glowny Product Link Matcher - Musinsa / 29CM
실행: streamlit run product_link_matcher.py
"""
 
from __future__ import annotations
 
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Iterable
from urllib.parse import quote, quote_plus, urljoin, urlparse
 
import pandas as pd
import requests
import streamlit as st
 
# ────────────────────────────────────────────
# 공통 설정
# ────────────────────────────────────────────
 
HEADERS_MUSINSA = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json, text/html,*/*;q=0.9",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.musinsa.com/",
}
 
HEADERS_29CM = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,*/*;q=0.9",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.29cm.co.kr/",
}
 
 
# ────────────────────────────────────────────
# 상품명 전처리
# ────────────────────────────────────────────
 
def normalize(text: str) -> str:
    text = str(text or "").lower().strip()
    text = re.sub(r"\(w\)|\(m\)|\[w\]|\[m\]", " ", text)
    text = re.sub(r"[^0-9a-zA-Z가-힣\s/]", " ", text)
    return re.sub(r"\s+", " ", text).strip()
 
 
def similarity(a: str, b: str) -> float:
    a_n, b_n = normalize(a), normalize(b)
    if not a_n or not b_n:
        return 0.0
    seq = SequenceMatcher(None, a_n, b_n).ratio()
    ta, tb = set(a_n.split()), set(b_n.split())
    jac = len(ta & tb) / (len(ta | tb) or 1)
    return round(seq * 0.55 + jac * 0.45, 4)
 
 
def make_search_query(raw_name: str) -> str:
    """
    '글로니' 브랜드명 및 컬러 접두어 제거 후 핵심 제품명만 추출.
    예) 'Glowny Cinch Back Roll-Up Jeans Blue' → '글로니 Cinch Back Roll-Up Jeans'
    입력명이 영문이면 앞에 '글로니' 를 붙여 검색 정확도를 높임.
    """
    name = re.sub(r"(?i)^glowny\s+", "", raw_name).strip()
 
    # 끝에 컬러명 제거 (단순 색상어)
    color_pat = re.compile(
        r"\s+(white|black|gray|grey|beige|brown|navy|blue|green|red|pink|"
        r"ivory|cream|oatmeal|melange|ash|ecru|khaki|camel|sand|charcoal|"
        r"light\s+gray|dark\s+gray|melange\s+gray|light\s+beige)$",
        flags=re.IGNORECASE,
    )
    name = color_pat.sub("", name).strip()
 
    return f"글로니 {name}"
 
 
# ────────────────────────────────────────────
# 무신사
# ────────────────────────────────────────────
 
MUSINSA_API = (
    "https://www.musinsa.com/api/search/v2/goods"
    "?keyword={q}&sortCode=RELEVANCE&page=1&size=20&brandCode=glowny"
)
MUSINSA_API_NO_BRAND = (
    "https://www.musinsa.com/api/search/v2/goods"
    "?keyword={q}&sortCode=RELEVANCE&page=1&size=20"
)
MUSINSA_BRAND_PAGE = "https://www.musinsa.com/brands/glowny?category=&page=1&per_page=60&sort=NEWER&keyword={q}"
 
 
def _musinsa_via_api(session: requests.Session, query: str, timeout: int) -> list[dict]:
    """무신사 내부 검색 API (JSON 응답) 사용."""
    results = []
    for url_tpl in [MUSINSA_API, MUSINSA_API_NO_BRAND]:
        url = url_tpl.format(q=quote_plus(query))
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                items = (
                    data.get("data", {}).get("list", [])
                    or data.get("data", {}).get("goods", [])
                    or data.get("list", [])
                    or []
                )
                for item in items:
                    goods_no = item.get("goodsNo") or item.get("goods_no") or item.get("id", "")
                    name = (
                        item.get("goodsName")
                        or item.get("goods_name")
                        or item.get("name", "")
                    )
                    price = str(item.get("normalPrice") or item.get("price", ""))
                    if goods_no:
                        results.append({
                            "url": f"https://www.musinsa.com/products/{goods_no}",
                            "name": name,
                            "price": price,
                        })
        except Exception:
            pass
        if results:
            break
    return results
 
 
def _musinsa_via_search_html(session: requests.Session, query: str, timeout: int) -> list[dict]:
    """HTML 검색 결과 fallback (og:title 방식)."""
    url = f"https://www.musinsa.com/search/musinsa/integration?q={quote_plus(query)}&type=product"
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code != 200:
            return []
        # data-goods-no 속성 또는 /products/{id} 패턴 추출
        ids = re.findall(r"/products/(\d+)", r.text)
        ids += re.findall(r'goods[_\-]?no["\s:=]+["\']?(\d+)', r.text, re.IGNORECASE)
        seen, results = set(), []
        for gid in ids:
            if gid in seen:
                continue
            seen.add(gid)
            results.append({"url": f"https://www.musinsa.com/products/{gid}", "name": "", "price": ""})
        return results
    except Exception:
        return []
 
 
def search_musinsa(session: requests.Session, raw_name: str, timeout: int,
                   min_conf: float, max_candidates: int) -> dict:
    query = make_search_query(raw_name)
    candidates = _musinsa_via_api(session, query, timeout)
    if not candidates:
        candidates = _musinsa_via_search_html(session, query, timeout)
    if not candidates:
        return {"url": "", "name": "", "price": "", "confidence": 0.0,
                "search_url": f"https://www.musinsa.com/search/musinsa/integration?q={quote_plus(query)}&type=product",
                "error": "후보 없음 (봇 차단 가능성)"}
 
    search_url = f"https://www.musinsa.com/search/musinsa/integration?q={quote_plus(query)}&type=product"
    scored = []
    for c in candidates[:max_candidates]:
        name = c.get("name", "")
        if not name:
            # 상세 페이지에서 이름 가져오기
            try:
                dr = session.get(c["url"], timeout=timeout)
                og = re.search(r'property="og:title"\s+content="([^"]+)"', dr.text)
                name = og.group(1) if og else ""
            except Exception:
                pass
        score = similarity(raw_name, name) if name else 0.1
        scored.append((score, c["url"], name, c.get("price", "")))
 
    scored.sort(reverse=True)
    best = scored[0]
    if best[0] < min_conf:
        return {"url": best[1], "name": best[2], "price": best[3],
                "confidence": best[0], "search_url": search_url,
                "error": f"유사도 낮음({best[0]:.2f}) — 직접 확인 권장"}
    return {"url": best[1], "name": best[2], "price": best[3],
            "confidence": best[0], "search_url": search_url, "error": ""}
 
 
# ────────────────────────────────────────────
# 29CM
# ────────────────────────────────────────────
 
CM29_APIS = [
    "https://search.29cm.co.kr/api/v2/search/products?query={q}&page=0&size=20",
    "https://www.29cm.co.kr/api/v2/search/products?query={q}&page=0&size=20",
]
CM29_SEARCH_URLS = [
    "https://www.29cm.co.kr/search?query={q}",
    "https://www.29cm.co.kr/search?keyword={q}",
]
 
 
def _29cm_via_api(session: requests.Session, query: str, timeout: int) -> list[dict]:
    for tpl in CM29_APIS:
        url = tpl.format(q=quote_plus(query))
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code != 200:
                continue
            data = r.json()
            items = (
                data.get("data", {}).get("products", [])
                or data.get("products", [])
                or data.get("result", {}).get("products", [])
                or []
            )
            results = []
            for item in items:
                pid = item.get("itemNo") or item.get("id") or item.get("product_id", "")
                name = item.get("itemName") or item.get("name", "")
                price = str(item.get("consumerPrice") or item.get("price", ""))
                if pid:
                    results.append({
                        "url": f"https://www.29cm.co.kr/products/{pid}",
                        "name": name,
                        "price": price,
                    })
            if results:
                return results
        except Exception:
            pass
    return []
 
 
def _29cm_via_html(session: requests.Session, query: str, timeout: int) -> list[dict]:
    for tpl in CM29_SEARCH_URLS:
        url = tpl.format(q=quote_plus(query))
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code >= 400:
                continue
            ids = re.findall(r"/products/(\d+)", r.text)
            ids += re.findall(r'"itemNo"\s*:\s*(\d+)', r.text)
            seen, results = set(), []
            for pid in ids:
                if pid in seen:
                    continue
                seen.add(pid)
                results.append({"url": f"https://www.29cm.co.kr/products/{pid}", "name": "", "price": ""})
            if results:
                return results
        except Exception:
            pass
    return []
 
 
def search_29cm(session: requests.Session, raw_name: str, timeout: int,
                min_conf: float, max_candidates: int) -> dict:
    query = make_search_query(raw_name)
    candidates = _29cm_via_api(session, query, timeout)
    if not candidates:
        candidates = _29cm_via_html(session, query, timeout)
 
    search_url = CM29_SEARCH_URLS[0].format(q=quote_plus(query))
 
    if not candidates:
        return {"url": "", "name": "", "price": "", "confidence": 0.0,
                "search_url": search_url, "error": "후보 없음 (봇 차단 가능성)"}
 
    scored = []
    for c in candidates[:max_candidates]:
        name = c.get("name", "")
        if not name:
            try:
                dr = session.get(c["url"], timeout=timeout)
                og = re.search(r'property="og:title"\s+content="([^"]+)"', dr.text)
                if og:
                    name = og.group(1)
            except Exception:
                pass
        score = similarity(raw_name, name) if name else 0.1
        scored.append((score, c["url"], name, c.get("price", "")))
 
    scored.sort(reverse=True)
    best = scored[0]
    if best[0] < min_conf:
        return {"url": best[1], "name": best[2], "price": best[3],
                "confidence": best[0], "search_url": search_url,
                "error": f"유사도 낮음({best[0]:.2f}) — 직접 확인 권장"}
    return {"url": best[1], "name": best[2], "price": best[3],
            "confidence": best[0], "search_url": search_url, "error": ""}
 
 
# ────────────────────────────────────────────
# 메인 매칭 실행
# ────────────────────────────────────────────
 
def run_matching(
    names: list[str],
    timeout: int,
    min_conf: float,
    max_candidates: int,
    musinsa_cookie: str | None,
    cm29_cookie: str | None,
    progress_cb=None,
) -> pd.DataFrame:
    m_session = requests.Session()
    m_session.headers.update(HEADERS_MUSINSA)
    if musinsa_cookie:
        m_session.headers["Cookie"] = musinsa_cookie.strip()
 
    c_session = requests.Session()
    c_session.headers.update(HEADERS_29CM)
    if cm29_cookie:
        c_session.headers["Cookie"] = cm29_cookie.strip()
 
    rows = []
    total = len(names)
 
    for i, raw_name in enumerate(names):
        if progress_cb:
            progress_cb(i / total, f"({i+1}/{total}) {raw_name[:40]}...")
 
        m = search_musinsa(m_session, raw_name, timeout, min_conf, max_candidates)
        time.sleep(0.3)
        c = search_29cm(c_session, raw_name, timeout, min_conf, max_candidates)
        time.sleep(0.3)
 
        both = bool(m["url"] and c["url"])
        rows.append({
            "입력 상품명": raw_name,
            "검색 키워드": make_search_query(raw_name),
            "무신사 매칭명": m["name"],
            "무신사 URL": m["url"],
            "무신사 가격": m["price"],
            "무신사 유사도": m["confidence"],
            "무신사 검색링크": m["search_url"],
            "무신사 오류": m["error"],
            "29CM 매칭명": c["name"],
            "29CM URL": c["url"],
            "29CM 가격": c["price"],
            "29CM 유사도": c["confidence"],
            "29CM 검색링크": c["search_url"],
            "29CM 오류": c["error"],
            "매칭상태": "둘다 매칭" if both else ("부분 매칭" if (m["url"] or c["url"]) else "미매칭"),
        })
 
    if progress_cb:
        progress_cb(1.0, "완료!")
    return pd.DataFrame(rows)
 
 
# ────────────────────────────────────────────
# Streamlit UI
# ────────────────────────────────────────────
 
def render_app():
    st.set_page_config(page_title="글로니 상품 매칭", layout="wide", page_icon="🔗")
    st.title("🔗 글로니 상품 링크 자동 매칭")
    st.caption("무신사 · 29CM 자동 검색 — 상품명 기반으로 URL 매칭")
 
    with st.expander("⚠️ 사용 전 읽어주세요", expanded=False):
        st.markdown("""
**동작 방식**
- 글로니 상품명 → 브랜드명·컬러 제거 → `글로니 {핵심명}` 으로 검색
- 무신사: 내부 검색 API → HTML fallback
- 29CM: 검색 API → HTML fallback
 
**봇 차단 대응**
- 두 사이트 모두 Cloudflare 보호 중입니다.
- 막힐 경우 브라우저에서 해당 사이트 로그인 후 쿠키를 사이드바에 붙여넣으세요.
- `cf_clearance` 쿠키가 가장 중요합니다.
 
**컬러 매칭**
- 무신사/29CM에서 컬러는 별도 상품이 아닌 옵션(variant)으로 존재하는 경우가 많아
  상품명 매칭 후 해당 상품 페이지에서 컬러 옵션을 직접 확인하세요.
""")
 
    # ── 사이드바 ──
    with st.sidebar:
        st.header("설정")
        timeout = st.slider("요청 타임아웃(초)", 5, 30, 12)
        min_conf = st.slider("최소 유사도 임계치", 0.05, 0.95, 0.25, 0.05,
                              help="낮을수록 더 많은 결과, 높을수록 더 엄격")
        max_cand = st.slider("후보 상세 조회 개수", 3, 15, 8)
        st.divider()
        st.subheader("쿠키 (선택)")
        musinsa_cookie = st.text_area("무신사 Cookie", height=80,
                                       placeholder="mss_au=...; cf_clearance=...;")
        cm29_cookie = st.text_area("29CM Cookie", height=80,
                                    placeholder="access_token=...; cf_clearance=...;")
 
    # ── 입력 ──
    col1, col2 = st.columns([2, 1])
    with col1:
        text_input = st.text_area(
            "상품명 목록 (한 줄 1개)",
            height=220,
            placeholder="G CLASSIC WAFFLE PANTS WHITE\nG CINCH BACK ROLL-UP JEANS BLUE\n...",
        )
    with col2:
        uploaded = st.file_uploader("CSV / Excel 업로드", type=["csv", "xlsx", "xls"])
        st.caption("지원 컬럼: `input_name`, `상품명`, `name`, `full_name`, `product_name`")
        st.markdown("---")
        st.markdown("**검색 키워드 변환 예시**")
        st.code("입력: Glowny Cinch Back Roll-Up Jeans Blue\n→ 검색: 글로니 Cinch Back Roll-Up Jeans")
 
    if st.button("🚀 매칭 실행", type="primary", use_container_width=True):
        names: list[str] = [x.strip() for x in text_input.splitlines() if x.strip()]
 
        if uploaded:
            try:
                if uploaded.name.endswith(".csv"):
                    df_in = pd.read_csv(uploaded)
                else:
                    df_in = pd.read_excel(uploaded)
 
                col_map = {c.lower().strip(): c for c in df_in.columns}
                found_col = None
                for candidate in ["input_name", "상품명", "name", "full_name", "product_name"]:
                    if candidate in col_map:
                        found_col = col_map[candidate]
                        break
                if not found_col:
                    # 첫 번째 컬럼 사용
                    found_col = df_in.columns[0]
                    st.info(f"컬럼명 자동 선택: `{found_col}`")
 
                csv_names = [str(v).strip() for v in df_in[found_col].dropna() if str(v).strip()]
                names.extend(csv_names)
                st.success(f"파일에서 {len(csv_names)}개 상품명 로드")
            except Exception as e:
                st.error(f"파일 읽기 오류: {e}")
 
        names = list(dict.fromkeys(n for n in names if n))
        if not names:
            st.warning("상품명을 입력하거나 파일을 업로드해주세요.")
            st.stop()
 
        st.info(f"총 {len(names)}개 상품 매칭 시작...")
        progress_bar = st.progress(0.0)
        status_text = st.empty()
 
        def progress_cb(pct, msg):
            progress_bar.progress(pct)
            status_text.text(msg)
 
        with st.spinner("매칭 중..."):
            result_df = run_matching(
                names, timeout, min_conf, max_cand,
                musinsa_cookie or None, cm29_cookie or None,
                progress_cb,
            )
 
        progress_bar.empty()
        status_text.empty()
 
        # ── 결과 요약 ──
        total = len(result_df)
        both = (result_df["매칭상태"] == "둘다 매칭").sum()
        partial = (result_df["매칭상태"] == "부분 매칭").sum()
        none_ = (result_df["매칭상태"] == "미매칭").sum()
 
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("전체", total)
        c2.metric("✅ 둘다 매칭", both)
        c3.metric("⚠️ 부분 매칭", partial)
        c4.metric("❌ 미매칭", none_)
 
        # ── 컬러 하이라이트 ──
        def color_status(val):
            if val == "둘다 매칭":
                return "background-color: #d4edda; color: #155724"
            elif val == "부분 매칭":
                return "background-color: #fff3cd; color: #856404"
            else:
                return "background-color: #f8d7da; color: #721c24"
 
        display_cols = [
            "입력 상품명", "검색 키워드",
            "무신사 매칭명", "무신사 URL", "무신사 유사도",
            "29CM 매칭명", "29CM URL", "29CM 유사도",
            "매칭상태",
        ]
        styled = result_df[display_cols].style.applymap(color_status, subset=["매칭상태"])
        st.dataframe(styled, use_container_width=True, height=500)
 
        # ── 미매칭 항목 검색 링크 ──
        unmatch = result_df[result_df["매칭상태"] != "둘다 매칭"]
        if len(unmatch) > 0:
            with st.expander(f"🔍 미매칭/부분 매칭 {len(unmatch)}개 — 수동 확인 링크"):
                for _, row in unmatch.iterrows():
                    st.markdown(f"**{row['입력 상품명']}**")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.markdown(f"[무신사 검색 →]({row['무신사 검색링크']})")
                        if row["무신사 오류"]:
                            st.caption(f"오류: {row['무신사 오류']}")
                    with col_b:
                        st.markdown(f"[29CM 검색 →]({row['29CM 검색링크']})")
                        if row["29CM 오류"]:
                            st.caption(f"오류: {row['29CM 오류']}")
 
        # ── 다운로드 ──
        st.divider()
        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            csv_data = result_df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "📥 전체 결과 CSV 다운로드",
                data=csv_data.encode("utf-8-sig"),
                file_name=f"glowny_matched_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with col_dl2:
            matched_only = result_df[result_df["무신사 URL"].str.len() > 0]
            csv_matched = matched_only[["입력 상품명", "무신사 URL", "29CM URL", "매칭상태"]].to_csv(
                index=False, encoding="utf-8-sig"
            )
            st.download_button(
                "📥 매칭된 항목만 CSV",
                data=csv_matched.encode("utf-8-sig"),
                file_name=f"glowny_matched_only_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
 
 
if __name__ == "__main__":
    render_app()


