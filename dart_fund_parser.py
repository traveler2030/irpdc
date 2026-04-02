#!/usr/bin/env python3
"""
DART 투자설명서 자동 수집·파싱 스크립트 v3
==========================================
퇴직연금 펀드의 투자설명서에서
환헤지 여부(비율 포함), 세부 자산배분, 운용전략을 자동 추출합니다.

사용법:
    python dart_fund_parser.py --api-key YOUR_DART_API_KEY

필요 패키지:
    pip install requests beautifulsoup4 lxml tqdm
"""

import argparse
import json
import os
import re
import sys
import time
import zipfile
import io
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict

try:
    import requests
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except ImportError:
    print("필요 패키지를 설치해주세요:")
    print("  pip install requests beautifulsoup4 lxml tqdm")
    sys.exit(1)

from common import load_config, setup_logging, retry_request

log = setup_logging("dart_fund_parser")

BASE_DIR = Path(__file__).parent
config = load_config()
FUND_LIST_FILE = BASE_DIR / config.get("output", {}).get("fund_list", "fund_list_for_dart.json")
OUTPUT_FILE = BASE_DIR / config.get("output", {}).get("dart_results", "dart_parsed_results.json")
CACHE_DIR = BASE_DIR / config.get("cache_dirs", {}).get("dart", "dart_cache")
DART_API_BASE = "https://opendart.fss.or.kr/api"
API_DELAY = config.get("api_delay", {}).get("dart", 0.5)


# ─── 1단계: DART 기업코드 ────────────────────────────────
def download_corp_codes(api_key):
    cache_file = CACHE_DIR / "corp_codes.json"
    if cache_file.exists():
        print("  [캐시] corp_codes.json 로드")
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    print("  DART 기업코드 다운로드 중...")
    sess = requests.Session()
    resp = retry_request(sess, "get", f"{DART_API_BASE}/corpCode.xml", params={"crtfc_key": api_key})
    if not resp or resp.status_code != 200:
        log.error("기업코드 다운로드 실패: HTTP %s", resp.status_code if resp else "N/A")
        return {}

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("CORPCODE.xml") as f:
            tree = ET.parse(f)

    corp_map = {}
    for item in tree.getroot().findall("list"):
        code = item.findtext("corp_code", "")
        name = item.findtext("corp_name", "")
        if code and name:
            corp_map[name] = code

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(corp_map, f, ensure_ascii=False, indent=2)
    print(f"  {len(corp_map)}개 기업코드 다운로드 완료")
    return corp_map


def match_company_codes(companies, corp_map):
    matched = {}
    for company in companies:
        if company in corp_map:
            matched[company] = corp_map[company]
            continue
        short = company.replace("자산운용", "").strip()
        for name, code in corp_map.items():
            if short in name and "자산운용" in name:
                matched[company] = code
                break
        if company not in matched:
            for name, code in corp_map.items():
                if company[:4] in name:
                    matched[company] = code
                    break
    # ─── 수동 매핑 (DART 등록명이 다른 운용사) ───
    matched["케이비자산운용"] = "00206513"

    return matched


# ─── 2단계: 펀드공시 검색 ────────────────────────────────
def search_fund_disclosures(api_key, corp_code, company_name):
    cache_file = CACHE_DIR / f"disclosures_{corp_code}.json"
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    all_results = []
    page = 1
    sess = requests.Session()
    while True:
        time.sleep(API_DELAY)
        resp = retry_request(sess, "get", f"{DART_API_BASE}/list.json", params={
            "crtfc_key": api_key, "corp_code": corp_code,
            "bgn_de": "20230101", "end_de": "20261231",
            "pblntf_ty": "G", "page_no": page, "page_count": 100,
        })
        if not resp or resp.status_code != 200:
            break
        data = resp.json()
        if data.get("status") != "000":
            break
        items = data.get("list", [])
        all_results.extend(items)
        if page >= data.get("total_page", 1):
            break
        page += 1

    prospectuses = [i for i in all_results if "투자설명서" in i.get("report_nm", "")]
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(prospectuses, f, ensure_ascii=False, indent=2)
    return prospectuses


def match_fund_to_disclosure(fund_name, base_name, disclosures):
    clean = re.sub(r'(Class\S+|종류\S+|\bCpe?\b|\bC-P2e?\b|\bC-Re?\b|\bS-[PR]\b|\(퇴직연금?\))', '', fund_name).strip()
    keywords = re.findall(r'[가-힣]{2,}', clean)
    best, best_score = None, 0
    for disc in disclosures:
        rn = disc.get("report_nm", "")
        score = sum(1 for kw in keywords if kw in rn)
        if score > best_score:
            best_score = score
            best = disc
    return best if best_score >= 2 else None


# ─── 3단계: 문서 다운로드 ────────────────────────────────
def download_document(api_key, rcept_no):
    cache_file = CACHE_DIR / f"doc_{rcept_no}.txt"
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            return f.read()

    time.sleep(API_DELAY)
    sess = requests.Session()
    resp = retry_request(sess, "get", f"{DART_API_BASE}/document.xml", params={
        "crtfc_key": api_key, "rcept_no": rcept_no,
    })
    if not resp or resp.status_code != 200:
        log.warning("문서 다운로드 실패: rcept_no=%s", rcept_no)
        return ""

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            texts = []
            for fname in zf.namelist():
                try:
                    raw = zf.read(fname)
                    soup = BeautifulSoup(raw, "lxml")
                    texts.append(soup.get_text(separator="\n", strip=True))
                except Exception:
                    continue
            full_text = "\n".join(texts)
    except zipfile.BadZipFile:
        return ""

    with open(cache_file, "w", encoding="utf-8") as f:
        f.write(full_text[:500000])
    return full_text


# ─── 4단계: 파싱 ─────────────────────────────────────────

def _ctx(text, keyword, radius=300):
    idx = text.find(keyword)
    if idx == -1:
        return ""
    return text[max(0, idx - radius):min(len(text), idx + radius)]


def _extract_detail(text, patterns):
    found = {}
    for pattern, label in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 0 < val <= 100 and label not in found:
                found[label] = val
    return [{"name": n, "pct": p} for n, p in found.items()]


def parse_hedge(text):
    result = {"type": "미확인", "detail": ""}

    # 비율 추출
    ratio_pats = [
        r"환헤지\s*비율[^.]*?(\d{1,3})\s*%",
        r"(\d{1,3})\s*%\s*(?:이상|수준|내외).*?환헤지",
        r"환위험.*?(\d{1,3})\s*%\s*(?:이상|수준|내외).*?(?:헤지|회피)",
        r"환헤지.*?목표.*?(\d{1,3})\s*%",
    ]
    hedge_ratio = None
    for p in ratio_pats:
        m = re.search(p, text)
        if m:
            v = int(m.group(1))
            if 0 < v <= 100:
                hedge_ratio = v
                break

    h_score = sum(1 for p in [
        r"환헤지를\s*실시", r"환위험을\s*(?:전액|100%|완전).*?(?:회피|헤지)",
        r"90%\s*이상.*?환헤지", r"환율\s*변동\s*위험.*?(?:최소화|제거)",
    ] if re.search(p, text))

    uh_score = sum(1 for p in [
        r"환헤지를?\s*(?:하지\s*않|실시하지\s*않|수행하지\s*않)",
        r"환위험에\s*(?:노출|그대로)", r"환노출",
        r"환율\s*변동.*?그대로.*?노출",
    ] if re.search(p, text))

    partial_score = sum(1 for p in [
        r"부분.*?환헤지", r"환헤지\s*비율.*?(?:변동|조정|탄력)",
        r"글라이드\s*패스.*?환", r"환헤지.*?일부",
        r"전략적.*?환헤지", r"목표시점.*?따라.*?환헤지.*?비율",
    ] if re.search(p, text))

    if partial_score > 0 or (h_score > 0 and uh_score > 0):
        result["type"] = "부분헤지"
        if hedge_ratio:
            result["detail"] = f"환헤지 {hedge_ratio}%, 환노출 {100 - hedge_ratio}%"
        else:
            ctx = _ctx(text, "환헤지", 300)
            pcts = [int(v) for v in re.findall(r"(\d{1,3})\s*%", ctx) if 10 <= int(v) <= 90]
            if pcts:
                result["detail"] = f"환헤지 약 {pcts[0]}%, 환노출 약 {100 - pcts[0]}%"
            else:
                result["detail"] = "비율 변동 (글라이드패스 연동)"
    elif h_score > uh_score:
        result["type"] = "H"
        result["detail"] = f"환헤지 {hedge_ratio}%" if hedge_ratio and hedge_ratio < 100 else "환헤지 실시"
    elif uh_score > 0:
        result["type"] = "UH"
        result["detail"] = "환헤지 미실시 (환노출)"

    return result


def parse_allocation(text):
    stock = {"total": None, "detail": []}
    bond = {"total": None, "detail": []}
    commodity = {"total": None, "detail": []}
    alt = {"total": None, "detail": []}

    def _find_total(patterns):
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                v = int(m.group(1))
                if 0 < v <= 100:
                    return v
        return None

    stock["total"] = _find_total([
        r"주식.*?(?:투자|편입|비중|비율).*?(\d{1,3})\s*%",
        r"(?:투자|편입).*?주식.*?(\d{1,3})\s*%",
        r"주식(?:형)?.*?(\d{1,3})\s*%\s*(?:이하|이내|내외|수준|까지)",
    ])

    stock["detail"] = _extract_detail(text, [
        (r"(?:미국|US|S&P|나스닥|NASDAQ).*?(?:주식|성장주|대형주|지수).*?(\d{1,3})\s*%", "미국주식"),
        (r"(\d{1,3})\s*%.*?(?:미국|US|S&P).*?(?:주식|성장|대형)", "미국주식"),
        (r"(?:미국|US)\s*(?:대형)?(?:성장|가치|배당).*?(\d{1,3})\s*%", "미국주식"),
        (r"(?:선진국|해외선진|MSCI\s*World|글로벌선진).*?(?:주식)?.*?(\d{1,3})\s*%", "선진국주식"),
        (r"(\d{1,3})\s*%.*?(?:선진국|해외선진).*?주식", "선진국주식"),
        (r"(?:신흥국|이머징|EM|신흥시장).*?(?:주식)?.*?(\d{1,3})\s*%", "신흥국주식"),
        (r"(\d{1,3})\s*%.*?(?:신흥국|이머징).*?주식", "신흥국주식"),
        (r"(?:국내|한국|코스피|KOSPI).*?주식.*?(\d{1,3})\s*%", "국내주식"),
        (r"(\d{1,3})\s*%.*?(?:국내|한국|코스피).*?주식", "국내주식"),
        (r"(?:유럽|EURO|STOXX).*?(?:주식)?.*?(\d{1,3})\s*%", "유럽주식"),
        (r"(?:일본|JAPAN|TOPIX|닛케이).*?(?:주식)?.*?(\d{1,3})\s*%", "일본주식"),
        (r"(?:글로벌|해외|전세계|ACWI).*?주식.*?(\d{1,3})\s*%", "글로벌주식"),
        (r"(\d{1,3})\s*%.*?(?:글로벌|해외|전세계).*?주식", "글로벌주식"),
        (r"(?:소형주|Small\s*Cap|스몰캡).*?(\d{1,3})\s*%", "소형주"),
        (r"(?:가치주|Value).*?(\d{1,3})\s*%", "가치주"),
        (r"(?:성장주|Growth).*?(\d{1,3})\s*%", "성장주"),
        (r"(?:배당주|Dividend).*?(\d{1,3})\s*%", "배당주"),
        (r"(?:테크|기술주|Technology|IT).*?주식.*?(\d{1,3})\s*%", "기술주"),
    ])

    bond["total"] = _find_total([
        r"채권.*?(?:투자|편입|비중|비율).*?(\d{1,3})\s*%",
        r"(?:투자|편입).*?채권.*?(\d{1,3})\s*%",
        r"채권(?:형)?.*?(\d{1,3})\s*%\s*(?:이하|이내|내외|수준|까지)",
    ])

    bond["detail"] = _extract_detail(text, [
        (r"(?:국내|한국).*?(?:국공채|국채|통안채|지방채).*?(\d{1,3})\s*%", "국내국공채"),
        (r"(\d{1,3})\s*%.*?(?:국내|한국).*?(?:국공채|국채)", "국내국공채"),
        (r"(?:국고채|국채).*?(\d{1,3})\s*%", "국고채"),
        (r"(?:회사채|크레딧|신용).*?(\d{1,3})\s*%", "회사채"),
        (r"(\d{1,3})\s*%.*?(?:회사채|크레딧)", "회사채"),
        (r"(?:투자등급|투자적격|IG|Investment\s*Grade).*?(?:채권)?.*?(\d{1,3})\s*%", "투자등급채권"),
        (r"(\d{1,3})\s*%.*?(?:투자등급|투자적격|IG)", "투자등급채권"),
        (r"(?:하이일드|High\s*Yield|HY|투기등급).*?(\d{1,3})\s*%", "하이일드채권"),
        (r"(\d{1,3})\s*%.*?(?:하이일드|High\s*Yield|HY)", "하이일드채권"),
        (r"(?:글로벌|해외|선진국).*?채권.*?(\d{1,3})\s*%", "글로벌채권"),
        (r"(\d{1,3})\s*%.*?(?:글로벌|해외|선진국).*?채권", "글로벌채권"),
        (r"(?:신흥국|이머징|EM).*?채권.*?(\d{1,3})\s*%", "신흥국채권"),
        (r"(\d{1,3})\s*%.*?(?:신흥국|이머징).*?채권", "신흥국채권"),
        (r"(?:물가연동|TIPS|인플레이션).*?(?:채권)?.*?(\d{1,3})\s*%", "물가연동채"),
        (r"(?:단기|초단기|MMF|유동성).*?(?:채권|자금).*?(\d{1,3})\s*%", "단기채/유동성"),
        (r"(\d{1,3})\s*%.*?(?:단기|초단기).*?채권", "단기채/유동성"),
        (r"(?:MBS|모기지|주택저당).*?(\d{1,3})\s*%", "MBS/모기지"),
        (r"(?:ABS|자산유동화).*?(\d{1,3})\s*%", "ABS"),
        (r"(?:전환사채|CB).*?(\d{1,3})\s*%", "전환사채"),
    ])

    commodity["total"] = _find_total([
        r"(?:원자재|커머디티|commodity).*?(\d{1,3})\s*%",
        r"(\d{1,3})\s*%.*?(?:원자재|커머디티|commodity)",
    ])

    commodity["detail"] = _extract_detail(text, [
        (r"(?:금\s|Gold|골드).*?(\d{1,3})\s*%", "금(Gold)"),
        (r"(\d{1,3})\s*%.*?(?:금\s|Gold|골드)", "금(Gold)"),
        (r"(?:원유|WTI|Brent|석유|오일|Oil).*?(\d{1,3})\s*%", "원유"),
        (r"(\d{1,3})\s*%.*?(?:원유|WTI|Brent)", "원유"),
        (r"(?:은\s|Silver|실버).*?(\d{1,3})\s*%", "은(Silver)"),
        (r"(?:농산물|곡물|Agri).*?(\d{1,3})\s*%", "농산물"),
        (r"(?:광업|금속|Metal|비철|구리|Copper).*?(\d{1,3})\s*%", "금속/광업"),
        (r"(?:원자재\s*ETF|commodity\s*ETF|원자재\s*지수).*?(\d{1,3})\s*%", "원자재ETF/지수"),
        (r"(\d{1,3})\s*%.*?(?:원자재\s*ETF|commodity\s*ETF)", "원자재ETF/지수"),
        (r"(?:에너지|Energy).*?(\d{1,3})\s*%", "에너지"),
        (r"(?:탄소배출권|탄소|Carbon).*?(\d{1,3})\s*%", "탄소배출권"),
    ])

    alt["total"] = _find_total([
        r"(?:대체|대안|Alternative).*?(?:투자|자산).*?(\d{1,3})\s*%",
        r"(\d{1,3})\s*%.*?(?:대체|대안|Alternative).*?(?:투자|자산)",
    ])

    alt["detail"] = _extract_detail(text, [
        (r"(?:리츠|REITs?|부동산).*?(\d{1,3})\s*%", "REITs/부동산"),
        (r"(\d{1,3})\s*%.*?(?:리츠|REITs?|부동산)", "REITs/부동산"),
        (r"(?:인프라|Infrastructure).*?(\d{1,3})\s*%", "인프라"),
        (r"(\d{1,3})\s*%.*?(?:인프라|Infrastructure)", "인프라"),
        (r"(?:사모|PE|Private\s*Equity).*?(\d{1,3})\s*%", "PE/사모"),
        (r"(?:헤지펀드|Hedge\s*Fund).*?(\d{1,3})\s*%", "헤지펀드"),
        (r"(?:MLP|파이프라인|Master\s*Limited).*?(\d{1,3})\s*%", "MLP"),
        (r"(?:삼림|Timber|목재).*?(\d{1,3})\s*%", "삼림/목재"),
        (r"(?:실물자산|Real\s*Asset).*?(\d{1,3})\s*%", "실물자산"),
        (r"(?:벤처|Venture|VC).*?(\d{1,3})\s*%", "벤처/VC"),
    ])

    # 세부합산으로 총비중 보충
    if commodity["total"] is None and commodity["detail"]:
        commodity["total"] = sum(d["pct"] for d in commodity["detail"])
    if alt["total"] is None and alt["detail"]:
        alt["total"] = sum(d["pct"] for d in alt["detail"])

    return {"stock": stock, "bond": bond, "commodity": commodity, "alt": alt}


STRATEGY_SKIP = [
    "투자위험등급", "VaR", "최대손실예상", "아니오",
    "예시>", "제2부", "제1부", "위험등급을 분류", "위험등급 분류",
    "설정된 후 3년", "설정 후 3년", "수익률 변동성",
    "모집(매출) 총액", "투자자에게 적합", "본인의 투자목적에 부합",
    "집합투자규약에 명시된 보수", "과거의 운용실적", "미래의 수익을 보장",
    "원본 손실", "예금자보호법", "97.5%", "95%", "일간 수익률의",
]

STRATEGY_KEYWORDS = [
    "글로벌 자산배분", "자산배분전략", "분산투자",
    "글라이드패스", "글라이드 패스", "생애주기",
    "목표시점", "은퇴시점", "타겟데이트",
    "주식과 채권", "주식 및 채권", "ETF", "인덱스", "패시브", "액티브",
    "국내외", "해외", "글로벌",
    "리밸런싱", "포트폴리오", "멀티인컴",
    "모자형", "자투자신탁", "모투자신탁", "투자대상",
]


def parse_strategy(text):
    """핵심 투자전략 추출 (형식적 문구 제거 버전)"""
    if not text:
        return ""

    sentences = re.split(r'(?<=[.다요음니])\s+', text)
    candidates = []

    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 15 or len(sent) > 300:
            continue
        if any(s in sent for s in STRATEGY_SKIP):
            continue
        score = sum(1 for kw in STRATEGY_KEYWORDS if kw in sent)
        if score > 0:
            candidates.append((score, sent))

    candidates.sort(key=lambda x: -x[0])

    if not candidates:
        # 폴백: 기존 헤더 기반 추출
        for header in [
            r"이\s*투자신탁은", r"(?:투자|운용)\s*(?:전략|방침|목적)\s*[:\-\n]",
            r"집합투자기구의\s*(?:투자|운용)\s*(?:전략|목적)",
            r"이\s*집합투자기구는",
        ]:
            m = re.search(header, text)
            if m:
                snippet = re.sub(r'\s+', ' ', text[m.start():m.start() + 600])
                sents = re.split(r'(?<=[.다요음])\s+', snippet)
                strategy = " ".join(sents[:3]).strip()[:200]
                if len(strategy) > 20:
                    return strategy
        return ""

    result_sents = []
    total_len = 0
    used = set()
    for score, sent in candidates:
        short = sent[:30]
        if short in used:
            continue
        used.add(short)
        result_sents.append(sent)
        total_len += len(sent)
        if len(result_sents) >= 3 or total_len > 300:
            break

    result = " ".join(result_sents)
    return result[:300] if len(result) > 300 else result


# ─── 메인 ────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="DART 투자설명서 자동 파싱 v2")
    parser.add_argument("--api-key", required=True, help="DART OpenAPI 인증키")
    parser.add_argument("--limit", type=int, default=0, help="처리 펀드 수 제한 (0=전체)")
    parser.add_argument("--skip-download", action="store_true", help="캐시만 사용")
    args = parser.parse_args()

    CACHE_DIR.mkdir(exist_ok=True)

    if not FUND_LIST_FILE.exists():
        print(f"[오류] {FUND_LIST_FILE} 없음. 같은 폴더에 놓아주세요.")
        sys.exit(1)

    with open(FUND_LIST_FILE, "r", encoding="utf-8") as f:
        funds = json.load(f)

    print(f"=== DART 투자설명서 자동 파싱 v3 ===")
    print(f"  대상: {len(funds)}개 | 캐시: {CACHE_DIR}\n")

    # 1. 기업코드
    print("[1/4] 기업코드 다운로드...")
    corp_map = download_corp_codes(args.api_key)
    if not corp_map:
        print("  [오류] 기업코드 실패. API 키 확인.")
        sys.exit(1)

    companies = sorted(set(f["company"] for f in funds))
    company_codes = match_company_codes(companies, corp_map)
    print(f"  운용사 매칭: {len(company_codes)}/{len(companies)}")
    for c in companies:
        code = company_codes.get(c, "?")
        print(f"    {'✓' if code != '?' else '✗'} {c} → {code}")
    print()

    # 2. 펀드공시 검색
    print("[2/4] 펀드공시 검색...")
    all_disc = {}
    for co, code in tqdm(company_codes.items(), desc="  운용사별"):
        disc = search_fund_disclosures(args.api_key, code, co)
        all_disc[co] = disc
        if disc:
            tqdm.write(f"    {co}: {len(disc)}건")
    print()

    # 3. 다운로드 + 파싱
    print("[3/4] 투자설명서 다운로드 및 파싱...")
    results = {}
    fund_iter = funds[:args.limit] if args.limit > 0 else funds

    for fund in tqdm(fund_iter, desc="  펀드별"):
        fid = fund["id"]
        fname = fund["name"]
        bname = fund["baseName"]
        co = fund["company"]

        r = {
            "id": fid, "name": fname, "baseName": bname,
            "hedge": {"type": "미확인", "detail": ""},
            "stock": {"total": None, "detail": []},
            "bond": {"total": None, "detail": []},
            "commodity": {"total": None, "detail": []},
            "alt": {"total": None, "detail": []},
            "strategy": "", "dart_url": "", "dart_report": "",
        }

        # 펀드명 1차 환헤지
        if "(H)" in fname and "(UH)" not in fname:
            r["hedge"] = {"type": "H", "detail": "환헤지 실시 (펀드명 표기)"}
        elif "(UH)" in fname:
            r["hedge"] = {"type": "UH", "detail": "환헤지 미실시 (펀드명 표기)"}

        # 투자설명서 매칭
        disc = all_disc.get(co, [])
        matched = match_fund_to_disclosure(fname, bname, disc)

        if matched:
            rcept = matched.get("rcept_no", "")
            r["dart_url"] = f"https://dart.fss.or.kr/dsaf001/main.do?rcept_no={rcept}"
            r["dart_report"] = matched.get("report_nm", "")

            if not args.skip_download:
                text = download_document(args.api_key, rcept)
                if text:
                    if r["hedge"]["type"] == "미확인":
                        r["hedge"] = parse_hedge(text)
                    alloc = parse_allocation(text)
                    r["stock"] = alloc["stock"]
                    r["bond"] = alloc["bond"]
                    r["commodity"] = alloc["commodity"]
                    r["alt"] = alloc["alt"]
                    r["strategy"] = parse_strategy(text)

        results[str(fid)] = r
    print()

    # 4. 저장
    print("[4/4] 결과 저장...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 통계
    ht = defaultdict(int)
    st_t = st_d = bd_d = cm = at = strat = url = 0
    for r in results.values():
        ht[r["hedge"]["type"]] += 1
        if r["stock"]["total"] is not None: st_t += 1
        if r["stock"]["detail"]: st_d += 1
        if r["bond"]["detail"]: bd_d += 1
        if r["commodity"]["total"] is not None: cm += 1
        if r["alt"]["total"] is not None: at += 1
        if r["strategy"]: strat += 1
        if r["dart_url"]: url += 1

    print(f"\n{'='*50}")
    print(f"  완료! 처리: {len(results)}개")
    print(f"{'='*50}")
    print(f"  DART 링크: {url}개")
    print(f"  환헤지: {dict(ht)}")
    print(f"  주식 총비중: {st_t}개 | 세부: {st_d}개")
    print(f"  채권 세부: {bd_d}개")
    print(f"  원자재: {cm}개 | 대체: {at}개")
    print(f"  운용전략: {strat}개")
    print(f"\n  결과: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
