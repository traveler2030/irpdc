#!/usr/bin/env python3
"""
제로인(FundDoctor) 펀드 설명 수집기
====================================
펀드특징 + 운용전략 텍스트를 원천 소스(제로인)에서 직접 수집합니다.
HTML 서버 렌더링이라 requests로 바로 수집 가능.

pip install requests beautifulsoup4 tqdm
python funddoctor_scraper.py
"""
import json, re, sys, time
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except ImportError:
    print("pip install requests beautifulsoup4 tqdm"); sys.exit(1)

from common import load_config, get_reference_date, setup_logging, retry_request

log = setup_logging("funddoctor")

BASE = Path(__file__).parent
FLIST = BASE / "fund_list_for_dart.json"

config = load_config()
OUT = BASE / config.get("output", {}).get("funddoctor_results", "funddoctor_results.json")
CACHE = BASE / config.get("cache_dirs", {}).get("funddoctor", "funddoctor_cache")
DLY = config.get("api_delay", {}).get("funddoctor", 1.0)
REF_DATE = get_reference_date()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}


def fetch_profile(sess, code):
    """제로인 펀드 프로필 페이지"""
    CACHE.mkdir(exist_ok=True)
    cf = CACHE / f"{code}.html"
    if cf.exists():
        with open(cf, "r", encoding="utf-8") as f:
            return f.read()

    # fprofile2.jsp 시도 (신버전)
    url = f"https://www.funddoctor.co.kr/afn/fund/fprofile2.jsp?fund_cd={code}&gijun_ymd={REF_DATE}"
    resp = retry_request(sess, "get", url, timeout=15)
    if resp and resp.status_code == 200 and len(resp.text) > 500:
        with open(cf, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return resp.text

    # fprofile.jsp 시도 (구버전)
    url2 = f"https://www.funddoctor.co.kr/afn/fund/fprofile.jsp?fund_cd={code}&gijun_ymd={REF_DATE}"
    resp2 = retry_request(sess, "get", url2, timeout=15)
    if resp2 and resp2.status_code == 200 and len(resp2.text) > 500:
        with open(cf, "w", encoding="utf-8") as f:
            f.write(resp2.text)
        return resp2.text

    log.warning("[%s] 페이지 로드 실패", code)
    return ""


def parse_profile(html):
    """HTML에서 데이터 추출"""
    result = {
        "fund_feature": "",
        "fund_strategy": "",
        "risk_grade": "",
        "zeroin_grade_3y": "",
        "zeroin_grade_5y": "",
        "fund_type": "",
    }

    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # 펀드 설명 텍스트
    desc_patterns = [
        r"((?:\d\)\s*)?이\s*투자신탁은[^<]{50,2000})",
        r"((?:\d\)\s*)?이\s*집합투자기구는[^<]{50,2000})",
        r"((?:\d\)\s*)?이\s*펀드는[^<]{50,2000})",
    ]

    for p in desc_patterns:
        m = re.search(p, text)
        if m:
            desc = m.group(1).strip()
            result["fund_feature"] = desc[:1500]
            break

    # 투자전략
    strat_idx = text.find("투자전략")
    if strat_idx > 0:
        strat_text = text[strat_idx:strat_idx + 2000]
        for end in ["※", "비교지수", "판매사", "보수", "위험"]:
            ei = strat_text.find(end, 10)
            if ei > 0:
                strat_text = strat_text[:ei]
                break
        if len(strat_text) > 30:
            result["fund_strategy"] = strat_text[:1500]

    # 위험등급
    grade_m = re.search(r"(\d)등급\s*\(([^)]+)\)", text)
    if grade_m:
        result["risk_grade"] = f"{grade_m.group(1)}등급({grade_m.group(2)})"

    # 제로인 평가유형
    type_m = re.search(r"제로인\s*평가유형[^가-힣]*([가-힣]+)", text)
    if type_m:
        result["fund_type"] = type_m.group(1)

    return result


def main():
    if not FLIST.exists():
        log.error("%s 없음", FLIST); sys.exit(1)
    with open(FLIST, "r", encoding="utf-8") as f:
        funds = json.load(f)

    print(f"=== 제로인(FundDoctor) 수집기 ===")
    print(f"  대상: {len(funds)}개 | 기준일: {REF_DATE}\n")

    sess = requests.Session()
    sess.headers.update(HEADERS)
    try:
        sess.get("https://www.funddoctor.co.kr/index.jsp", timeout=15)
    except Exception as e:
        log.warning("메인 페이지 접속 실패: %s", e)
    print(f"  세션 쿠키: {len(sess.cookies)}개\n")

    # 전체 수집
    results = {}
    errors = []

    for fund in tqdm(funds, desc="  수집중"):
        code = fund.get("code", "")
        if not code:
            continue

        time.sleep(DLY)
        html = fetch_profile(sess, code)
        result = parse_profile(html)
        result["code"] = code
        results[code] = result

        if not html:
            errors.append({"code": code, "name": fund.get("baseName", ""), "error": "페이지 로드 실패"})

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    if errors:
        err_file = BASE / "funddoctor_errors.json"
        with open(err_file, "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        log.warning("  실패: %d개 → %s", len(errors), err_file)

    hf = sum(1 for r in results.values() if r["fund_feature"])
    hs = sum(1 for r in results.values() if r["fund_strategy"])
    hg = sum(1 for r in results.values() if r["risk_grade"])

    print(f"\n{'='*50}")
    print(f"  완료! {len(results)}개")
    print(f"  펀드설명: {hf} | 전략: {hs} | 등급: {hg}")
    print(f"  → {OUT}")


if __name__ == "__main__":
    main()
