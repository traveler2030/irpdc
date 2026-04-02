#!/usr/bin/env python3
"""
FunETF API 스크래퍼 v5 — 설정 통합, 에러핸들링 개선
pip install requests tqdm
약 15분 소요 (327개, 페이지방문+API 2회)
"""
import json, re, sys, time
from pathlib import Path

try:
    import requests
    from tqdm import tqdm
except ImportError:
    print("pip install requests tqdm"); sys.exit(1)

from common import load_config, get_reference_date, setup_logging, retry_request

log = setup_logging("funetf")

BASE = Path(__file__).parent
FLIST = BASE / "fund_list_for_dart.json"

config = load_config()
OUT = BASE / config.get("output", {}).get("funetf_results", "funetf_results.json")
CACHE = BASE / config.get("cache_dirs", {}).get("funetf", "funetf_cache")
DLY = config.get("api_delay", {}).get("funetf", 0.5)
REF_DATE = get_reference_date()
API = "https://www.funetf.co.kr/api/public/product/view"


def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })
    try:
        s.get("https://www.funetf.co.kr/", timeout=15)
    except Exception as e:
        log.warning("메인 페이지 접속 실패: %s", e)
    print(f"  세션 쿠키: {len(s.cookies)}개")
    return s


def extract_params(html, code):
    """페이지 HTML에서 API에 필요한 파라미터 추출"""
    p = {"fundCd": code}

    patterns = {
        "gijunYmd": [r"gijunYmd['\"]?\s*[:=]\s*['\"](\d{8})", r"gijunYmd=(\d{8})"],
        "wGijunYmd": [r"wGijunYmd['\"]?\s*[:=]\s*['\"](\d{8})", r"wGijunYmd=(\d{8})"],
        "seoljYmd": [r"seoljYmd['\"]?\s*[:=]\s*['\"](\d{8})", r"seoljYmd=(\d{8})"],
        "repFundCd": [r"repFundCd['\"]?\s*[:=]\s*['\"]([A-Z0-9]+)", r"repFundCd=([A-Z0-9]+)"],
        "ltypeCd": [r"ltypeCd['\"]?\s*[:=]\s*['\"](\d+)", r"ltypeCd=(\d+)"],
        "stypeCd": [r"stypeCd['\"]?\s*[:=]\s*['\"](\d+)", r"stypeCd=(\d+)"],
        "zeroinTypeLcd": [r"zeroinTypeLcd['\"]?\s*[:=]\s*['\"](\d+)"],
        "zeroinTypeCd": [r"zeroinTypeCd['\"]?\s*[:=]\s*['\"](\d+)"],
        "mketDvsn": [r"mketDvsn['\"]?\s*[:=]\s*['\"](\d+)"],
        "gijunYmdNy": [r"gijunYmdNy['\"]?\s*[:=]\s*['\"](\d{8})"],
        "pfGijunYmd": [r"pfGijunYmd['\"]?\s*[:=]\s*['\"](\d{8})"],
        "_csrf": [r'name="_csrf"\s+content="([^"]+)', r"_csrf['\"]?\s*[:=]\s*['\"]([^'\"]+)"],
    }

    for key, pats in patterns.items():
        for pat in pats:
            m = re.search(pat, html)
            if m:
                p[key] = m.group(1)
                break

    p.setdefault("gijunYmd", REF_DATE)
    p.setdefault("wGijunYmd", REF_DATE)
    p.setdefault("usdYn", "N")
    p.setdefault("roleGroupType", "ANONYMOUS")
    p.setdefault("roleType", "ROLE_ANONYMOUS")

    return p


def fetch_page(sess, code):
    """펀드 페이지 HTML 가져오기 (캐시)"""
    CACHE.mkdir(exist_ok=True)
    cf = CACHE / f"{code}.html"
    if cf.exists():
        with open(cf, "r", encoding="utf-8") as f:
            return f.read()

    resp = retry_request(sess, "get",
                         f"https://www.funetf.co.kr/product/fund/view/{code}",
                         timeout=15)
    if resp and resp.status_code == 200:
        with open(cf, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return resp.text

    log.warning("[%s] 페이지 로드 실패", code)
    return ""


def call_api(sess, endpoint, params, code=""):
    """API 호출"""
    headers = {
        "Accept": "application/json",
        "Referer": f"https://www.funetf.co.kr/product/fund/view/{params.get('fundCd', '')}",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = retry_request(sess, "get", f"{API}/{endpoint}",
                         params=params, headers=headers, timeout=15)
    if resp and resp.status_code == 200:
        try:
            return resp.json()
        except Exception as e:
            log.warning("[%s] JSON 파싱 실패 (%s): %s", code, endpoint, e)
    elif resp:
        log.warning("[%s] API %s 응답: HTTP %d", code, endpoint, resp.status_code)
    return None


def main():
    if not FLIST.exists():
        log.error("%s 없음", FLIST); sys.exit(1)
    with open(FLIST, "r", encoding="utf-8") as f:
        funds = json.load(f)

    print(f"=== FunETF API v5 ===")
    print(f"  대상: {len(funds)}개 | 기준일: {REF_DATE}\n")

    sess = get_session()

    # 전체 수집
    print("[수집 시작]")
    results = {}
    errors = []

    for fund in tqdm(funds, desc="  수집중"):
        code = fund.get("code", "")
        if not code:
            continue

        result = {
            "code": code,
            "volatility_3y": None, "sharpe_3y": None, "beta_3y": None,
            "jensen_alpha_3y": None, "info_ratio_3y": None,
            "pct_rank_vol": None, "pct_rank_sharpe": None,
            "fund_feature": "", "fund_strategy": "",
        }

        # 페이지 방문 + 파라미터 추출
        time.sleep(DLY)
        html = fetch_page(sess, code)
        if not html:
            errors.append({"code": code, "error": "페이지 로드 실패"})
            results[code] = result
            continue

        params = extract_params(html, code)

        # 파라미터 추출 검증
        if "repFundCd" not in params:
            log.info("[%s] repFundCd 미추출 — 일부 API 호출 제한 가능", code)

        # 위험지표 (3년)
        time.sleep(DLY)
        rparams = {**params, "schRiskTerm": "36", "schCtenDvsn": "MK_VIEW"}
        risk = call_api(sess, "riskanalysis", rparams, code)
        if risk and isinstance(risk, list) and len(risk) >= 2:
            r0 = risk[0]
            r1 = risk[1]
            if r0 and isinstance(r0, dict):
                result["volatility_3y"] = r0.get("yyDev")
                result["sharpe_3y"] = r0.get("sharp")
                result["beta_3y"] = r0.get("betaMkt")
                result["jensen_alpha_3y"] = r0.get("alphaMkt")
                result["info_ratio_3y"] = r0.get("ir")
            if r1 and isinstance(r1, dict):
                result["pct_rank_vol"] = r1.get("yyDev")
                result["pct_rank_sharpe"] = r1.get("sharp")
        elif risk is not None:
            log.info("[%s] 위험지표 응답 형식 예상 외: %s", code, str(risk)[:100])

        # 펀드특징/운용전략
        time.sleep(DLY)
        desc = call_api(sess, "zeroindiscription", params, code)
        if desc:
            d2 = desc.get("discription2", [])
            if d2 and len(d2) > 0:
                result["fund_feature"] = d2[0].get("discription3", "") or ""
                result["fund_strategy"] = d2[0].get("discription4", "") or ""

        results[code] = result

    # 저장
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    if errors:
        err_file = BASE / "funetf_errors.json"
        with open(err_file, "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        log.warning("  실패: %d개 → %s", len(errors), err_file)

    hv = sum(1 for r in results.values() if r["volatility_3y"] is not None)
    hf = sum(1 for r in results.values() if r["fund_feature"])
    ht = sum(1 for r in results.values() if r["fund_strategy"])

    print(f"\n{'='*50}")
    print(f"  완료! {len(results)}개")
    print(f"  변동성(3Y): {hv} | 특징: {hf} | 전략: {ht}")
    print(f"  → {OUT}")


if __name__ == "__main__":
    main()
