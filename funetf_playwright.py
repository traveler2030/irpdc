#!/usr/bin/env python3
"""
FunETF Playwright v3
pip install playwright tqdm
playwright install chromium
python funetf_playwright.py
"""
import json, sys, time
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
    from tqdm import tqdm
except ImportError:
    print("pip install playwright tqdm"); print("playwright install chromium"); sys.exit(1)

BASE = Path(__file__).parent
FLIST = BASE / "fund_list_for_dart.json"
JSFILE = BASE / "funetf_collect.js"
OUT = BASE / "funetf_results.json"
PARTIAL = BASE / "funetf_partial.json"
DLY = 8

def main():
    if not FLIST.exists():
        print(f"[오류] {FLIST} 없음"); sys.exit(1)
    if not JSFILE.exists():
        print(f"[오류] {JSFILE} 없음"); sys.exit(1)
    
    with open(FLIST, "r", encoding="utf-8") as f:
        funds = json.load(f)
    with open(JSFILE, "r", encoding="utf-8") as f:
        js_code = f.read()

    results = {}
    if PARTIAL.exists():
        with open(PARTIAL, "r", encoding="utf-8") as f:
            results = json.load(f)
        print(f"  이전 결과 {len(results)}개 로드")

    print(f"=== FunETF Playwright v3 ===")
    print(f"  대상: {len(funds)}개 | 수집됨: {len(results)}개\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("[1] FunETF 접속...")
        page.goto("https://www.funetf.co.kr/", wait_until="load", timeout=60000)
        time.sleep(5)
        print("  접속 완료\n")

        ok = fail = skip = 0

        for i, fund in enumerate(tqdm(funds, desc="[2] 수집중")):
            code = fund.get("code", "")
            if not code: continue

            if code in results and results[code].get("volatility_3y") is not None:
                skip += 1
                continue

            time.sleep(DLY)

            try:
                result = page.evaluate(js_code, code)
                if result and (result.get("volatility_3y") is not None or result.get("fund_feature")):
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                result = {"code": code, "volatility_3y": None, "sharpe_3y": None,
                         "beta_3y": None, "jensen_alpha_3y": None,
                         "fund_feature": "", "fund_strategy": ""}
                fail += 1
                tqdm.write(f"  [{code}] 오류: {str(e)[:60]}")

            results[code] = result

            if i < 5 or i % 20 == 0:
                vol = f"{result['volatility_3y']:.2f}" if result.get('volatility_3y') else "-"
                feat = "O" if result.get('fund_feature') else "-"
                tqdm.write(f"  [{i+1}] {code} vol={vol} feat={feat} | ok:{ok} fail:{fail} skip:{skip}")

            if (i + 1) % 50 == 0:
                with open(PARTIAL, "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                tqdm.write(f"  --- 중간 저장 ({len(results)}개) ---")

        browser.close()

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    hv = sum(1 for r in results.values() if r.get("volatility_3y") is not None)
    hf = sum(1 for r in results.values() if r.get("fund_feature"))

    print(f"\n{'='*50}")
    print(f"  완료! {len(results)}개")
    print(f"  위험지표: {hv} | 펀드특징: {hf}")
    print(f"  성공: {ok} | 실패: {fail} | 스킵: {skip}")
    print(f"  → {OUT}")
    if PARTIAL.exists(): PARTIAL.unlink()

if __name__ == "__main__":
    main()
