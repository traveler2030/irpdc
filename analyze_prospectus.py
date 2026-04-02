#!/usr/bin/env python3
"""
투자설명서 분석 결과를 dart_parsed_results.json에 병합하는 유틸리티.
Claude Code 에이전트가 생성한 _analysis_batch_*.json 파일들을 읽어서 병합합니다.

사용법:
    python analyze_prospectus.py --merge
"""
import json
import os
import re
from pathlib import Path

BASE = Path(__file__).parent
DART_RESULTS = BASE / "dart_parsed_results.json"
ANALYSIS_DIR = BASE  # _analysis_batch_*.json files


def get_fund_mapping():
    """fund_list에서 id→code, dart_results에서 id→rcept_no 매핑"""
    with open(BASE / "fund_list_for_dart.json", "r", encoding="utf-8") as f:
        funds = json.load(f)

    with open(DART_RESULTS, "r", encoding="utf-8") as f:
        dart = json.load(f)

    # rcept_no → fund ids 매핑
    rcept_to_ids = {}
    for fid, data in dart.items():
        url = data.get("dart_url", "")
        m = re.search(r"rcept_no=(\d+)", url)
        if m:
            rcept = m.group(1)
            if rcept not in rcept_to_ids:
                rcept_to_ids[rcept] = []
            rcept_to_ids[rcept].append(fid)

    return funds, dart, rcept_to_ids


def merge_analysis():
    """_analysis_batch_*.json 파일들을 dart_parsed_results.json에 병합"""
    funds, dart, rcept_to_ids = get_fund_mapping()

    # 분석 결과 파일 로드
    analysis_files = sorted(BASE.glob("_analysis_batch_*.json"))
    if not analysis_files:
        print("분석 결과 파일 없음 (_analysis_batch_*.json)")
        return

    all_results = {}
    for af in analysis_files:
        with open(af, "r", encoding="utf-8") as f:
            batch = json.load(f)
        all_results.update(batch)
        print(f"  로드: {af.name} ({len(batch)}개)")

    print(f"\n총 분석 결과: {len(all_results)}개 문서")

    # 병합
    updated = 0
    for rcept_no, analysis in all_results.items():
        fids = rcept_to_ids.get(rcept_no, [])
        for fid in fids:
            if fid not in dart:
                continue

            d = dart[fid]

            # 환헤지
            if analysis.get("hedge_type") and analysis["hedge_type"] != "미확인":
                d["hedge"] = {
                    "type": analysis["hedge_type"],
                    "detail": analysis.get("hedge_detail", "")
                }

            # 자산배분
            alloc = analysis.get("allocation", {})
            if alloc.get("stock", {}).get("total") is not None:
                d["stock"] = alloc["stock"]
            if alloc.get("bond", {}).get("total") is not None:
                d["bond"] = alloc["bond"]
            if alloc.get("commodity", {}).get("total") is not None:
                d["commodity"] = alloc["commodity"]
            if alloc.get("alt", {}).get("total") is not None:
                d["alt"] = alloc["alt"]

            # 전략/특색
            if analysis.get("strategy"):
                d["strategy"] = analysis["strategy"]
            if analysis.get("feature"):
                d["feature"] = analysis["feature"]

            updated += 1

    # 저장
    with open(DART_RESULTS, "w", encoding="utf-8") as f:
        json.dump(dart, f, ensure_ascii=False, indent=2)

    print(f"\n병합 완료: {updated}개 펀드 업데이트")
    print(f"→ {DART_RESULTS}")


if __name__ == "__main__":
    import sys
    if "--merge" in sys.argv:
        merge_analysis()
    else:
        print("사용법: python analyze_prospectus.py --merge")
