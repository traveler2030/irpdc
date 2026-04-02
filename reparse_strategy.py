#!/usr/bin/env python3
"""
전략 재추출 전용 스크립트
dart_cache/ 에 이미 다운로드된 문서에서 전략만 재추출합니다.
실행: python reparse_strategy.py
"""
import json,re,os
from pathlib import Path

BASE=Path(__file__).parent
CACHE=BASE/"dart_cache"
IN=BASE/"dart_parsed_results.json"
OUT=BASE/"dart_parsed_results.json"

# ─── 형식적 문구 (건너뛸 것) ──────────────────────────────
SKIP=[
    "투자위험등급","VaR","최대손실예상","업데이트","아니오",
    "해외 집합투자기구에 관한 사항","예시>","제2부","제1부",
    "위험등급을 분류","위험등급 분류","설정된 후 3년",
    "설정 후 3년","수익률 변동성","투자위험등급을 산",
    "모집(매출) 총액","투자자에게 적합","본인의 투자목적에 부합",
    "자체적인 기준에 따른","투자자 본인이 판단",
    "집합투자규약에 명시된 보수","재간접투자에 따른 피투자",
    "과거의 운용실적","미래의 수익을 보장",
    "원본 손실","예금자보호법","보수 이외에",
    "97.5%","95%","일간 수익률의","주간 수익률",
    "기준일 변경","환헤지비용 업데이트",
]

def is_skip(text):
    for s in SKIP:
        if s in text:
            return True
    return False

def extract_strategy(text):
    """핵심 투자전략만 추출 (형식적 문구 제거)"""
    if not text:
        return ""
    
    # 1단계: 핵심 전략 문장 후보 수집
    candidates = []
    
    # 전략 관련 키워드가 포함된 문장 찾기
    strategy_keywords = [
        "글로벌 자산배분", "자산배분전략", "분산투자",
        "글라이드패스", "글라이드 패스", "생애주기",
        "목표시점", "은퇴시점", "타겟데이트",
        "주식과 채권", "주식 및 채권", "주식, 채권",
        "ETF", "인덱스", "패시브", "액티브",
        "뱅가드", "캐피탈그룹", "JP모건", "PIMCO", "슈로더",
        "BNY멜론", "AXA", "Amundi", "피델리티", "누버거",
        "국내외", "해외", "글로벌",
        "주식형 집합투자증권", "채권형 집합투자증권",
        "자산별 투자비중을 조절",
        "리밸런싱", "포트폴리오",
        "수익원천", "자본수익", "멀티인컴", "시장중립", "기본수익",
        "배당", "인컴", "성장",
        "채권혼합", "주식혼합",
        "위험자산", "안전자산",
        "모자형", "자투자신탁", "모투자신탁",
        "투자대상", "투자하는 자산",
    ]
    
    # 문장 분리
    sentences = re.split(r'(?<=[.다요음니])\s+', text)
    
    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 15 or len(sent) > 300:
            continue
        if is_skip(sent):
            continue
        # 전략 키워드 매칭 점수
        score = sum(1 for kw in strategy_keywords if kw in sent)
        if score > 0:
            candidates.append((score, sent))
    
    # 점수 높은 순으로 정렬
    candidates.sort(key=lambda x: -x[0])
    
    if not candidates:
        return ""
    
    # 상위 2~3문장 조합
    result_sents = []
    total_len = 0
    used = set()
    for score, sent in candidates:
        # 중복 방지 (비슷한 문장 제거)
        short = sent[:30]
        if short in used:
            continue
        used.add(short)
        result_sents.append(sent)
        total_len += len(sent)
        if len(result_sents) >= 3 or total_len > 300:
            break
    
    result = " ".join(result_sents)
    # 최대 300자
    if len(result) > 300:
        result = result[:297] + "..."
    
    return result


def main():
    if not IN.exists():
        print(f"[오류] {IN} 없음")
        return
    
    with open(IN, "r", encoding="utf-8") as f:
        results = json.load(f)
    
    print(f"=== 전략 재추출 ===")
    print(f"  대상: {len(results)}개 펀드")
    
    updated = 0
    no_cache = 0
    
    for fid, r in results.items():
        dart_url = r.get("dart_url", "")
        if not dart_url:
            continue
        
        # rcept_no 추출
        m = re.search(r"rcept_no=(\d+)", dart_url)
        if not m:
            continue
        rcept_no = m.group(1)
        
        # 캐시 파일 확인
        cache_file = CACHE / f"doc_{rcept_no}.txt"
        if not cache_file.exists():
            no_cache += 1
            continue
        
        with open(cache_file, "r", encoding="utf-8") as f:
            text = f.read()
        
        new_strategy = extract_strategy(text)
        if new_strategy and len(new_strategy) > len(r.get("strategy", "")):
            r["strategy"] = new_strategy
            updated += 1
    
    # 저장
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # 통계
    has_strat = sum(1 for r in results.values() if r["strategy"])
    print(f"\n  업데이트: {updated}개")
    print(f"  캐시없음: {no_cache}개")
    print(f"  전략 보유: {has_strat}/{len(results)}개")
    
    # 샘플
    print(f"\n=== 샘플 ===")
    cnt = 0
    for r in results.values():
        if r["strategy"] and cnt < 5:
            print(f"  {r['baseName'][:40]}")
            print(f"    → {r['strategy'][:120]}")
            print()
            cnt += 1
    
    print(f"  → {OUT} 을 Claude에 업로드!")

if __name__ == "__main__":
    main()
