#!/usr/bin/env python3
"""
빌드 스크립트: 4개 JSON 데이터를 병합하여 index.html 생성
==========================================================
사용법:
    python build_html.py
    python build_html.py --template template.html --output index.html

데이터 소스:
    1. fund_list_for_dart.json  (펀드 마스터 목록 + AUM/수익률)
    2. dart_parsed_results.json (환헤지, 자산배분, 전략, DART URL)
    3. funddoctor_results.json  (펀드설명, 위험등급)
    4. funetf_results.json      (변동성, 샤프비율 등 위험지표)
"""
import argparse
import json
import re
import sys
from pathlib import Path

from common import load_config, get_reference_date, setup_logging

log = setup_logging("build_html")

BASE = Path(__file__).parent
config = load_config()
output_cfg = config.get("output", {})

# 운용사 URL 매핑 (수동)
COMPANY_URLS = {
    "교보악사자산운용": "https://www.kyoboaxa-im.co.kr/fund/list",
    "다올자산운용": "https://www.daolfund.co.kr/",
    "대신자산운용": "https://www.daeshinfund.com/",
    "디비자산운용": "https://www.db-am.co.kr/",
    "마이다스에셋자산운용": "https://www.midasasset.co.kr/",
    "미래에셋자산운용": "https://www.miraeasset.com/",
    "삼성자산운용": "https://www.samsungfund.com/",
    "신한자산운용": "https://www.shinhanamc.com/",
    "에셋플러스자산운용": "https://www.assetplus.co.kr/",
    "우리자산운용": "https://www.woorifund.com/",
    "케이비자산운용": "https://www.kbam.co.kr/",
    "키움투자자산운용": "https://www.kiwoomam.com/",
    "타임폴리오자산운용": "https://www.timefolio.co.kr/",
    "한국투자신탁운용": "https://www.kitmc.com/",
    "한화자산운용": "https://www.hanwhafund.co.kr/",
    "현대인베스트먼트자산운용": "https://www.hi-am.co.kr/",
    "흥국자산운용": "https://www.heungkukfund.com/",
    "BNK자산운용": "https://www.bnkasset.co.kr/",
    "NH아문디자산운용": "https://www.amundi.co.kr/",
}


def load_json(path, default=None):
    """JSON 파일 로드, 없으면 기본값 반환"""
    p = Path(path)
    if not p.exists():
        log.warning("파일 없음: %s", p)
        return default if default is not None else {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_fund_data():
    """4개 데이터 소스를 병합하여 최종 D 배열 생성"""
    # 1) 마스터 목록
    fund_list = load_json(BASE / output_cfg.get("fund_list", "fund_list_for_dart.json"), [])
    if not fund_list:
        log.error("fund_list_for_dart.json이 비어있습니다.")
        sys.exit(1)

    # 2) DART 결과
    dart_data = load_json(BASE / output_cfg.get("dart_results", "dart_parsed_results.json"))

    # 3) FundDoctor 결과
    fd_data = load_json(BASE / output_cfg.get("funddoctor_results", "funddoctor_results.json"))

    # 4) FunETF 결과
    fe_data = load_json(BASE / output_cfg.get("funetf_results", "funetf_results.json"))

    log.info("데이터 소스: 펀드목록=%d, DART=%d, FundDoctor=%d, FunETF=%d",
             len(fund_list), len(dart_data), len(fd_data), len(fe_data))

    # 자식 펀드 그룹핑 (같은 baseName → 부모/자식)
    base_groups = {}
    for fund in fund_list:
        bn = fund.get("baseName", fund.get("name", ""))
        if bn not in base_groups:
            base_groups[bn] = []
        base_groups[bn].append(fund)

    merged = []
    seen_bases = set()

    for fund in fund_list:
        fid = fund.get("id", 0)
        code = fund.get("code", "")
        bn = fund.get("baseName", fund.get("name", ""))
        company = fund.get("company", "")

        # DART 데이터 (id 기반)
        dart = dart_data.get(str(fid), {})

        # FundDoctor 데이터 (code 기반)
        fd = fd_data.get(code, {})

        # FunETF 데이터 (code 기반)
        fe = fe_data.get(code, {})

        # 환헤지
        hedge = dart.get("hedge", {"type": "미확인", "detail": ""})
        # 펀드명에서 환헤지 추론 (DART 데이터 없을 때)
        if hedge["type"] == "미확인":
            name = fund.get("name", "")
            if "(H)" in name and "(UH)" not in name:
                hedge = {"type": "H", "detail": "환헤지 실시 (펀드명 표기)"}
            elif "(UH)" in name:
                hedge = {"type": "UH", "detail": "환헤지 미실시 (펀드명 표기)"}

        # 펀드설명: FunETF > FundDoctor 우선
        sp2 = fe.get("fund_feature", "") or fd.get("fund_feature", "")
        sd2 = fe.get("fund_strategy", "") or fd.get("fund_strategy", "")

        # 투자 스타일 추론
        sty = _infer_style(bn, sp2, sd2, fund.get("subType", ""))

        # 자식 펀드 (같은 baseName의 다른 코드)
        children = []
        group = base_groups.get(bn, [])
        if len(group) > 1 and bn not in seen_bases:
            seen_bases.add(bn)
            for sibling in group:
                if sibling["code"] != code:
                    children.append(sibling["code"])

        # 실질수익률 (수익률 - TER)
        r1 = fund.get("r1", 0)
        ftr_val = fund.get("ftr") or fe.get("sharpe_3y") or 0
        nr1 = round(r1 - (ftr_val or 0), 2) if r1 else 0

        # 자산비중
        sp = fund.get("sp", 0)
        bp = fund.get("bp", 0)
        lp = fund.get("lp", 0)

        record = {
            "c": company,
            "bn": bn,
            "cl": fund.get("cl", ""),
            "sf": fund.get("safety", ""),
            "t": fund.get("tdf", "Non-TDF"),
            "v": fund.get("vintage", 0),
            "a": fund.get("a", 0),
            "r1": r1,
            "r2": fund.get("r2", 0),
            "r3": fund.get("r3", 0),
            "nr": nr1,
            "ft2": fund.get("ft2") or fe.get("volatility_3y"),
            "ftr": ftr_val,
            "cd": code,
            "h": hedge["type"],
            "hd": hedge.get("detail", ""),
            "du": dart.get("dart_url", ""),
            "ds": dart.get("dart_report", ""),
            "sa": dart.get("stock", {"total": None, "detail": []}),
            "ba": dart.get("bond", {"total": None, "detail": []}),
            "sp": sp,
            "bp": bp,
            "lp": lp,
            "st": fund.get("subType", ""),
            "sp2": sp2,
            "sd2": sd2,
            "sty": sty,
            "cu": COMPANY_URLS.get(company, ""),
            "ch": children,
        }
        merged.append(record)

    log.info("병합 완료: %d개 펀드", len(merged))
    return merged


def _infer_style(name, feature, strategy, sub_type):
    """펀드 이름/설명에서 투자 스타일 추론"""
    text = f"{name} {feature} {strategy}".lower()
    if "글라이드" in text or "tdf" in name.upper() or "타겟데이트" in text:
        if "패시브" in text or "인덱스" in text or "etf" in text:
            return "글로벌 패시브"
        return "글로벌 액티브"
    if "인컴" in text or "배당" in text:
        return "인컴/배당"
    if "인덱스" in text or "패시브" in text or "etf" in name.lower():
        return "패시브"
    if "채권" in sub_type:
        return "채권형"
    if "ocio" in text or "자산배분" in text:
        return "자산배분"
    return ""


def build_html(template_path, output_path, fund_data):
    """템플릿에 데이터를 주입하여 최종 HTML 생성"""
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    ref_date = get_reference_date()
    formatted_date = f"{ref_date[:4]}.{ref_date[4:6]}.{ref_date[6:8]}"

    # 데이터 JSON (최소화)
    data_json = json.dumps(fund_data, ensure_ascii=False, separators=(",", ":"))

    # 플레이스홀더 교체
    html = template.replace("{{FUND_DATA}}", data_json)
    html = html.replace("{{FUND_COUNT}}", str(len(fund_data)))
    html = html.replace("{{REF_DATE}}", formatted_date)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = output_path.stat().st_size / 1024
    log.info("HTML 생성: %s (%.0fKB, %d개 펀드)", output_path.name, size_kb, len(fund_data))
    return output_path


def main():
    parser = argparse.ArgumentParser(description="JSON 데이터 → index.html 빌드")
    parser.add_argument("--template", default="template.html", help="HTML 템플릿 파일")
    parser.add_argument("--output", default=output_cfg.get("html", "index.html"), help="출력 HTML 파일")
    args = parser.parse_args()

    template_path = BASE / args.template
    output_path = BASE / args.output

    if not template_path.exists():
        log.error("템플릿 없음: %s", template_path)
        sys.exit(1)

    print("=== HTML 빌드 ===")
    print(f"  템플릿: {template_path.name}")
    print(f"  출력: {output_path.name}\n")

    # 데이터 병합
    fund_data = merge_fund_data()

    # HTML 빌드
    build_html(template_path, output_path, fund_data)

    # 통계
    h_count = sum(1 for d in fund_data if d["h"] != "미확인")
    sa_count = sum(1 for d in fund_data if d["sa"].get("total") is not None)
    ft_count = sum(1 for d in fund_data if d["ft2"] is not None)
    sp_count = sum(1 for d in fund_data if d["sp2"])

    print(f"\n{'='*50}")
    print(f"  완료! {len(fund_data)}개 펀드")
    print(f"  환헤지 확인: {h_count} | 자산배분: {sa_count}")
    print(f"  위험지표: {ft_count} | 펀드설명: {sp_count}")
    print(f"  → {output_path}")


if __name__ == "__main__":
    main()
