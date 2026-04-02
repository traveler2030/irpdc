#!/usr/bin/env python3
"""
엑셀 파서: 두 엑셀 파일에서 fund_list_for_dart.json 생성
==========================================================
사용법:
    python excel_parser.py --pension 연금상품공시.xls --fee 펀드별보수비용비교.xls
    python excel_parser.py  (config.json의 기본 파일명 사용)

출력: fund_list_for_dart.json
"""
import argparse
import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BASE = Path(__file__).parent
CONFIG_FILE = BASE / "config.json"


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _float(v):
    if v is None or v == "":
        return 0
    try:
        return round(float(v), 2)
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# 자산비중 추론
# ---------------------------------------------------------------------------

def _infer_allocation(fund_type, name, tdf, vintage):
    """펀드 유형과 이름에서 전형적 자산비중 추론 → (stock, bond, liquid)"""
    import datetime

    if tdf == "TDF" and vintage > 0:
        # TDF: 빈티지에 따른 글라이드패스 추정
        years_to_target = vintage - datetime.datetime.now().year
        if years_to_target <= 0:
            return (20, 70, 10)
        elif years_to_target <= 5:
            return (30, 60, 10)
        elif years_to_target <= 10:
            return (45, 48, 7)
        elif years_to_target <= 15:
            return (55, 40, 5)
        elif years_to_target <= 20:
            return (65, 30, 5)
        else:
            return (75, 22, 3)

    if tdf == "TDF":
        return (40, 50, 10)

    if fund_type == "채권형":
        return (0, 90, 10)

    if "MMF" in fund_type or "단기금융" in fund_type:
        return (0, 20, 80)

    if fund_type == "혼합채권형":
        return (25, 65, 10)

    if fund_type == "혼합채권파생형":
        return (20, 65, 15)

    if fund_type == "재간접형":
        if "채권혼합" in name or "채권 혼합" in name:
            return (25, 65, 10)
        if "채권" in name and "주식" not in name:
            return (0, 85, 15)
        if "혼합" in name:
            return (30, 55, 15)
        return (20, 60, 20)

    # 폴백
    return (30, 50, 20)


# ---------------------------------------------------------------------------
# 연금상품공시 파싱
# ---------------------------------------------------------------------------

def parse_pension_disclosure(path):
    """
    연금상품공시 엑셀 파싱.
    구조: Row0=헤더, Row1=서브헤더, Row2~=데이터
    Col: 회사(0), 펀드유형(1), 상품종류(2), 펀드명(3), ?(4), 설정일(5),
         기준가(6), 설정원본(7), 순자산(8, 백만원),
         수익률Y1(9), Y2(10), Y3(11),
         주식금액(12), 주식비중(13), 채권금액(14), 채권비중(15),
         유동자산금액(16), 유동자산비중(17)
    """
    import xlrd
    wb = xlrd.open_workbook(str(path))
    ws = wb.sheet_by_index(0)
    log.info("  %s: %d행 x %d열", path.name, ws.nrows, ws.ncols)

    # 서브헤더(row1) 확인해서 Y1/Y2/Y3 위치 파악
    sub_headers = [str(ws.cell_value(1, c)).strip() for c in range(ws.ncols)]
    log.info("  서브헤더: %s", sub_headers[:12])

    # Y1, Y2, Y3 인덱스 찾기
    y1_col = y2_col = y3_col = -1
    for c, h in enumerate(sub_headers):
        if h == "Y1":
            y1_col = c
        elif h == "Y2":
            y2_col = c
        elif h == "Y3":
            y3_col = c

    if y1_col < 0:
        # 폴백: 수익률 컬럼은 보통 9,10,11
        y1_col, y2_col, y3_col = 9, 10, 11
        log.warning("  Y1/Y2/Y3 서브헤더 미발견, col 9/10/11 사용")

    funds = []
    for r in range(2, ws.nrows):
        company = str(ws.cell_value(r, 0)).strip()
        fund_type = str(ws.cell_value(r, 1)).strip()
        product_type = str(ws.cell_value(r, 2)).strip()
        name = str(ws.cell_value(r, 3)).strip()
        aum_million = _float(ws.cell_value(r, 8))  # 백만원 단위

        if not name or not company:
            continue

        # 퇴직연금 상품만 필터 (상품종류가 있으면)
        if product_type and "퇴직" not in product_type:
            continue

        # 순자산 → 억원 변환
        aum = round(aum_million / 100, 1)

        # 수익률
        r1 = _float(ws.cell_value(r, y1_col))
        r2 = _float(ws.cell_value(r, y2_col))
        r3 = _float(ws.cell_value(r, y3_col))

        # baseName: 클래스/종류 접미사 제거
        base_name = re.sub(
            r'\s*(Class\S*|종류\S*|\bC-?P\d*e?\d*\b|\bC-Re?\b|\bS-[PR]\b)\s*(\(퇴직연금?\))?\s*$',
            '', name
        ).strip()

        # 클래스 접미사
        cls_match = re.search(
            r'(Class\S+|종류\S+|\bC-?P\d*e?\d*\b|\bC-Re?\b|\bS-[PR]\b)(\(퇴직연금?\))?',
            name
        )
        cl = cls_match.group(0).strip() if cls_match else ""

        # TDF
        is_tdf = "TDF" if "TDF" in name.upper() or "타겟데이트" in name else "Non-TDF"

        # vintage
        vintage = 0
        if is_tdf == "TDF":
            vm = re.search(r'(?:TDF|타겟데이트)\s*(\d{4})', name, re.IGNORECASE)
            if vm:
                vintage = int(vm.group(1))

        # safety 분류 (DC/IRP 안전자산 30% 룰 기준)
        if is_tdf == "TDF":
            safety = "적격TDF"
        elif fund_type in ("채권형",) or ("채권" in fund_type and "혼합" not in fund_type):
            safety = "안전자산"
        elif "MMF" in fund_type or "단기금융" in fund_type:
            safety = "안전자산"
        elif fund_type in ("혼합채권형", "혼합채권파생형"):
            safety = "안전자산"
        elif fund_type == "재간접형" and re.search(r'채권.{0,5}재간접|채권혼합.{0,5}재간접', name):
            safety = "안전자산"
        elif fund_type == "재간접형" and re.search(r'혼합.{0,5}재간접', name) and "주식" not in name[:15]:
            safety = "안전자산"
        else:
            safety = "편입불가"

        # subType
        sub_type = fund_type if fund_type else "Unknown"

        # 자산비중: 엑셀 데이터 (있으면) 또는 유형별 추론
        stock_pct = _float(ws.cell_value(r, 13)) if ws.ncols > 13 else 0
        bond_pct = _float(ws.cell_value(r, 15)) if ws.ncols > 15 else 0
        liquid_pct = _float(ws.cell_value(r, 17)) if ws.ncols > 17 else 0

        if stock_pct == 0 and bond_pct == 0:
            # 유형별 전형적 비중 추론
            stock_pct, bond_pct, liquid_pct = _infer_allocation(
                fund_type, name, is_tdf, vintage)

        funds.append({
            "name": name,
            "baseName": base_name,
            "cl": cl,
            "company": company,
            "code": "",  # 표준코드는 보수비용비교에서 매칭
            "subType": sub_type,
            "safety": safety,
            "tdf": is_tdf,
            "vintage": vintage,
            "a": aum,
            "r1": r1,
            "r2": r2,
            "r3": r3,
            "sp": round(stock_pct, 1),   # 주식비중
            "bp": round(bond_pct, 1),    # 채권비중
            "lp": round(liquid_pct, 1),  # 유동자산비중
        })

    log.info("  연금상품공시: %d개 퇴직연금 펀드 파싱", len(funds))
    return funds


# ---------------------------------------------------------------------------
# 보수비용비교 파싱
# ---------------------------------------------------------------------------

def parse_fee_comparison(path):
    """
    보수비용비교 엑셀 → {펀드명: {code, ter, fee_total, ...}} 딕셔너리.
    구조: Row0=헤더, Row1=서브헤더, Row2~=데이터
    Col: 운용회사(0), 펀드명(1), ?(2), 펀드유형(3), 설정일(4),
         보수:운용(5),판매(6),수탁(7),사무(8),합계A(9),유사평균(10),
         기타비용B(11), TER=A+B(12),
         판매수수료:선취(13),후취(14), 매매중개(15), 표준코드(16)
    """
    import xlrd
    wb = xlrd.open_workbook(str(path))
    ws = wb.sheet_by_index(0)
    log.info("  %s: %d행 x %d열", path.name, ws.nrows, ws.ncols)

    # 서브헤더로 TER 컬럼 확인
    headers = [str(ws.cell_value(0, c)).strip() for c in range(ws.ncols)]
    ter_col = -1
    code_col = -1
    fee_col = -1
    for c, h in enumerate(headers):
        if "TER" in h:
            ter_col = c
        if "표준코드" in h:
            code_col = c
        if "합계" in str(ws.cell_value(1, c)):
            fee_col = c

    if ter_col < 0:
        ter_col = 12
    if code_col < 0:
        code_col = 16
    if fee_col < 0:
        fee_col = 9

    log.info("  TER col=%d, 코드 col=%d, 보수합계 col=%d", ter_col, code_col, fee_col)

    fee_map = {}
    for r in range(2, ws.nrows):
        name = str(ws.cell_value(r, 1)).strip()
        code = str(ws.cell_value(r, code_col)).strip()
        ter = _float(ws.cell_value(r, ter_col))
        fee_total = _float(ws.cell_value(r, fee_col))

        if not name:
            continue

        fee_map[name] = {
            "code": code,
            "ter": ter,
            "fee_total": fee_total,
        }

    log.info("  보수비용비교: %d개 펀드 로드", len(fee_map))
    return fee_map


def match_fees_to_funds(funds, fee_map):
    """펀드명으로 매칭하여 code/TER 병합"""
    matched = 0
    for fund in funds:
        name = fund["name"]

        # 1차: 정확 매칭
        if name in fee_map:
            fm = fee_map[name]
            fund["code"] = fm["code"]
            fund["ft2"] = fm["fee_total"]
            fund["ftr"] = fm["ter"]
            matched += 1
            continue

        # 2차: baseName으로 부분 매칭
        bn = fund["baseName"]
        best_name = None
        best_score = 0
        for fee_name in fee_map:
            # 키워드 매칭
            keywords = re.findall(r'[가-힣]{2,}', bn)
            score = sum(1 for kw in keywords if kw in fee_name)
            if score > best_score:
                best_score = score
                best_name = fee_name

        if best_name and best_score >= 3:
            fm = fee_map[best_name]
            fund["code"] = fm["code"]
            fund["ft2"] = fm["fee_total"]
            fund["ftr"] = fm["ter"]
            matched += 1

    log.info("  보수 매칭: %d/%d개", matched, len(funds))


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main():
    config = load_config()
    excel_cfg = config.get("excel_files", {})

    parser = argparse.ArgumentParser(description="엑셀 → fund_list_for_dart.json 변환")
    parser.add_argument("--pension", default=excel_cfg.get("pension_disclosure", "연금상품공시.xls"))
    parser.add_argument("--fee", default=excel_cfg.get("fee_comparison", "펀드별보수비용비교.xls"))
    parser.add_argument("--output", default=config.get("output", {}).get("fund_list", "fund_list_for_dart.json"))
    parser.add_argument("--min-aum", type=float, default=50, help="최소 순자산 (억원)")
    args = parser.parse_args()

    pension_path = BASE / args.pension
    fee_path = BASE / args.fee
    output_path = BASE / args.output

    # 자동 탐지
    if not pension_path.exists():
        candidates = sorted(BASE.glob("*연금상품공시*.*"), key=lambda p: p.stat().st_mtime, reverse=True)
        if candidates:
            pension_path = candidates[0]
            log.info("  자동 탐지: %s", pension_path.name)
        else:
            log.error("연금상품공시 파일을 찾을 수 없습니다."); sys.exit(1)

    if not fee_path.exists():
        candidates = sorted(BASE.glob("*보수비용비교*.*"), key=lambda p: p.stat().st_mtime, reverse=True)
        if candidates:
            fee_path = candidates[0]
            log.info("  자동 탐지: %s", fee_path.name)

    print("=== 엑셀 파서 ===")
    print(f"  연금상품공시: {pension_path.name}")
    print(f"  보수비용비교: {fee_path.name}")
    print()

    # 1) 연금상품공시
    log.info("[1/3] 연금상품공시 파싱...")
    funds = parse_pension_disclosure(pension_path)

    if not funds:
        log.error("  펀드가 0개입니다. 엑셀 형식을 확인해주세요."); sys.exit(1)

    # 2) 보수비용비교 매칭
    if fee_path.exists():
        log.info("[2/3] 보수비용비교 매칭...")
        fee_map = parse_fee_comparison(fee_path)
        match_fees_to_funds(funds, fee_map)
    else:
        log.warning("[2/3] 보수비용비교 파일 없음, 건너뜀")

    # 3) 필터 + ID + 저장
    log.info("[3/3] 필터링 및 저장...")

    # 안전자산 30% 룰 편입불가 상품 제거
    before = len(funds)
    funds = [f for f in funds if f["safety"] != "편입불가"]
    log.info("  안전자산 편입불가 제거: %d → %d개", before, len(funds))

    if args.min_aum > 0:
        before = len(funds)
        funds = [f for f in funds if f["a"] >= args.min_aum]
        log.info("  순자산 %d억+ 필터: %d → %d개", args.min_aum, before, len(funds))

    # code 없는 펀드 제거
    no_code = sum(1 for f in funds if not f.get("code"))
    if no_code:
        log.warning("  표준코드 미매칭: %d개", no_code)

    for i, f in enumerate(funds):
        f["id"] = i

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(funds, f, ensure_ascii=False, indent=2)

    has_r1 = sum(1 for f in funds if f.get("r1", 0) != 0)
    has_ter = sum(1 for f in funds if f.get("ftr", 0) != 0)

    print(f"\n{'='*50}")
    print(f"  완료! {len(funds)}개 펀드")
    print(f"  TDF: {sum(1 for f in funds if f['tdf']=='TDF')}개")
    print(f"  운용사: {len(set(f['company'] for f in funds))}개")
    print(f"  수익률(Y1): {has_r1}개 | TER: {has_ter}개")
    print(f"  → {output_path}")


if __name__ == "__main__":
    main()
