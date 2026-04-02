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

        # safety 분류
        if "채권" in fund_type and "혼합" not in fund_type:
            safety = "안전자산"
        elif "TDF" in name.upper() and ("주식혼합" in fund_type or "혼합주식" in fund_type):
            safety = "적격TDF"
        elif "혼합자산" in fund_type or "혼합자산" in name:
            safety = "판단필요(혼합자산)"
        else:
            safety = "판단필요(혼합)"

        # TDF
        is_tdf = "TDF" if "TDF" in name.upper() or "타겟데이트" in name else "Non-TDF"

        # vintage
        vintage = 0
        if is_tdf == "TDF":
            vm = re.search(r'(?:TDF|타겟데이트)\s*(\d{4})', name, re.IGNORECASE)
            if vm:
                vintage = int(vm.group(1))

        # subType
        sub_type = fund_type if fund_type else "Unknown"

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

    # 안전자산 편입 불가 Non-TDF 제거 (주식형, 혼합주식형, 주식파생형 등)
    EXCLUDE_TYPES = {"주식형", "혼합주식형", "주식파생형", "혼합주식파생형"}
    before = len(funds)
    def _is_stock_fund(f):
        if f["tdf"] != "Non-TDF":
            return False
        if f["subType"] in EXCLUDE_TYPES:
            return True
        # 재간접형 중 주식-재간접, 주식혼합-재간접 제거
        if f["subType"] == "재간접형":
            name = f["name"]
            if re.search(r'주식.{0,3}재간접|주식혼합.{0,3}재간접', name):
                return True
        return False
    funds = [f for f in funds if not _is_stock_fund(f)]
    log.info("  안전자산 편입불가 Non-TDF 제거: %d → %d개", before, len(funds))

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
