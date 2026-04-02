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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE = Path(__file__).parent
CONFIG_FILE = BASE / "config.json"

# ---------------------------------------------------------------------------
# 엑셀 읽기 (xls → xlrd, xlsx → openpyxl)
# ---------------------------------------------------------------------------

def read_excel(path):
    """엑셀 파일을 읽어서 [{col: val, ...}, ...] 리스트로 반환"""
    path = Path(path)
    if not path.exists():
        log.error("파일 없음: %s", path)
        return []

    ext = path.suffix.lower()
    rows = []

    if ext == ".xls":
        try:
            import xlrd
        except ImportError:
            log.error("xlrd 패키지 필요: pip install xlrd")
            sys.exit(1)
        wb = xlrd.open_workbook(str(path))
        ws = wb.sheet_by_index(0)
        headers = [str(ws.cell_value(0, c)).strip() for c in range(ws.ncols)]
        for r in range(1, ws.nrows):
            row = {}
            for c in range(ws.ncols):
                val = ws.cell_value(r, c)
                row[headers[c]] = val
            rows.append(row)
    elif ext == ".xlsx":
        try:
            import openpyxl
        except ImportError:
            log.error("openpyxl 패키지 필요: pip install openpyxl")
            sys.exit(1)
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        ws = wb.active
        data = list(ws.iter_rows(values_only=True))
        if not data:
            return []
        headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(data[0])]
        for row_vals in data[1:]:
            row = {}
            for i, val in enumerate(row_vals):
                if i < len(headers):
                    row[headers[i]] = val
            rows.append(row)
        wb.close()
    else:
        log.error("지원하지 않는 형식: %s (xls/xlsx만 지원)", ext)
        return []

    log.info("  %s: %d행 x %d열 읽기 완료", path.name, len(rows), len(rows[0]) if rows else 0)
    return rows


# ---------------------------------------------------------------------------
# 연금상품공시 파싱
# ---------------------------------------------------------------------------

def find_column(row, candidates):
    """row dict에서 candidates 중 매칭되는 키를 찾아 값 반환"""
    for key in row:
        for cand in candidates:
            if cand in key:
                return row[key]
    return None


def parse_pension_disclosure(rows):
    """연금상품공시 엑셀 → 펀드 기본 정보 리스트"""
    if not rows:
        return []

    # 컬럼 이름 확인 (로그)
    log.info("  컬럼: %s", list(rows[0].keys())[:15])

    funds = []
    for row in rows:
        # 펀드코드 찾기
        code = find_column(row, ["펀드코드", "상품코드", "코드"])
        name = find_column(row, ["펀드명", "상품명", "펀드이름"])
        company = find_column(row, ["운용사", "자산운용사", "운용회사"])

        if not code or not name:
            continue

        code = str(code).strip()
        name = str(name).strip()
        company = str(company).strip() if company else ""

        if not code or not name:
            continue

        # baseName 추출: Class/종류 접미사 제거
        base_name = re.sub(
            r'(Class\S*|종류\S*|\bC-?P\d*e?\d*\b|\bC-Re?\b|\bS-[PR]\b)\s*(\(퇴직연금?\))?\s*$',
            '', name
        ).strip()

        # 클래스 접미사 추출
        cls_match = re.search(
            r'(Class\S+|종류\S+|\bC-?P\d*e?\d*\b|\bC-Re?\b|\bS-[PR]\b)(\(퇴직연금?\))?',
            name
        )
        cl = cls_match.group(0) if cls_match else ""

        # subType
        sub_type = find_column(row, ["유형", "펀드유형", "상품유형", "투자유형"])
        sub_type = str(sub_type).strip() if sub_type else "Unknown"

        # safety 분류
        safety = find_column(row, ["안전자산", "자산구분", "위험분류", "안전"])
        safety = str(safety).strip() if safety else ""
        if not safety or safety == "None":
            # 이름/유형으로 추론
            if "채권" in sub_type and "혼합" not in sub_type:
                safety = "안전자산"
            elif "TDF" in name.upper():
                safety = "적격TDF"
            elif "혼합자산" in sub_type or "혼합자산" in name:
                safety = "판단필요(혼합자산)"
            else:
                safety = "판단필요(혼합)"

        # TDF 여부
        is_tdf = "TDF" if "TDF" in name.upper() or "타겟데이트" in name else "Non-TDF"

        # vintage 추출
        vintage = 0
        if is_tdf == "TDF":
            vm = re.search(r'(?:TDF|타겟데이트)\s*(\d{4})', name, re.IGNORECASE)
            if vm:
                vintage = int(vm.group(1))

        # AUM (순자산)
        aum = find_column(row, ["순자산", "자산규모", "AUM", "설정원본"])
        try:
            aum = float(aum) if aum else 0
        except (ValueError, TypeError):
            aum = 0

        # 수익률
        r1 = _float(find_column(row, ["1년수익률", "수익률(1년)", "1Y"]))
        r2 = _float(find_column(row, ["2년수익률", "수익률(2년)", "2Y"]))
        r3 = _float(find_column(row, ["3년수익률", "수익률(3년)", "3Y"]))

        funds.append({
            "code": code,
            "name": name,
            "baseName": base_name,
            "cl": cl,
            "company": company,
            "subType": sub_type,
            "safety": safety,
            "tdf": is_tdf,
            "vintage": vintage,
            "a": aum,
            "r1": r1,
            "r2": r2,
            "r3": r3,
        })

    log.info("  연금상품공시: %d개 펀드 파싱", len(funds))
    return funds


def _float(v):
    """안전한 float 변환"""
    if v is None:
        return 0
    try:
        return round(float(v), 2)
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# 보수비용비교 파싱 (보조 데이터)
# ---------------------------------------------------------------------------

def parse_fee_comparison(rows, funds_by_code):
    """보수비용비교 엑셀에서 추가 정보를 펀드에 병합"""
    if not rows:
        log.warning("  보수비용비교 파일 비어있음")
        return

    log.info("  컬럼: %s", list(rows[0].keys())[:15])

    merged = 0
    for row in rows:
        code = find_column(row, ["펀드코드", "상품코드", "코드"])
        if not code:
            continue
        code = str(code).strip()
        if code not in funds_by_code:
            continue

        fund = funds_by_code[code]

        # AUM이 없으면 여기서 가져오기
        if fund["a"] == 0:
            aum = find_column(row, ["순자산", "자산규모", "AUM"])
            if aum:
                try:
                    fund["a"] = round(float(aum), 1)
                except (ValueError, TypeError):
                    pass

        # 수익률 보충
        if fund["r1"] == 0:
            fund["r1"] = _float(find_column(row, ["1년수익률", "수익률(1년)"]))
        if fund["r2"] == 0:
            fund["r2"] = _float(find_column(row, ["2년수익률", "수익률(2년)"]))
        if fund["r3"] == 0:
            fund["r3"] = _float(find_column(row, ["3년수익률", "수익률(3년)"]))

        merged += 1

    log.info("  보수비용비교: %d개 펀드 데이터 병합", merged)


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def main():
    config = load_config()
    excel_cfg = config.get("excel_files", {})

    parser = argparse.ArgumentParser(description="엑셀 → fund_list_for_dart.json 변환")
    parser.add_argument("--pension", default=excel_cfg.get("pension_disclosure", "연금상품공시.xls"),
                        help="연금상품공시 엑셀 파일 경로")
    parser.add_argument("--fee", default=excel_cfg.get("fee_comparison", "펀드별보수비용비교.xls"),
                        help="펀드별 보수비용비교 엑셀 파일 경로")
    parser.add_argument("--output", default=config.get("output", {}).get("fund_list", "fund_list_for_dart.json"),
                        help="출력 JSON 파일")
    parser.add_argument("--min-aum", type=float, default=50,
                        help="최소 순자산 필터 (억원, 기본: 50)")
    args = parser.parse_args()

    pension_path = BASE / args.pension
    fee_path = BASE / args.fee
    output_path = BASE / args.output

    print("=== 엑셀 파서 ===")
    print(f"  연금상품공시: {pension_path.name}")
    print(f"  보수비용비교: {fee_path.name}")
    print()

    # 1) 연금상품공시 파싱
    if not pension_path.exists():
        # glob으로 비슷한 파일 찾기
        candidates = list(BASE.glob("*연금상품공시*.*"))
        if candidates:
            pension_path = candidates[0]
            log.info("  자동 탐지: %s", pension_path.name)
        else:
            log.error("연금상품공시 파일을 찾을 수 없습니다.")
            log.error("  다음 위치에 엑셀 파일을 넣어주세요: %s", BASE)
            sys.exit(1)

    log.info("[1/3] 연금상품공시 파싱...")
    pension_rows = read_excel(pension_path)
    funds = parse_pension_disclosure(pension_rows)

    if not funds:
        log.error("  펀드 데이터를 추출할 수 없습니다. 엑셀 형식을 확인해주세요.")
        sys.exit(1)

    # 2) 보수비용비교 병합 (파일이 있으면)
    if not fee_path.exists():
        candidates = list(BASE.glob("*보수비용비교*.*"))
        if candidates:
            fee_path = candidates[0]
            log.info("  자동 탐지: %s", fee_path.name)

    if fee_path.exists():
        log.info("[2/3] 보수비용비교 병합...")
        fee_rows = read_excel(fee_path)
        funds_by_code = {f["code"]: f for f in funds}
        parse_fee_comparison(fee_rows, funds_by_code)
    else:
        log.warning("[2/3] 보수비용비교 파일 없음, 건너뜀")

    # 3) 순자산 필터 + ID 부여 + 저장
    log.info("[3/3] 필터링 및 저장...")
    if args.min_aum > 0:
        before = len(funds)
        funds = [f for f in funds if f["a"] >= args.min_aum]
        log.info("  순자산 %d억+ 필터: %d → %d개", args.min_aum, before, len(funds))

    # ID 부여
    for i, f in enumerate(funds):
        f["id"] = i

    # fund_list_for_dart.json 형식으로 저장 (빌드용 추가 필드는 유지)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(funds, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"  완료! {len(funds)}개 펀드")
    print(f"  TDF: {sum(1 for f in funds if f['tdf']=='TDF')}개")
    print(f"  운용사: {len(set(f['company'] for f in funds))}개")
    print(f"  → {output_path}")


if __name__ == "__main__":
    main()
