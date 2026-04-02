#!/usr/bin/env python3
"""
통합 파이프라인: 엑셀 파싱 → 데이터 수집 → HTML 빌드
=====================================================
사용법:
    python run_pipeline.py --api-key YOUR_DART_KEY
    python run_pipeline.py --api-key KEY --skip-dart --skip-funetf
    python run_pipeline.py --build-only  (JSON이 이미 있으면 HTML만 빌드)
    python run_pipeline.py --api-key KEY --date 20260401

단계:
    1. 엑셀 파싱 (엑셀 파일이 있으면)
    2. DART 투자설명서 수집
    3. 제로인(FundDoctor) 수집
    4. FunETF 위험지표 수집
    5. HTML 빌드
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

from common import load_config, setup_logging

log = setup_logging("pipeline")
BASE = Path(__file__).parent


def run_step(name, cmd, skip=False):
    """서브프로세스 실행"""
    if skip:
        print(f"\n{'─'*50}")
        print(f"  [{name}] 건너뜀")
        return True

    print(f"\n{'─'*50}")
    print(f"  [{name}] 시작...")
    print(f"  명령: {' '.join(cmd)}")
    print(f"{'─'*50}\n")

    result = subprocess.run(cmd, cwd=str(BASE))
    if result.returncode != 0:
        log.error("[%s] 실패 (exit code: %d)", name, result.returncode)
        return False
    print(f"\n  [{name}] 완료 ✓")
    return True


def main():
    parser = argparse.ArgumentParser(description="퇴직연금 펀드 비교기 통합 파이프라인")
    parser.add_argument("--api-key", help="DART OpenAPI 인증키")
    parser.add_argument("--date", help="기준일자 (YYYYMMDD, 예: 20260401)")
    parser.add_argument("--skip-excel", action="store_true", help="엑셀 파싱 건너뜀")
    parser.add_argument("--skip-dart", action="store_true", help="DART 수집 건너뜀")
    parser.add_argument("--skip-funddoctor", action="store_true", help="제로인 수집 건너뜀")
    parser.add_argument("--skip-funetf", action="store_true", help="FunETF 수집 건너뜀")
    parser.add_argument("--build-only", action="store_true", help="HTML 빌드만 실행")
    parser.add_argument("--limit", type=int, default=0, help="DART 처리 펀드 수 제한")
    args = parser.parse_args()

    # 기준일자 업데이트
    if args.date:
        config_path = BASE / "config.json"
        config = load_config()
        config["reference_date"] = args.date
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        log.info("기준일자 업데이트: %s", args.date)

    python = sys.executable

    print("=" * 50)
    print("  퇴직연금 펀드 비교기 — 통합 파이프라인")
    print("=" * 50)

    if args.build_only:
        run_step("HTML 빌드", [python, "build_html.py"])
        return

    # 1. 엑셀 파싱
    has_excel = any(BASE.glob("*연금상품공시*.*")) or any(BASE.glob("*pension*.*"))
    skip_excel = args.skip_excel or not has_excel
    if not has_excel and not args.skip_excel:
        log.info("엑셀 파일 없음 — 기존 fund_list_for_dart.json 사용")
    run_step("엑셀 파싱", [python, "excel_parser.py"], skip=skip_excel)

    # fund_list 확인
    fund_list_path = BASE / "fund_list_for_dart.json"
    if not fund_list_path.exists():
        log.error("fund_list_for_dart.json이 없습니다. 엑셀 파일을 넣거나 직접 생성해주세요.")
        sys.exit(1)

    # 2. DART
    if not args.api_key and not args.skip_dart:
        log.warning("--api-key 미지정 → DART 수집 건너뜀")
        args.skip_dart = True

    dart_cmd = [python, "dart_fund_parser.py", "--api-key", args.api_key or ""]
    if args.limit > 0:
        dart_cmd.extend(["--limit", str(args.limit)])
    run_step("DART 투자설명서", dart_cmd, skip=args.skip_dart)

    # 3. 제로인
    run_step("제로인(FundDoctor)", [python, "funddoctor_scraper.py"], skip=args.skip_funddoctor)

    # 4. FunETF
    run_step("FunETF 위험지표", [python, "funetf_scraper.py"], skip=args.skip_funetf)

    # 5. HTML 빌드
    run_step("HTML 빌드", [python, "build_html.py"])

    print(f"\n{'='*50}")
    print("  파이프라인 완료!")
    print(f"  → {BASE / 'index.html'}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
