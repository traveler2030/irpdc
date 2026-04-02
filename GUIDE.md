# 퇴직연금 펀드 비교기 — 실행 가이드

## 개요

퇴직연금 펀드의 데이터를 수집하여 HTML 비교기를 생성하는 파이프라인입니다.

**데이터 소스:**
- 엑셀 2종 (연금상품공시 + 펀드별 보수비용비교)
- DART OpenAPI (투자설명서 → 환헤지, 자산배분, 전략)
- 제로인 FundDoctor (펀드설명, 위험등급)
- FunETF (변동성, 샤프비율 등 위험지표)

---

## 사전 준비

### 1. 패키지 설치
```bash
pip install -r requirements.txt
```

### 2. DART OpenAPI 키 발급
1. https://opendart.fss.or.kr 접속
2. 회원가입 → 로그인 → "인증키 신청"
3. 발급된 인증키 복사

### 3. 엑셀 파일 준비
- `연금상품공시_YYYYMMDD.xls` — 금감원 통합연금포털에서 다운로드
- `펀드별보수비용비교_YYYYMMDD.xls` — 금투협(dis.kofia.or.kr)에서 다운로드
- 프로젝트 폴더에 저장

---

## 실행 방법

### 전체 파이프라인 (권장)
```bash
python run_pipeline.py --api-key YOUR_DART_KEY
```

### 기준일자 변경
```bash
python run_pipeline.py --api-key KEY --date 20260401
```

### 특정 단계 건너뛰기
```bash
python run_pipeline.py --api-key KEY --skip-funetf --skip-funddoctor
```

### HTML만 다시 빌드 (JSON이 이미 있으면)
```bash
python run_pipeline.py --build-only
```

---

## 개별 스크립트

### 엑셀 파싱
```bash
python excel_parser.py --pension 연금상품공시.xls --fee 보수비용비교.xls
```

### DART 수집 (테스트: 5개만)
```bash
python dart_fund_parser.py --api-key KEY --limit 5
```

### 제로인 수집
```bash
python funddoctor_scraper.py
```

### FunETF 수집
```bash
python funetf_scraper.py
```

### HTML 빌드
```bash
python build_html.py
```

---

## 설정 (config.json)

| 항목 | 설명 | 기본값 |
|------|------|--------|
| `reference_date` | 기준일자 (YYYYMMDD) | `20260323` |
| `api_delay.dart` | DART API 호출 간격(초) | `0.5` |
| `api_delay.funddoctor` | 제로인 요청 간격(초) | `1.0` |
| `api_delay.funetf` | FunETF 요청 간격(초) | `0.5` |
| `retry.max_attempts` | HTTP 재시도 횟수 | `3` |

---

## 파일 구조

```
irpdc/
├── config.json              # 설정
├── excel_parser.py          # 엑셀 → fund_list_for_dart.json
├── dart_fund_parser.py      # DART 투자설명서 파싱
├── funddoctor_scraper.py    # 제로인 수집
├── funetf_scraper.py        # FunETF 수집
├── build_html.py            # JSON 병합 → index.html
├── run_pipeline.py          # 전체 파이프라인
├── common.py                # 공통 유틸리티
├── template.html            # HTML 템플릿
├── index.html               # 최종 결과물
├── fund_list_for_dart.json  # 펀드 마스터 목록
├── *_results.json           # 각 소스별 수집 결과
└── *_cache/                 # 캐시 (삭제 가능)
```

---

## 데이터 갱신 절차

1. 최신 엑셀 2종 다운로드 → 프로젝트 폴더에 저장
2. `python run_pipeline.py --api-key KEY --date YYYYMMDD`
3. `index.html`을 브라우저에서 확인

---

## 문제 해결

| 증상 | 해결 |
|------|------|
| API 키 오류 | opendart.fss.or.kr에서 키 재확인 |
| 엑셀 파싱 실패 | 컬럼명이 변경되었을 수 있음 — excel_parser.py 로그 확인 |
| 중간에 중단 | 캐시가 남아있으므로 재실행 시 이어서 처리 |
| API 한도 초과 | 다음 날 재실행 (캐시 활용) |
| HTML 빌드 실패 | template.html 존재 확인, JSON 파일 형식 확인 |
