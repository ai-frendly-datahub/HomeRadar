# HomeRadar

[![GitHub Actions](https://github.com/<username>/HomeRadar/workflows/HomeRadar%20Crawler/badge.svg)](https://github.com/<username>/HomeRadar/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

부동산 시장 데이터를 수집·분석해 실거주자와 투자자가 주요 시장 신호를 빠르게 파악할 수 있도록 돕는 리포팅 자동화 MVP입니다.

HomeRadar는 **Radar 시리즈**의 일원으로, WineRadar와 동일한 아키텍처를 기반으로 부동산 도메인에 특화된 기능을 제공합니다.

## 프로젝트 목표

- **실거래 데이터 자동 수집**: 국토교통부(MOLIT) API를 통한 아파트 매매·전세 실거래가 일일 자동 수집
- **시장 신호 분석**: 단지별 가격 추이, 전세-매매 갭 변화, 급등/급락 신호를 자동 감지
- **투자·거주 리포트**: 실거주자와 투자자 관점의 지역별 시장 현황 리포트 자동 생성
- **지역 RSS 연동**: 부동산 관련 뉴스·정책 RSS를 함께 수집하여 시장 맥락 정보 제공
- **AI 부동산 도구**: MCP 서버로 AI 어시스턴트에서 부동산 데이터 자연어 검색 및 시장 통계 조회

## 브랜드/네이밍 패턴

- **최상위 브랜드**: HomeRadar (국문: 홈레이더)
- **모듈/서비스**: `HomeRadar <세부 도메인>` (Apt, Rent, Invest 등)
- **초기 포커스**: HomeRadar Apt (아파트 중심 서비스)

## 주요 기능 (MVP - HomeRadar Apt)

1. **관심 단지 추적**: 실거주/투자 관점의 아파트 단지 북마크 + 알림
2. **가격/전세/실거래 신호**: 단기 급등/급락, 전세-매매 갭 변화, 최근 실거래 흐름
3. **수요/공급/이슈 교차**: 재개발/뉴타운 일정, 청약 경쟁률, 학군·교통 호재를 점수화해 "지금 주목" 카드 생성
4. **자동 리포팅**: 매일 주요 시장 신호를 HTML 리포트로 생성 및 GitHub Pages 배포

## 데모

- **[📊 Live Daily Reports](https://[username].github.io/HomeRadar/)** – GitHub Pages에 게시되는 일일 HTML 리포트 (매일 자동 업데이트, 준비 중)

## 기술적 특징

이 프로젝트는 다음 기능을 목표로 하는 스켈레톤 코드를 제공합니다:

- 매일 1회 주요 소스에서 부동산 데이터 자동 수집
- 엔터티 기반 중요도 판별 및 스코어링 (단지명, 지역, 개발 사업 등)
- 그래프 저장소에 부동산-엔터티 관계 보존
- 관점별(View) 쿼리 API (지역/단지/가격대/개발이슈 등)
- HTML 리포트 생성 + GitHub Pages 배포 + 알림 채널
- TDD 기반 개발 - 테스트와 문서 우선

## 빠른 시작

### 사전 요구사항

- Python 3.11 이상
- Git

### 로컬 실행

```bash
git clone https://github.com/<username>/HomeRadar.git
cd HomeRadar

# 가상환경 생성 및 의존성 설치
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements-dev.txt

# 1회 수집 실행 (리포트 생성 포함)
python main.py --mode once --generate-report

# 정기 수집 스케줄러 실행 (24시간 간격)
python main.py --mode scheduler --interval 24

# 데모 파이프라인 실행
python demo_pipeline.py  # RSS 뉴스 수집 → 저장 → 엔티티 추출 → 쿼리

# MOLIT API 데모 (실거래가 수집)
set MOLIT_SERVICE_KEY=your_key_here
python demo_molit.py
```

### MCP 서버 (Claude Desktop 연동)

HomeRadar를 Claude Desktop의 MCP 서버로 사용할 수 있습니다.

1. **의존성 설치**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Claude Desktop 설정**:
   - 설정 파일 위치:
     - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
     - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

   - 다음 내용 추가:
   ```json
   {
     "mcpServers": {
       "homeradar": {
         "command": "python",
         "args": ["-m", "mcp_server.server"],
         "cwd": "D:\\HomeRadar",
         "env": {
           "HOMERADAR_DB_PATH": "D:\\HomeRadar\\data\\homeradar.duckdb"
         }
       }
     }
   }
   ```

3. **Claude Desktop 재시작**

4. **사용 예시**:
   - "강남구 아파트 최근 거래를 보여줘"
   - "재개발 관련 뉴스를 찾아줘"
   - "서울 지역의 전세가 급등 단지를 조회해줘"

### 환경 변수

- `HOMERADAR_DB_PATH`: DuckDB 파일 경로. 기본값은 `data/homeradar.duckdb`.
- `TZ`: 실행 타임존(예: `Asia/Seoul`).

## 프로젝트 구조

```
HomeRadar/
├── collectors/        # 수집기(API, RSS, HTML 파서 등)
│   ├── base.py       # BaseCollector, RawItem 정의
│   ├── api_collector.py      # 정부 API 수집기 (국토부 등)
│   ├── rss_collector.py      # RSS 뉴스 수집기
│   └── html_collector.py     # HTML 페이지 수집기
├── analyzers/         # 필터링, 엔터티 추출, 스코어링
│   ├── entity_extractor.py   # 단지명, 지역, 사업명 추출
│   └── price_analyzer.py     # 가격 변동 분석
├── graph/             # 그래프 저장/조회
│   ├── graph_store.py        # DuckDB 기반 저장소
│   └── graph_queries.py      # 관점별 쿼리
├── reporters/         # HTML 리포터
│   ├── html_reporter.py      # 일일 리포트 생성
│   └── kpi_logger.py         # KPI 로깅 및 추적
├── pushers/           # 알림 채널 (Telegram, Email 등)
├── mcp_server/        # MCP 서버 (선택적 확장)
├── config/            # 실행 모드 및 소스 설정
│   └── sources.yaml          # 데이터 소스 정의
├── tools/             # 유틸리티 스크립트
├── docs/              # 아키텍처/배포/PRD 등 문서
│   ├── BRANDING.md           # Radar 시리즈 브랜딩 가이드
│   ├── ARCHITECTURE.md       # 전체 모듈 구조
│   ├── DATA_MODEL.md         # 데이터 스키마
│   └── PRD.md                # 제품 요구사항 정의
└── tests/             # unit / integration / e2e 테스트
    ├── unit/
    ├── integration/
    └── e2e/
```

## 테스트 & TDD 전략

HomeRadar는 테스트 주도 개발(TDD)을 기본 원칙으로 하며, 단계별 테스트 디렉터리를 분리했습니다.

### 단계별 디렉터리

- `tests/unit/`: 타입/계약/순수 함수 수준의 단위 테스트 (`pytest tests/unit`)
- `tests/integration/`: 모듈 간 상호작용 검증 (`pytest tests/integration`)
- `tests/e2e/`: `python main.py` 실행을 포함한 엔드투엔드 시나리오 (`pytest tests/e2e`)

### 실행 절차

1. 개발 의존성 설치: `pip install -r requirements-dev.txt`
2. 단위 테스트: `pytest tests/unit`
3. 통합 테스트: `pytest tests/integration`
4. E2E 테스트: `pytest tests/e2e`
5. 전체 테스트: `pytest`

## 문서

- [BRANDING.md](docs/BRANDING.md) – Radar 시리즈 브랜딩 및 네이밍 가이드
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) – 전체 모듈 구조와 데이터 흐름
- [DATA_MODEL.md](docs/DATA_MODEL.md) – 그래프/엔터티 스키마
- [API_SPEC.md](docs/API_SPEC.md) – 모듈 간 인터페이스
- [DEPLOYMENT.md](docs/DEPLOYMENT.md) – 로컬/CI 배포 절차
- [CODING_GUIDE.md](docs/CODING_GUIDE.md) – 코드 스타일 및 규칙
- [PRD.md](docs/PRD.md) – 제품 요구사항 정의
- [ROADMAP.md](docs/ROADMAP.md) – 단계별 개발 계획

## 기술 스택

- **언어**: Python 3.11+
- **데이터 저장소**: DuckDB 파일 (기본 경로 `data/homeradar.duckdb`)
- **수집/파싱**: `requests`, `beautifulsoup4`, `feedparser`
- **템플릿**: `jinja2`
- **CI/CD**: GitHub Actions + GitHub Pages
- **알림**: Telegram Bot API (확장 가능)
- **테스트**: `pytest`

## 데이터 소스

### 현재 계획된 소스

1. **정부 공식 데이터** (T1_official)
   - 국토부 실거래가 API (아파트 매매/전월세)
   - 청약홈 분양 정보

2. **부동산 전문 뉴스** (T2_professional)
   - 뉴스랜드 RSS
   - 매일경제 부동산 섹션
   - 한국경제 부동산 뉴스

3. **시장 데이터 플랫폼** (T3_aggregator)
   - 네이버 부동산 (향후)
   - 직방, 다방 등 (향후)

## 개발 상태

### ✅ 완료 (2025-11-24)

- [x] 프로젝트 구조 초기화
- [x] BaseCollector 및 RawItem 모델 정의
- [x] 소스 설정 파일 (sources.yaml) 작성 - 15개 소스 정의
- [x] RSS 뉴스 수집기 구현 - 3개 소스 테스트 완료 (142 items)
- [x] 그래프 저장소 (DuckDB) 구현 - urls, entities, transactions 테이블
- [x] 엔터티 추출기 구현 - 브랜드/지역/프로젝트/키워드 (364 entities)
- [x] **국토부 API 수집기 구현** - MOLIT 아파트 실거래가 API (17 tests)

### 🚧 현재 진행 중

- [ ] HTML 리포터 구현
- [ ] GitHub Actions 워크플로 설정
- [ ] 메인 스케줄러 구현

### 📋 다음 단계

1. HTML 리포터 및 GitHub Pages 배포
2. 가격 변동 분석 및 스코어링 시스템
3. 메인 스케줄러 (자동 수집)
4. CI/CD 파이프라인 구축
5. 알림 시스템 (Telegram/Email)

## Radar 시리즈

HomeRadar는 도메인별 레이더 시리즈의 일원입니다:

- **[WineRadar](https://github.com/<username>/WineRadar)** - 와인 산업 트렌드 레이더
- **HomeRadar** (현재) - 부동산 시장 레이더
- **TrendRadar** (계획) - 소비재 트렌드 레이더
- **PriceRadar** (계획) - 가격 변동 레이더

각 레이더는 공통 아키텍처를 공유하며, 도메인별 특화 기능을 제공합니다.

## 기여 가이드

1. 이슈를 만들거나 기존 이슈에 의견을 남깁니다.
2. Fork + 브랜치 생성 후 작업합니다.
3. `docs/CODING_GUIDE.md`와 TDD 원칙을 준수합니다.
4. `pytest`로 모든 단계의 테스트를 통과시킵니다.
5. Pull Request를 제출합니다.

## 라이선스

MIT License – 자세한 내용은 [LICENSE](LICENSE)를 참고하세요.
