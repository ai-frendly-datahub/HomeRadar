# Radar 시리즈 네이밍/브랜드 가이드

## 상위 원칙
- 브랜드 접두어: `[Domain]Radar`
- 코드/패키지: 소문자 케밥/스네이크(`home-radar`, `homeradar`)
- 확장성: 메인 브랜드 + 서브 모듈(`HomeRadar Apt`, `HomeRadar Rent` 등)

## 레포/도메인 규칙
- GitHub: `{domain}-radar` (예: `home-radar`, `price-radar`, `trend-radar`, `wine-radar`)
- 패키지 네임스페이스: `{domain}radar` (파이썬 패키지명)
- 도메인: `{domain}radar.com` 또는 `radar.<tld>/<domain>`
- CI/CD: 공통 워크플로 이름 패턴 `Radar {Domain} Crawler` / `Radar {Domain} Deploy`

## 제품/화면 표기
- 메인: `HomeRadar` (국문: 홈레이더)
- 모듈: `HomeRadar Apt`, `HomeRadar Rent`, `HomeRadar Invest` …
- 한글 병기 시: “홈레이더 아파트”, “홈레이더 렌트”

## 슬로건/태그라인 예시
- HomeRadar: “내가 찍어둔 집을 추적하는 홈레이더”
- PriceRadar: “지금 사야 할 상품을 찾아주는 가격 레이더”
- TrendRadar: “사람들이 지금 관심 두는 키워드를 비추는 트렌드 레이더”

## 색/톤 제안 (초안)
- HomeRadar: 네이비/민트(신뢰 + 프레시), 라운디드 카드
- PriceRadar: 코랄/블루(딜/체크), 밝은 대비
- TrendRadar: 퍼플/블루(인사이트), 그래디언트 라인
- WineRadar: 버건디/샴페인 (기존 유지)

## 폴더/문서 공통 구조 예시
```
{Domain}Radar/
  README.md
  docs/
    BRANDING.md          # 공통 가이드
    {DOMAIN}_PRD.md      # 서비스별 PRD
  {domain}radar/         # 코드 루트
    collectors/
    analyzers/
    scoring/
    reporters/
```
