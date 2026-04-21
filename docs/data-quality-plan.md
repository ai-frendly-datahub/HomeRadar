# Data Quality Plan

- 생성 시각: `2026-04-11T16:05:37.910248+00:00`
- 우선순위: `P0`
- 데이터 품질 점수: `92`
- 가장 약한 축: `추적성`
- Governance: `high`
- Primary Motion: `conversion`

## 현재 이슈

- 현재 설정상 즉시 차단 이슈 없음. 운영 지표와 freshness SLA만 명시하면 됨

## 필수 신호

- MOLIT 실거래가와 전월세 실거래 API 정상 수집 여부
- 청약홈·분양 일정과 매물 inventory의 날짜 기준 정합성
- 부동산 뉴스 신호와 실제 거래·재고 신호의 분리

## 품질 게이트

- 거래일·신고일·수집일을 별도 필드로 유지
- 지역 코드와 단지명을 canonical key로 정규화
- API key 미설정 시 collector는 실패가 아니라 skip 사유를 남김
- 시장 RSS는 공식 거래·청약·공매 fact를 덮어쓰지 않고 corroboration 상태로만 병합

## 다음 구현 순서

- HomeVerificationState를 HTML/MCP 리포트 출력에 포함
- config/sources.yaml의 freshness SLA를 검증하는 stale/skip 리포트를 추가
- 매물 inventory 후보는 source_backlog에서 ToS·anti-bot·stale listing 검증 후 단계적 활성화
- 지역·단지 단위 중복 제거 리포트를 검증 산출물에 포함

## 운영 규칙

- 원문 URL, 수집일, 이벤트 발생일은 별도 필드로 유지한다.
- 공식 source와 커뮤니티/시장 source를 같은 신뢰 등급으로 병합하지 않는다.
- collector가 인증키나 네트워크 제한으로 skip되면 실패를 숨기지 말고 skip 사유를 기록한다.
- 이 문서는 `scripts/build_data_quality_review.py --write-repo-plans`로 재생성한다.
